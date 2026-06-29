package collections

import (
	"encoding/binary"
	"errors"

	"github.com/yannick/wavespan/internal/storage"
)

// Budget collScope sub-scope bytes (under subData|chunk(ns)|chunk(coll)). Existing collScope sub-scopes
// are 0x00..0x04 (scopeCard/Elem/ZPtr/Type in statemachine.go:36-39, scopeTTLPtr in ttl.go:17), so 0x05
// is the first free byte. The lease-EXPIRY index is NOT a collScope sub-scope — it is shard-level
// (subBudExp, below) so the leader sweeps it shard-wide like the TTL index (plan I1, §3.4).
const (
	scopeBudPool  byte = 0x05 // the pool record (combined cfg+state; append-extensible)
	scopeBudLease byte = 0x06 // <leaseID> -> leaseRec
	scopeBudTomb  byte = 0x07 // <leaseID> -> settled tombstone {finalSpent, reason} (point lookup, 2b.3)
)

// Shard-level (prefix) due-index sub-scopes for the budget, alongside subTTL/subDedup/subDedupRing
// (0x02..0x04). Expiry-ordered and ns/coll-embedded so ONE shard-wide scan finds every due lease across
// all collections (mirrors ttl.go's index):
//
//	<prefix>|subBudExp|be(reclaimNotBeforeMs)|chunk(ns)|chunk(coll)|leaseID -> empty  (auto-reclaim, 2b.2)
//	<prefix>|subBudTombGC|be(gcDueMs)|chunk(ns)|chunk(coll)|leaseID         -> empty  (tombstone GC, 2b.5)
const (
	subBudExp    byte = 0x05
	subBudTombGC byte = 0x06
)

// Budget modes (Stage 1 ships STRICT only; modeRelaxed reserved for Stage 4 and rejected at init here).
const (
	modeStrict  uint8 = 1
	modeRelaxed uint8 = 2
)

// Sentinels returned in ProposeResult.Data (mirror wrongType/notNumber in command.go:42-44).
var (
	budNoCapacity = []byte("BUDNOCAP")    // grant could not allocate (>0) in STRICT
	budExists     = []byte("BUDEXISTS")   // init on an existing pool
	budNoLease    = []byte("BUDNOLEASE")  // report/return on unknown lease
	budNoBudget   = []byte("BUDNOBUDGET") // grant against a pool that does not exist (B9: distinct from budNoLease)
	budBadMode    = []byte("BUDBADMODE")  // init with a non-STRICT mode, or invalid cap (B3/B4)
	budBadParam   = []byte("BUDBADPARAM") // init/grant with an out-of-bounds pacing/timing param (2a.3)
	budSettled    = []byte("BUDSETTLED")  // grant against a leaseID with a settled tombstone (2b.3; never re-grant)
)

var (
	errShortPool   = errors.New("collections: short budget pool record")
	errShortLease  = errors.New("collections: short budget lease record")
	errShortTomb   = errors.New("collections: short budget tombstone record")
	errMissingPool = errors.New("collections: budget lease without a pool (corrupt state)")
)

// Tombstone reasons (§3.5/§3.6): a settlement is either a holder-ATTESTED graceful Return (credit the
// true remainder to available) or a forced UN-attested expiry (debit the whole remainder to spent).
const (
	reasonReturn byte = 1
	reasonExpire byte = 2
)

// poolRec is the budget pool. Quantities are int64 micro-units.
type poolRec struct {
	Cap       int64
	Available int64
	LeasedOut int64 // Σ (lease.amount - lease.spent) over outstanding leases (UNSPENT held)
	Spent     int64 // Σ lease.spent (cumulative consumed)
	Epoch     uint64
	Mode      uint8
	Rate      int64 // micro-units/sec; 0 = no pacing (Stage-1 behavior)
	Burst     int64 // token-bucket ceiling; Stage 1: == Cap
	// Stage-2 pacing tail (append-only; a Stage-1 57-byte record decodes these as 0).
	LastRefillMs int64 // leader-stamped ms of the last token accrual (lazy-init on first paced grant)
	Tokens       int64 // current paced tokens available to grant (≤ Burst)
	// Stage-2 timing config tail (append-only; a 73-byte 2a.1 record decodes these as 0). These feed the
	// expiry/reclaim window (2b) and are echoed to the holder (2c); a grant's ttlOverride falls back to
	// DefaultTTLMs when 0.
	SelfGuardMs        int64 // holder-side self-fence band; >= maxClockSkewMs when any TTL is in play (I2)
	MaxPauseMs         int64 // holder-side max pause before self-fence; widens the reclaim deadline (2b)
	DefaultTTLMs       int64 // default per-lease TTL when a grant carries no ttlOverride (0 = non-expiring)
	DedupRetryWindowMs int64 // tombstone GC window; sizes B5 dedup safety (>= minDedupRetryWindowMs, I3)
}

// Pacing/timing admission bounds (money-load-bearing; consumed by applyBudInit + applyBudGrant).
const (
	// maxClockSkewMs is the HLC mesh clock-skew bound (matches internal/version/hlc.go's default). The
	// reclaim formula (2b) and the self_guard floor are gated on it.
	maxClockSkewMs int64 = 500
	// pacingBurstCeil bounds burst so pacingMaxElapsedMs's product k*burst*1000 cannot overflow int64
	// (4 * (1<<50) * 1000 ≈ 4.5e18 < 9.2e18).
	pacingBurstCeil int64 = 1 << 50
	// minDedupRetryWindowMs is the cluster RPC-retry budget floor for the tombstone dedup window (I3):
	// a TTL'd budget whose window is shorter than the retry budget could re-grant a settled lease.
	minDedupRetryWindowMs int64 = 30_000
)

func (s *shardSM) budPoolKey(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudPool)
}
func (s *shardSM) budLeasePrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudLease)
}
func (s *shardSM) budLeaseKey(ns, coll, leaseID []byte) []byte {
	return append(s.budLeasePrefix(ns, coll), leaseID...)
}

// budTombKey is the collScope point-lookup key for a settled-lease tombstone (2b.3).
func (s *shardSM) budTombKey(ns, coll, leaseID []byte) []byte {
	return append(append(s.collScope(ns, coll), scopeBudTomb), leaseID...)
}

// --- shard-level due indices (mirror ttl.go's ttlSpace/ttlIndexKey) ---

func (s *shardSM) budExpSpace() []byte    { return append(append([]byte{}, s.prefix...), subBudExp) }
func (s *shardSM) budTombGCSpace() []byte { return append(append([]byte{}, s.prefix...), subBudTombGC) }

// budExpKey is the shard-level auto-reclaim index key: prefix|subBudExp|be(reclaim)|chunk(ns)|chunk(coll)|leaseID.
func (s *shardSM) budExpKey(reclaimMs int64, ns, coll, leaseID []byte) []byte {
	out := append(s.budExpSpace(), u64(uint64(reclaimMs))...)
	out = appendChunk(out, ns)
	out = appendChunk(out, coll)
	return append(out, leaseID...)
}

// budTombGCKey is the shard-level tombstone-GC index key (same layout, keyed by gcDueMs).
func (s *shardSM) budTombGCKey(gcDueMs int64, ns, coll, leaseID []byte) []byte {
	out := append(s.budTombGCSpace(), u64(uint64(gcDueMs))...)
	out = appendChunk(out, ns)
	out = appendChunk(out, coll)
	return append(out, leaseID...)
}

// dueBudLease is a lease whose reclaim deadline (or whose tombstone-GC deadline) has passed, returned by
// budExpiryDueQuery / budTombGCDueQuery for the sweeper. ReclaimMs carries the index key's time component
// (reclaimNotBeforeMs for the expiry index, gcDueMs for the GC index) so the sweep can recompute the key.
type dueBudLease struct {
	NS, Coll, LeaseID []byte
	ReclaimMs         int64
}

// setBudExp / delBudExp write or drop the shard-level expiry-index entry (value-less). Budget ops are not
// coalesced (B7), so these go straight to u.ops like ttl.go's setTTL — no in-batch overlay needed.
func (u *updateCtx) setBudExp(ns, coll, leaseID []byte, reclaimMs int64) {
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.budExpKey(reclaimMs, ns, coll, leaseID), Value: []byte{}})
}
func (u *updateCtx) delBudExp(ns, coll, leaseID []byte, reclaimMs int64) {
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.budExpKey(reclaimMs, ns, coll, leaseID), Delete: true})
}

func putI64(b []byte, v int64) { binary.BigEndian.PutUint64(b, uint64(v)) }
func getI64(b []byte) int64    { return int64(binary.BigEndian.Uint64(b)) }

// --- Task 2a.2: small integer helpers for the token-bucket pacing math (unexported, deterministic) ---

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// clampI64 returns v clamped to [lo, hi] (assumes lo <= hi).
func clampI64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// pacingMaxElapsedMs bounds how much idle time a single grant may accrue tokens for, so a long-idle
// budget cannot mint an unbounded burst on its next draw. I4: the product k*burst*1000 is computed
// BEFORE the divide — the rate-independent numerator is admission-bounded (burst <= pacingBurstCeil,
// Task 2a.3) so it can't overflow, and unlike k*(burst/rate)*1000 it never integer-truncates to 0 when
// rate > burst (which would wedge accrual at 0 forever).
func pacingMaxElapsedMs(rate, burst int64) int64 {
	const k = 4
	if rate <= 0 {
		return 0
	}
	return (k * burst * 1000) / rate
}

func encodePool(p poolRec) []byte {
	b := make([]byte, 8*4+8+1+8*2+8*2+8*4) // [prefix 57] | pacing(16) | timing config(32) -> 105
	putI64(b[0:], p.Cap)
	putI64(b[8:], p.Available)
	putI64(b[16:], p.LeasedOut)
	putI64(b[24:], p.Spent)
	binary.BigEndian.PutUint64(b[32:], p.Epoch)
	b[40] = p.Mode
	putI64(b[41:], p.Rate)
	putI64(b[49:], p.Burst)
	putI64(b[57:], p.LastRefillMs)
	putI64(b[65:], p.Tokens)
	putI64(b[73:], p.SelfGuardMs)
	putI64(b[81:], p.MaxPauseMs)
	putI64(b[89:], p.DefaultTTLMs)
	putI64(b[97:], p.DedupRetryWindowMs)
	return b
}

// decodePool is APPEND-TOLERANT (B1): it reads only the fixed 57-byte prefix and ignores any trailing
// bytes, so Stages 2-3 may append fields (lastRefillMs, tokens, pendingReclaim) to the same record with
// NO snapshot migration. Contract: append-only — never reorder or resize an existing field.
func decodePool(b []byte) (poolRec, error) {
	if len(b) < 57 { // a shorter buffer is corruption; a longer one is a newer-stage record (tolerated)
		return poolRec{}, errShortPool
	}
	p := poolRec{
		Cap: getI64(b[0:]), Available: getI64(b[8:]), LeasedOut: getI64(b[16:]), Spent: getI64(b[24:]),
		Epoch: binary.BigEndian.Uint64(b[32:]), Mode: b[40], Rate: getI64(b[41:]), Burst: getI64(b[49:]),
	}
	if len(b) >= 73 { // Stage-2 pacing tail present (a Stage-1 record stops at 57 -> fields stay 0)
		p.LastRefillMs = getI64(b[57:])
		p.Tokens = getI64(b[65:])
	}
	if len(b) >= 105 { // Stage-2 timing-config tail present (a 2a.1 record stops at 73 -> fields stay 0)
		p.SelfGuardMs = getI64(b[73:])
		p.MaxPauseMs = getI64(b[81:])
		p.DefaultTTLMs = getI64(b[89:])
		p.DedupRetryWindowMs = getI64(b[97:])
	}
	return p, nil
}

// leaseRec is one outstanding lease.
type leaseRec struct {
	Holder []byte
	Amount int64
	Spent  int64
	Epoch  uint64
	// Stage-2 timing tail (append-only; a Stage-1 lease with no tail decodes these as 0). These are
	// encoded AFTER the trailing holder chunk so the Stage-1 holder offset is unshifted (§3.3). They are
	// only populated for a timed grant (resolved ttl>0); a non-timed lease leaves them 0.
	GrantedMs          int64 // leader-stamped grant time (also feeds pacing); 0 for a Stage-1 lease
	ReclaimNotBeforeMs int64 // replicated logical reclaim deadline (§2); the expiry-index key + sweep gate
	ExpiresMs          int64 // = GrantedMs+ttl; INFORMATIONAL only (§3.3 M2) — never feeds a stop/reclaim
}

func encodeLease(l leaseRec) []byte {
	b := make([]byte, 0, 8*3+4+len(l.Holder)+8*3)
	var num [8]byte
	putI64(num[:], l.Amount)
	b = append(b, num[:]...)
	putI64(num[:], l.Spent)
	b = append(b, num[:]...)
	binary.BigEndian.PutUint64(num[:], l.Epoch)
	b = append(b, num[:]...)
	b = appendChunk(b, l.Holder) // uint32 len-prefixed (command.go:300)
	// Stage-2 timing tail AFTER the holder chunk (append-tolerant: a Stage-1 decoder stops at the holder).
	putI64(num[:], l.GrantedMs)
	b = append(b, num[:]...)
	putI64(num[:], l.ReclaimNotBeforeMs)
	b = append(b, num[:]...)
	putI64(num[:], l.ExpiresMs)
	b = append(b, num[:]...)
	return b
}

func decodeLease(b []byte) (leaseRec, error) {
	if len(b) < 24 {
		return leaseRec{}, errShortLease
	}
	l := leaseRec{Amount: getI64(b[0:]), Spent: getI64(b[8:]), Epoch: binary.BigEndian.Uint64(b[16:])}
	holder, rest, err := takeChunk(b[24:]) // command.go:307
	if err != nil {
		return leaseRec{}, err
	}
	l.Holder = append([]byte{}, holder...)
	if len(rest) >= 24 { // Stage-2 timing tail present (a Stage-1 lease ends at the holder -> fields stay 0)
		l.GrantedMs = getI64(rest[0:])
		l.ReclaimNotBeforeMs = getI64(rest[8:])
		l.ExpiresMs = getI64(rest[16:])
	}
	return l, nil
}

// tombRec is a settled-lease tombstone: the final booked spent + how it settled (return vs expire). It
// is retained for >= the dedup retry window so a late retried Draw on the same leaseID returns
// budSettled instead of re-granting fresh money (closes B5 for the timed path, §3.6/§3.7).
type tombRec struct {
	FinalSpent int64
	Reason     byte
}

func encodeTomb(t tombRec) []byte {
	b := make([]byte, 9)
	putI64(b[0:], t.FinalSpent)
	b[8] = t.Reason
	return b
}

func decodeTomb(b []byte) (tombRec, error) {
	if len(b) < 9 {
		return tombRec{}, errShortTomb
	}
	return tombRec{FinalSpent: getI64(b[0:]), Reason: b[8]}, nil
}

// --- Task 3: overlay-aware budget helpers (mirror fieldVal/setFieldVal in hincr.go) ---
//
// Writes go to u.ops AND the in-batch overlay (u.vals, keyed by the full storage key) so two budget ops
// coalesced into one Update batch compose. Reads honor the overlay first, then fall back to u.s.getData.

// getPool returns the pool record honoring the in-batch overlay.
func (u *updateCtx) getPool(ns, coll []byte) (poolRec, bool, error) {
	k := u.s.budPoolKey(ns, coll)
	if v, ok := u.vals[string(k)]; ok {
		if v == nil {
			return poolRec{}, false, nil
		}
		p, err := decodePool(v)
		return p, err == nil, err
	}
	v, found, err := u.s.getData(k)
	if err != nil || !found {
		return poolRec{}, false, err
	}
	p, err := decodePool(v)
	return p, err == nil, err
}

func (u *updateCtx) setPool(ns, coll []byte, p poolRec) {
	k := u.s.budPoolKey(ns, coll)
	enc := encodePool(p)
	u.vals[string(k)] = enc
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Value: enc})
}

func (u *updateCtx) getLease(ns, coll, id []byte) (leaseRec, bool, error) {
	k := u.s.budLeaseKey(ns, coll, id)
	if v, ok := u.vals[string(k)]; ok {
		if v == nil {
			return leaseRec{}, false, nil
		}
		l, err := decodeLease(v)
		return l, err == nil, err
	}
	v, found, err := u.s.getData(k)
	if err != nil || !found {
		return leaseRec{}, false, err
	}
	l, err := decodeLease(v)
	return l, err == nil, err
}

func (u *updateCtx) setLease(ns, coll, id []byte, l leaseRec) {
	k := u.s.budLeaseKey(ns, coll, id)
	enc := encodeLease(l)
	u.vals[string(k)] = enc
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Value: enc})
}

func (u *updateCtx) delLease(ns, coll, id []byte) {
	k := u.s.budLeaseKey(ns, coll, id)
	u.vals[string(k)] = nil
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Delete: true})
}

// getTomb / setTomb / delTomb are overlay-aware tombstone helpers (mirror getLease/setLease/delLease).
func (u *updateCtx) getTomb(ns, coll, id []byte) (tombRec, bool, error) {
	k := u.s.budTombKey(ns, coll, id)
	if v, ok := u.vals[string(k)]; ok {
		if v == nil {
			return tombRec{}, false, nil
		}
		tr, err := decodeTomb(v)
		return tr, err == nil, err
	}
	v, found, err := u.s.getData(k)
	if err != nil || !found {
		return tombRec{}, false, err
	}
	tr, err := decodeTomb(v)
	return tr, err == nil, err
}

func (u *updateCtx) setTomb(ns, coll, id []byte, tr tombRec) {
	k := u.s.budTombKey(ns, coll, id)
	enc := encodeTomb(tr)
	u.vals[string(k)] = enc
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Value: enc})
}

func (u *updateCtx) delTomb(ns, coll, id []byte) {
	k := u.s.budTombKey(ns, coll, id)
	u.vals[string(k)] = nil
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Delete: true})
}

// setBudTombGC / delBudTombGC write or drop the shard-level tombstone-GC index entry (value-less); the
// sweep (2b.5) deletes the tombstone + this entry once gcDueMs passes.
func (u *updateCtx) setBudTombGC(ns, coll, leaseID []byte, gcDueMs int64) {
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.budTombGCKey(gcDueMs, ns, coll, leaseID), Value: []byte{}})
}
func (u *updateCtx) delBudTombGC(ns, coll, leaseID []byte, gcDueMs int64) {
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.budTombGCKey(gcDueMs, ns, coll, leaseID), Delete: true})
}

// --- Task 4: apply logic — init / grant / report / return ---
//
// Each runs inside applyOne under the deterministic overlay and must preserve INV-LOCAL
// (cap == available + leasedOut + spent). They return (ProposeResult, error) directly; benign failures
// return a sentinel in Data, never an error (errors are reserved for storage faults / corrupt entries).
// Quantities assigned to ProposeResult.Value are uint64-cast.

// applyBudInit creates a pool. Returns budExists if one already exists, or budBadMode for a non-STRICT
// mode / invalid cap (B3, B4) — both are sentinels in Data, not errors.
func (u *updateCtx) applyBudInit(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) < 25 {
		return ProposeResult{}, errShortCommand
	}
	k := c.Items[0].Key
	mode := k[0]
	capacity := getI64(k[1:])
	rate := getI64(k[9:])
	burst := getI64(k[17:])
	// Timing config (append-tolerant: a 25-byte Stage-1 key leaves these 0 -> non-paced, non-expiring).
	var selfGuardMs, maxPauseMs, defaultTTLMs, dedupRetryWindowMs int64
	if len(k) >= 57 {
		selfGuardMs = getI64(k[25:])
		maxPauseMs = getI64(k[33:])
		defaultTTLMs = getI64(k[41:])
		dedupRetryWindowMs = getI64(k[49:])
	}
	// B3: Stage 1 only supports STRICT. B4: reject a negative cap. (admission-validates at the SM too,
	// since the in-process API and direct proposers bypass the CRD layer; apply is deterministic).
	if mode != modeStrict || capacity < 0 {
		return ProposeResult{Data: budBadMode}, nil
	}
	// 2a.3 pacing/timing admission (budBadParam): keep the numerator bounded and never accept a TTL'd
	// budget whose guard band / dedup window is too small to be money-safe (I2/I3).
	if rate < 0 || burst < 0 || burst > pacingBurstCeil ||
		(defaultTTLMs > 0 && selfGuardMs < maxClockSkewMs) ||
		(defaultTTLMs > 0 && dedupRetryWindowMs < minDedupRetryWindowMs) {
		return ProposeResult{Data: budBadParam}, nil
	}
	if _, found, err := u.getPool(c.NS, c.Coll); err != nil {
		return ProposeResult{}, err
	} else if found {
		return ProposeResult{Data: budExists}, nil
	}
	// Create the typeBudget header here — only AFTER validation passed — so a rejected init (bad mode or
	// negative cap) leaves NO orphaned type header behind (keeps grant-before-define and collection
	// listings clean). The dispatch type guard already rejected a conflicting existing type, so ensureType
	// sees absent-or-budget and writes the header iff absent.
	if ok, err := u.ensureType(c.NS, c.Coll, typeBudget); err != nil {
		return ProposeResult{}, err
	} else if !ok {
		return ProposeResult{Data: wrongType}, nil
	}
	p := poolRec{Cap: capacity, Available: capacity, LeasedOut: 0, Spent: 0, Epoch: 1, Mode: mode, Rate: rate, Burst: burst,
		SelfGuardMs: selfGuardMs, MaxPauseMs: maxPauseMs, DefaultTTLMs: defaultTTLMs, DedupRetryWindowMs: dedupRetryWindowMs}
	if rate > 0 { // seed the token bucket full; lastRefillMs lazy-inits on the first paced grant (M-3, §3.1)
		p.Tokens = burst
	}
	u.setPool(c.NS, c.Coll, p)
	return ProposeResult{Value: 1}, nil
}

// applyBudGrant atomically allocates min(requested, available) and emits a lease.
// Idempotent for the lease's lifetime: a retry with the same leaseID (Items[0].Key) returns the existing
// lease, never re-debits. leaseID + Idem are NOT routed through the dedup ring (see idempotency design).
// B6: `holder` is recorded on the lease but NOT validated on later Report/Return — in Stage 1 the lease_id
// is the bearer capability. Holder binding/auth is Stage 3+ (design open Q6).
func (u *updateCtx) applyBudGrant(c command) (ProposeResult, error) {
	// Val layout (Stage 2): amount(8) | grantedMs(8) | ttlOverride(8) | holder(rest) — 24 fixed bytes.
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 || len(c.Items[0].Val) < 24 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	amount := getI64(c.Items[0].Val[0:8])
	grantedMs := getI64(c.Items[0].Val[8:16])
	ttlOverride := getI64(c.Items[0].Val[16:24])
	holder := c.Items[0].Val[24:]
	if amount < 0 { // B4: reject a negative draw (deterministic guard; never trust the caller)
		return ProposeResult{Data: budNoCapacity}, nil
	}
	// Step 0 (§3.7): a settled tombstone for this leaseID -> NEVER re-grant. Closes B5 for the timed path:
	// a late retried Draw on a returned/expired lease returns budSettled instead of minting fresh money.
	// Non-timed leases never get a tombstone, so this is transparent to the Stage-1 (untimed) re-grant path.
	if _, settled, err := u.getTomb(c.NS, c.Coll, leaseID); err != nil {
		return ProposeResult{}, err
	} else if settled {
		return ProposeResult{Data: budSettled}, nil
	}
	// idempotency: an existing lease row for this leaseID -> echo its grant with the SAME effective timing
	// (Gap#2, load-bearing for 2c.4's exactly-once refill): grantedMs/ttl reconstructed from the stored
	// lease, selfGuard/maxPause from the pool config. A retry must produce a byte-identical timing echo, or
	// the holder's freshness gate + deadline stamping break on exactly the retried-Draw path.
	if l, found, err := u.getLease(c.NS, c.Coll, leaseID); err != nil {
		return ProposeResult{}, err
	} else if found {
		gr := GrantResult{Granted: l.Amount, GrantedMs: l.GrantedMs}
		if l.ExpiresMs > 0 { // timed lease: ttl = ExpiresMs - GrantedMs (reconstructed, not re-resolved)
			gr.TTLMs = l.ExpiresMs - l.GrantedMs
		}
		if p, pf, perr := u.getPool(c.NS, c.Coll); perr != nil {
			return ProposeResult{}, perr
		} else if pf {
			gr.SelfGuardMs = p.SelfGuardMs
			gr.MaxPauseMs = p.MaxPauseMs
		}
		return ProposeResult{Value: uint64(l.Amount), Data: encodeGrantResult(gr)}, nil
	}
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoBudget}, nil // B9: budget pool does not exist (distinct from budNoLease)
	}
	// I2 authoritative gate: resolve the effective ttl (override, else the budget default) and refuse to
	// write a timed lease whose self_guard band is too small to be safe. A per-grant ttlOverride can make
	// ANY budget timed, so this apply-time check — not just the friendlier Define-time reject — is the
	// single authoritative gate (2b must not add a second one). 2a stores no reclaim deadline yet; this
	// only enforces the invariant so it holds the moment timing lands.
	ttl := ttlOverride
	if ttl == 0 {
		ttl = p.DefaultTTLMs
	}
	if ttl > 0 && p.SelfGuardMs < maxClockSkewMs {
		return ProposeResult{Data: budBadParam}, nil
	}
	// §3.2 token-bucket pacing (rate==0 => Stage-1 behavior, pacing state untouched).
	if p.Rate > 0 {
		if p.LastRefillMs == 0 { // lazy-init (M-3): the first grant accrues 0, never rate*grantedMs
			p.LastRefillMs = grantedMs
		}
		maxElapsed := pacingMaxElapsedMs(p.Rate, p.Burst)
		elapsed := clampI64(grantedMs-p.LastRefillMs, 0, maxElapsed)
		accrued := p.Rate * elapsed / 1000 // integer floor, deterministic
		ceil := min64(p.Burst, p.Cap-p.Spent)
		p.Tokens = min64(ceil, p.Tokens+accrued)
		if p.Tokens < 0 {
			p.Tokens = 0
		}
		if grantedMs > p.LastRefillMs {
			p.LastRefillMs = grantedMs
		}
	}
	grant := amount
	if p.Rate > 0 {
		grant = min64(grant, p.Tokens)
	}
	if grant > p.Available {
		grant = p.Available
	}
	if grant <= 0 { // STRICT: nothing to give (token-bound or available-bound)
		return ProposeResult{Data: budNoCapacity}, nil
	}
	p.Available -= grant
	p.LeasedOut += grant
	if p.Rate > 0 {
		p.Tokens -= grant
	}
	u.setPool(c.NS, c.Coll, p)
	lr := leaseRec{Holder: append([]byte{}, holder...), Amount: grant, Spent: 0, Epoch: p.Epoch, GrantedMs: grantedMs}
	if ttl > 0 { // timed lease: stamp the REPLICATED reclaim deadline (§2) + write the shard-level expiry index
		lr.ReclaimNotBeforeMs = grantedMs + ttl + 3*maxClockSkewMs + p.MaxPauseMs
		lr.ExpiresMs = grantedMs + ttl // informational only (§3.3 M2); never feeds a stop/reclaim decision
		u.setBudExp(c.NS, c.Coll, leaseID, lr.ReclaimNotBeforeMs)
	}
	u.setLease(c.NS, c.Coll, leaseID, lr)
	return ProposeResult{Value: uint64(grant), Data: encodeGrantResult(GrantResult{
		Granted: grant, GrantedMs: grantedMs, TTLMs: ttl, SelfGuardMs: p.SelfGuardMs, MaxPauseMs: p.MaxPauseMs})}, nil
}

// applyBudReport folds a cumulative-per-lease spent total (max), moving the delta leasedOut->spent.
func (u *updateCtx) applyBudReport(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 || len(c.Items[0].Val) < 8 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	l, found, err := u.getLease(c.NS, c.Coll, leaseID)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoLease}, nil
	}
	reported := getI64(c.Items[0].Val)
	if reported <= l.Spent { // stale/duplicate -> no-op (max fold)
		return ProposeResult{Value: uint64(l.Amount - l.Spent)}, nil
	}
	if reported > l.Amount { // clamp: a lease cannot spend more than granted (STRICT)
		reported = l.Amount
	}
	delta := reported - l.Spent
	l.Spent = reported
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found { // a lease implies a pool in Stage 1 (pools are never deleted) — guard corruption loudly
		return ProposeResult{}, errMissingPool
	}
	p.LeasedOut -= delta
	p.Spent += delta
	u.setPool(c.NS, c.Coll, p)
	u.setLease(c.NS, c.Coll, leaseID, l)
	return ProposeResult{Value: uint64(l.Amount - l.Spent)}, nil
}

// applyBudReturn is the holder-ATTESTED graceful settlement (§3.6, terminal-symmetric): tombstone-check
// first (idempotent no-op if already settled), fold the final report, then CREDIT the true remainder back
// to available and — in ONE entry — delete the lease row, delete the shard-level expiry index, and write a
// tombstone (+ its GC index for a timed lease). Crediting is safe ONLY here because the holder's final
// report is authoritative; forced expiry (§3.5) debits instead.
func (u *updateCtx) applyBudReturn(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	// terminal-symmetric: a settled lease (returned OR expired) is a tombstone no-op — a late return after
	// an expiry must not double-settle (§3.6).
	if _, settled, err := u.getTomb(c.NS, c.Coll, leaseID); err != nil {
		return ProposeResult{}, err
	} else if settled {
		return ProposeResult{Value: 0}, nil
	}
	l, found, err := u.getLease(c.NS, c.Coll, leaseID)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoLease}, nil // unknown lease (never granted)
	}
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found { // a lease implies a pool in Stage 1 — fail loud on corruption rather than write a bad bucket
		return ProposeResult{}, errMissingPool
	}
	// fold final report if present and larger (max fold; clamp to amount) — leasedOut -> spent
	if len(c.Items[0].Val) >= 8 {
		if reported := getI64(c.Items[0].Val); reported > l.Spent {
			if reported > l.Amount {
				reported = l.Amount
			}
			delta := reported - l.Spent
			l.Spent = reported
			p.LeasedOut -= delta
			p.Spent += delta
		}
	}
	// CREDIT the attested remainder back to available (the ONLY settlement path that does so).
	rem := l.Amount - l.Spent
	p.Available += rem
	p.LeasedOut -= rem
	u.setPool(c.NS, c.Coll, p)
	u.delLease(c.NS, c.Coll, leaseID)
	if l.ReclaimNotBeforeMs > 0 { // timed lease: drop its expiry-index entry + leave a settled tombstone
		u.delBudExp(c.NS, c.Coll, leaseID, l.ReclaimNotBeforeMs)
		u.setTomb(c.NS, c.Coll, leaseID, tombRec{FinalSpent: l.Spent, Reason: reasonReturn})
		u.setBudTombGC(c.NS, c.Coll, leaseID, l.ReclaimNotBeforeMs+p.DedupRetryWindowMs)
	}
	return ProposeResult{Value: uint64(rem)}, nil
}

// GrantResult is the effective grant echoed back from apply to Collections.BudgetGrant. The timing
// fields are only meaningful once timed leases land (2b); in 2a only Granted/GrantedMs are populated.
type GrantResult struct {
	Granted     int64
	GrantedMs   int64
	TTLMs       int64
	SelfGuardMs int64
	MaxPauseMs  int64
}

// encodeGrantResult / decodeGrantResult carry the effective grant in ProposeResult.Data. The success
// payload is a fixed 40 bytes (amount|grantedMs|ttl|selfGuard|maxPause) — the EFFECTIVE timing resolved
// inside apply, echoed so the holder runs the freshness gate + stamps its deadline + sets the self-fence
// (§5 M4). It is length-distinguishable from the budget sentinels (BUDNOCAP/BUDNOBUDGET/BUDSETTLED/...),
// none of which are 40 bytes, so the typed API switches sentinels first then decodes the rest as a
// success (Gap#1). A non-timed grant carries ttl/selfGuard/maxPause = the pool's (0 when unconfigured).
func encodeGrantResult(r GrantResult) []byte {
	b := make([]byte, 40)
	putI64(b[0:], r.Granted)
	putI64(b[8:], r.GrantedMs)
	putI64(b[16:], r.TTLMs)
	putI64(b[24:], r.SelfGuardMs)
	putI64(b[32:], r.MaxPauseMs)
	return b
}

func decodeGrantResult(b []byte) GrantResult {
	var r GrantResult
	if len(b) >= 8 {
		r.Granted = getI64(b[0:])
	}
	if len(b) >= 16 {
		r.GrantedMs = getI64(b[8:])
	}
	if len(b) >= 40 {
		r.TTLMs = getI64(b[16:])
		r.SelfGuardMs = getI64(b[24:])
		r.MaxPauseMs = getI64(b[32:])
	}
	return r
}

// --- Task 6: read path — budStatQuery + budCheckQuery Lookups ---

type budStatQuery struct{ NS, Coll []byte }

// BudStat is the read-side snapshot of a budget pool's accounting, returned by Collections.BudgetStat.
type BudStat struct {
	Exists                           bool
	Cap, Available, LeasedOut, Spent int64
	Epoch                            uint64
	Mode                             uint8
}

// lookupBudStat reads the pool from a snapshot (called by shardSM.Lookup).
// Reads directly from the snapshot exactly like snapCard / hGetQuery: snap.Get(storage.CFReplData, key)
// -> (value, found, err). There is NO getDataSnap helper.
func (s *shardSM) lookupBudStat(snap storage.Snapshot, q budStatQuery) (BudStat, error) {
	v, found, err := snap.Get(storage.CFReplData, s.budPoolKey(q.NS, q.Coll))
	if err != nil || !found {
		return BudStat{Exists: false}, err
	}
	p, err := decodePool(v)
	if err != nil {
		return BudStat{}, err
	}
	return BudStat{Exists: true, Cap: p.Cap, Available: p.Available, LeasedOut: p.LeasedOut, Spent: p.Spent, Epoch: p.Epoch, Mode: p.Mode}, nil
}

// --- B2: conservation-invariant probe (design §6.8) ---
type budCheckQuery struct{ NS, Coll []byte }

// budCheck reports whether INV-LOCAL holds for the pool, with the buckets for diagnostics.
type budCheck struct {
	Exists                           bool
	OK                               bool // available+leasedOut+spent == cap AND spent <= cap AND no bucket < 0
	Cap, Available, LeasedOut, Spent int64
}

// lookupBudCheck reads ONE consistent snapshot and asserts the conservation invariant. Read-only; the
// caller (BudgetStat-style API + the metrics StrictBudgetInvariantViolated alert) decides how to react.
func (s *shardSM) lookupBudCheck(snap storage.Snapshot, q budCheckQuery) (budCheck, error) {
	v, found, err := snap.Get(storage.CFReplData, s.budPoolKey(q.NS, q.Coll))
	if err != nil || !found {
		return budCheck{Exists: false}, err
	}
	p, err := decodePool(v)
	if err != nil {
		return budCheck{}, err
	}
	ok := p.Available+p.LeasedOut+p.Spent == p.Cap &&
		p.Spent <= p.Cap && p.Available >= 0 && p.LeasedOut >= 0 && p.Spent >= 0
	return budCheck{Exists: true, OK: ok, Cap: p.Cap, Available: p.Available, LeasedOut: p.LeasedOut, Spent: p.Spent}, nil
}
