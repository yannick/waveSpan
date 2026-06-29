package collections

import (
	"encoding/binary"
	"errors"

	"github.com/yannick/wavespan/internal/storage"
)

// Budget sub-scope bytes. Existing collScope sub-scopes are 0x00..0x04 (scopeCard/Elem/ZPtr/Type in
// statemachine.go:36-39, scopeTTLPtr in ttl.go:17), so 0x05 is the first free byte.
const (
	scopeBudPool  byte = 0x05 // the pool record (combined cfg+state; append-extensible)
	scopeBudLease byte = 0x06 // <leaseID> -> leaseRec
	// reserved (NOT used in Stage 1): 0x07 lease-expiry index, 0x08 settled tombstone (both Stage 2)
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
)

var (
	errShortPool   = errors.New("collections: short budget pool record")
	errShortLease  = errors.New("collections: short budget lease record")
	errMissingPool = errors.New("collections: budget lease without a pool (corrupt state)")
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
}

func (s *shardSM) budPoolKey(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudPool)
}
func (s *shardSM) budLeasePrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudLease)
}
func (s *shardSM) budLeaseKey(ns, coll, leaseID []byte) []byte {
	return append(s.budLeasePrefix(ns, coll), leaseID...)
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
	b := make([]byte, 8*4+8+1+8*2+8*2) // [prefix 57] | lastRefillMs,tokens (Stage-2 tail) -> 73
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
	return p, nil
}

// leaseRec is one outstanding lease.
type leaseRec struct {
	Holder []byte
	Amount int64
	Spent  int64
	Epoch  uint64
}

func encodeLease(l leaseRec) []byte {
	b := make([]byte, 0, 8*2+8+4+len(l.Holder))
	var num [8]byte
	putI64(num[:], l.Amount)
	b = append(b, num[:]...)
	putI64(num[:], l.Spent)
	b = append(b, num[:]...)
	binary.BigEndian.PutUint64(num[:], l.Epoch)
	b = append(b, num[:]...)
	b = appendChunk(b, l.Holder) // uint32 len-prefixed (command.go:276)
	return b
}

func decodeLease(b []byte) (leaseRec, error) {
	if len(b) < 24 {
		return leaseRec{}, errShortLease
	}
	l := leaseRec{Amount: getI64(b[0:]), Spent: getI64(b[8:]), Epoch: binary.BigEndian.Uint64(b[16:])}
	holder, _, err := takeChunk(b[24:]) // command.go:283
	if err != nil {
		return leaseRec{}, err
	}
	l.Holder = append([]byte{}, holder...)
	return l, nil
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
	// B3: Stage 1 only supports STRICT. B4: reject negative cap/rate/burst (admission-validates at the SM
	// too, since the in-process API and direct proposers bypass the CRD layer; apply is deterministic).
	if mode != modeStrict || capacity < 0 || rate < 0 || burst < 0 {
		return ProposeResult{Data: budBadMode}, nil
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
	p := poolRec{Cap: capacity, Available: capacity, LeasedOut: 0, Spent: 0, Epoch: 1, Mode: mode, Rate: rate, Burst: burst}
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
	// idempotency: existing lease row for this leaseID -> echo its grant (timing echo widens in 2b).
	if l, found, err := u.getLease(c.NS, c.Coll, leaseID); err != nil {
		return ProposeResult{}, err
	} else if found {
		return ProposeResult{Value: uint64(l.Amount), Data: encodeGrantResult(GrantResult{Granted: l.Amount})}, nil
	}
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoBudget}, nil // B9: budget pool does not exist (distinct from budNoLease)
	}
	_ = ttlOverride // resolved + self_guard-gated in Task 2a.3 (kept in the Val layout from 2a.2)
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
	u.setLease(c.NS, c.Coll, leaseID, leaseRec{Holder: append([]byte{}, holder...), Amount: grant, Spent: 0, Epoch: p.Epoch})
	return ProposeResult{Value: uint64(grant), Data: encodeGrantResult(GrantResult{Granted: grant, GrantedMs: grantedMs})}, nil
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

// applyBudReturn folds the final spent, returns the unspent remainder to available, deletes the lease.
func (u *updateCtx) applyBudReturn(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	l, found, err := u.getLease(c.NS, c.Coll, leaseID)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoLease}, nil // already returned / unknown
	}
	// fold final report if present and larger
	if len(c.Items[0].Val) >= 8 {
		if reported := getI64(c.Items[0].Val); reported > l.Spent {
			if reported > l.Amount {
				reported = l.Amount
			}
			delta := reported - l.Spent
			l.Spent = reported
			p, found, err := u.getPool(c.NS, c.Coll)
			if err != nil {
				return ProposeResult{}, err
			}
			if !found {
				return ProposeResult{}, errMissingPool
			}
			p.LeasedOut -= delta
			p.Spent += delta
			u.setPool(c.NS, c.Coll, p)
		}
	}
	rem := l.Amount - l.Spent
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found { // a lease implies a pool in Stage 1 — fail loud on corruption rather than write a bad bucket
		return ProposeResult{}, errMissingPool
	}
	p.Available += rem
	p.LeasedOut -= rem
	u.setPool(c.NS, c.Coll, p)
	u.delLease(c.NS, c.Coll, leaseID)
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
// payload is a fixed 16 bytes in 2a (amount|grantedMs) and widens to 40 in 2b (amount|grantedMs|ttl|
// selfGuard|maxPause). It is length-distinguishable from the budget sentinels (BUDNOCAP/BUDNOBUDGET/...),
// none of which are 16 or 40 bytes, so the typed API can switch sentinels first then decode the rest as
// a success (Gap#1).
func encodeGrantResult(r GrantResult) []byte {
	b := make([]byte, 16)
	putI64(b[0:], r.Granted)
	putI64(b[8:], r.GrantedMs)
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
