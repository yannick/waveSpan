package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

func TestBudgetOpKindsAndType(t *testing.T) {
	// New opKinds are distinct and contiguous after opBatch(15).
	ops := []opKind{opBudInit, opBudGrant, opBudReport, opBudReturn}
	seen := map[opKind]bool{}
	for _, o := range ops {
		if o <= opBatch {
			t.Fatalf("op %d must be > opBatch(%d)", o, opBatch)
		}
		if seen[o] {
			t.Fatalf("duplicate opKind %d", o)
		}
		seen[o] = true
	}
	// typeForOp maps all four budget ops to typeBudget.
	for _, o := range ops {
		if got := typeForOp(o); got != typeBudget {
			t.Fatalf("typeForOp(%d) = %d, want typeBudget(%d)", o, got, typeBudget)
		}
	}
}

func TestPoolRecRoundTrip(t *testing.T) {
	p := poolRec{Cap: 500_000_000, Available: 400_000_000, LeasedOut: 90_000_000, Spent: 10_000_000, Epoch: 3, Mode: modeStrict, Rate: 0, Burst: 500_000_000}
	got, err := decodePool(encodePool(p))
	if err != nil {
		t.Fatalf("decodePool: %v", err)
	}
	if got != p {
		t.Fatalf("round-trip = %+v, want %+v", got, p)
	}
}

// encodePoolStage1 writes EXACTLY the original 57-byte Stage-1 pool record (cap,avail,leased,spent |
// epoch | mode | rate,burst), with no pacing/timing tail. It exists only to prove the append-tolerance
// contract: a Stage-1 record must still decode under the Stage-2 decoder with the new fields = 0.
func encodePoolStage1(p poolRec) []byte {
	b := make([]byte, 8*4+8+1+8*2)
	putI64(b[0:], p.Cap)
	putI64(b[8:], p.Available)
	putI64(b[16:], p.LeasedOut)
	putI64(b[24:], p.Spent)
	binary.BigEndian.PutUint64(b[32:], p.Epoch)
	b[40] = p.Mode
	putI64(b[41:], p.Rate)
	putI64(b[49:], p.Burst)
	return b
}

func TestPoolRecPacingFieldsRoundTripAndBackCompat(t *testing.T) {
	p := poolRec{Cap: 1000, Available: 400, LeasedOut: 600, Epoch: 1, Mode: modeStrict, Rate: 50, Burst: 100, LastRefillMs: 123456, Tokens: 77}
	got, err := decodePool(encodePool(p))
	if err != nil || got != p {
		t.Fatalf("round-trip = %+v err=%v, want %+v", got, err, p)
	}
	// a Stage-1 record (57 bytes, no pacing tail) decodes with LastRefillMs=0, Tokens=0
	old := encodePoolStage1(poolRec{Cap: 1000, Available: 1000, Epoch: 1, Mode: modeStrict, Burst: 1000})
	g2, err := decodePool(old)
	if err != nil || g2.LastRefillMs != 0 || g2.Tokens != 0 {
		t.Fatalf("stage-1 record back-compat: %+v err=%v", g2, err)
	}
}

func TestLeaseRecRoundTrip(t *testing.T) {
	l := leaseRec{Holder: []byte("node-7"), Amount: 600_000, Spent: 250_000, Epoch: 3}
	got, err := decodeLease(encodeLease(l))
	if err != nil {
		t.Fatalf("decodeLease: %v", err)
	}
	if string(got.Holder) != string(l.Holder) || got.Amount != l.Amount || got.Spent != l.Spent || got.Epoch != l.Epoch {
		t.Fatalf("round-trip = %+v, want %+v", got, l)
	}
}

// --- Task 7: test harness ---

// newTestSM builds a shardSM over an in-memory store with the same prefix/applied plumbing tests use.
// There is NO shardPrefix() helper: baseSM.prefix is the 8-byte big-endian shardID (base_sm.go:26-29),
// built inline here exactly as newBaseSM builds it.
func newTestSM(t *testing.T) *shardSM {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	const shardID uint64 = 2
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, shardID)
	return &shardSM{baseSM: baseSM{store: mem, shardID: shardID, prefix: prefix}}
}

// newTestUpdateCtx opens an updateCtx with ALL overlay maps initialized (matching the literal in
// statemachine.go Update). Writing to a nil map (e.g. ensureType -> u.htype[cs]=want) panics, so every
// map must be non-nil even though the budget apply funcs only touch vals.
func newTestUpdateCtx(t *testing.T) *updateCtx {
	t.Helper()
	return &updateCtx{
		s:            newTestSM(t),
		exists:       map[string]bool{},
		zscore:       map[string]*float64{},
		cardDelta:    map[string]int64{},
		htype:        map[string]collType{},
		vals:         map[string][]byte{},
		inBatchDedup: map[string]ProposeResult{},
	}
}

// mustApply runs one command through the real SM apply path and returns the ProposeResult.
func mustApply(t *testing.T, sm *shardSM, c command) ProposeResult {
	t.Helper()
	r, err := applySingleForTest(sm, encodeCommand(c))
	if err != nil {
		t.Fatalf("apply %v: %v", c.Op, err)
	}
	return r
}

// --- Task 4: command builders + invariant helper ---

func initCmd(ns, coll []byte, capacity int64) command {
	k := make([]byte, 1+8*3)
	k[0] = modeStrict
	putI64(k[1:], capacity)  // cap
	putI64(k[9:], 0)         // rate
	putI64(k[17:], capacity) // burst = cap (Stage 1)
	return command{Op: opBudInit, NS: ns, Coll: coll, Items: []item{{Key: k}}}
}

// grant: leaseID in Items[0].Key; Val = amount(8B) || holder. The tests use leaseID == holder string
// purely for brevity; in production they differ (leaseID is per-refill). Idem stays EMPTY.
func grantCmd(ns, coll []byte, holder string, amt int64) command {
	v := make([]byte, 8+len(holder))
	putI64(v, amt)
	copy(v[8:], holder)
	return command{Op: opBudGrant, NS: ns, Coll: coll, Items: []item{{Key: []byte(holder), Val: v}}}
}
func reportCmd(ns, coll []byte, leaseID string, cum int64) command {
	v := make([]byte, 8)
	putI64(v, cum)
	return command{Op: opBudReport, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}
func returnCmd(ns, coll []byte, leaseID string, cum int64) command {
	v := make([]byte, 8)
	putI64(v, cum)
	return command{Op: opBudReturn, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}
func decodeGrant(b []byte) int64 {
	if len(b) < 8 {
		return -1
	}
	return getI64(b)
}
func assertInv(t *testing.T, p poolRec) {
	t.Helper()
	if p.Available+p.LeasedOut+p.Spent != p.Cap {
		t.Fatalf("INV-LOCAL violated: %d+%d+%d != cap %d", p.Available, p.LeasedOut, p.Spent, p.Cap)
	}
	if p.Available < 0 || p.LeasedOut < 0 || p.Spent > p.Cap {
		t.Fatalf("bucket out of range: %+v", p)
	}
}

// --- Task 3: overlay-aware pool helper ---

func TestUpdateCtxPoolOverlay(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42/total")
	p := poolRec{Cap: 100, Available: 100, Epoch: 1, Mode: modeStrict, Burst: 100}
	u.setPool(ns, coll, p)
	got, found, err := u.getPool(ns, coll)
	if err != nil || !found || got.Available != 100 {
		t.Fatalf("getPool after setPool = %+v found=%v err=%v", got, found, err)
	}
}

// --- Task 4: apply logic ---

func TestApplyBudInitAndGrant(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	if _, err := u.applyBudInit(initCmd(ns, coll, 1000)); err != nil {
		t.Fatalf("init: %v", err)
	}
	r, err := u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if g := decodeGrant(r.Data); g != 600 {
		t.Fatalf("grant amount = %d, want 600", g)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 400 || p.LeasedOut != 600 {
		t.Fatalf("after grant: avail=%d leased=%d, want 400/600", p.Available, p.LeasedOut)
	}
}

func TestApplyBudGrantSaturatesAtAvailable(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 500))
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 900)) // ask > available
	if g := decodeGrant(r.Data); g != 500 {               // PARTIAL grant to available
		t.Fatalf("grant = %d, want 500 (saturated)", g)
	}
	r2, _ := u.applyBudGrant(grantCmd(ns, coll, "B", 100)) // next grant gets nothing -> BUDNOCAP
	if string(r2.Data) != string(budNoCapacity) {
		t.Fatalf("second grant Data = %q, want BUDNOCAP", r2.Data)
	}
}

func TestApplyBudReportThenReturnConserves(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	_, _ = u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	_, _ = u.applyBudReport(reportCmd(ns, coll, "A", 250))
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Spent != 250 || p.LeasedOut != 350 {
		t.Fatalf("after report: spent=%d leased=%d want 250/350", p.Spent, p.LeasedOut)
	}
	// duplicate/stale report (cumulative 200 < 250) is a no-op (max fold)
	_, _ = u.applyBudReport(reportCmd(ns, coll, "A", 200))
	p, _, _ = u.getPool(ns, coll)
	if p.Spent != 250 {
		t.Fatalf("stale report changed spent to %d, want 250", p.Spent)
	}
	// return: unspent 350 goes back to available
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "A", 250))
	p, _, _ = u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 750 || p.LeasedOut != 0 || p.Spent != 250 {
		t.Fatalf("after return: avail=%d leased=%d spent=%d want 750/0/250", p.Available, p.LeasedOut, p.Spent)
	}
}

func TestApplyBudGrantIdempotent(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	r1, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // leaseID "A"
	r2, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // retry same leaseID
	if decodeGrant(r1.Data) != 600 || decodeGrant(r2.Data) != 600 {
		t.Fatalf("idempotent grant mismatch: %d vs %d", decodeGrant(r1.Data), decodeGrant(r2.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	if p.LeasedOut != 600 || p.Available != 400 { // NOT 1200/-200
		t.Fatalf("retry double-granted: leased=%d avail=%d", p.LeasedOut, p.Available)
	}
}

// B3: a non-STRICT mode is rejected at init (Stage 1 is STRICT-only).
func TestApplyBudInitRejectsNonStrict(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	c := initCmd(ns, coll, 1000)
	c.Items[0].Key[0] = modeRelaxed // override mode byte
	r, _ := u.applyBudInit(c)
	if string(r.Data) != string(budBadMode) {
		t.Fatalf("init RELAXED Data = %q, want BUDBADMODE", r.Data)
	}
	if _, found, _ := u.getPool(ns, coll); found {
		t.Fatalf("rejected init must not create a pool")
	}
}

// B4: a negative cap is rejected; a negative draw never debits.
func TestApplyBudRejectsNegativeInputs(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	if r, _ := u.applyBudInit(initCmd(ns, coll, -5)); string(r.Data) != string(budBadMode) {
		t.Fatalf("negative cap Data = %q, want BUDBADMODE", r.Data)
	}
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", -1))
	if string(r.Data) != string(budNoCapacity) {
		t.Fatalf("negative draw Data = %q, want BUDNOCAP", r.Data)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Available != 1000 { // unchanged
		t.Fatalf("negative draw debited available to %d", p.Available)
	}
}

// B9: a grant against an undefined budget returns budNoBudget (not budNoLease).
func TestApplyBudGrantNoBudget(t *testing.T) {
	u := newTestUpdateCtx(t)
	r, _ := u.applyBudGrant(grantCmd([]byte("pacing"), []byte("missing"), "A", 1))
	if string(r.Data) != string(budNoBudget) {
		t.Fatalf("grant on missing budget Data = %q, want BUDNOBUDGET", r.Data)
	}
}

// --- Task 5: applyOne dispatch + type guard ---

func TestApplyOneBudgetDispatchAndWrongType(t *testing.T) {
	sm := newTestSM(t)
	ns := []byte("pacing")
	mustApply(t, sm, initCmd(ns, []byte("li/1"), 1000))
	r := mustApply(t, sm, grantCmd(ns, []byte("li/1"), "A", 600))
	if decodeGrant(r.Data) != 600 {
		t.Fatalf("grant via applyOne = %d, want 600", decodeGrant(r.Data))
	}
	// a hash collection rejects budget ops with WRONGTYPE
	mustApply(t, sm, command{Op: opHSet, NS: ns, Coll: []byte("h"), Items: []item{{Key: []byte("f"), Val: []byte("1")}}})
	rw := mustApply(t, sm, grantCmd(ns, []byte("h"), "A", 1))
	if string(rw.Data) != string(wrongType) {
		t.Fatalf("budget op on hash Data = %q, want WRONGTYPE", rw.Data)
	}
}

// A rejected init (bad mode / negative cap) via the dispatch path must NOT leave an orphaned typeBudget
// header behind — otherwise the collection would wrongly appear to exist and block other datatypes.
func TestApplyOneBudInitRejectedLeavesNoType(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/x")
	bad := initCmd(ns, coll, 1000)
	bad.Items[0].Key[0] = modeRelaxed // non-STRICT -> rejected with budBadMode
	if r := mustApply(t, sm, bad); string(r.Data) != string(budBadMode) {
		t.Fatalf("rejected init Data = %q, want BUDBADMODE", r.Data)
	}
	// No orphaned typeBudget header: an HSet on the SAME collection must succeed (not WRONGTYPE).
	r := mustApply(t, sm, command{Op: opHSet, NS: ns, Coll: coll, Items: []item{{Key: []byte("f"), Val: []byte("1")}}})
	if string(r.Data) == string(wrongType) {
		t.Fatalf("rejected init left an orphaned typeBudget header (HSet got WRONGTYPE)")
	}
}

// --- Task 6: read path ---

func TestBudStatQuery(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))
	res, err := sm.Lookup(budStatQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	st := res.(BudStat)
	if st.Cap != 1000 || st.Available != 400 || st.LeasedOut != 600 {
		t.Fatalf("BudStat = %+v, want cap1000 avail400 leased600", st)
	}
}

// B2: the conservation-invariant probe holds after a normal grant/report cycle.
func TestBudCheckQuery(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))
	mustApply(t, sm, reportCmd(ns, coll, "A", 250))
	res, err := sm.Lookup(budCheckQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	chk := res.(budCheck)
	if !chk.OK {
		t.Fatalf("invariant probe failed: %+v", chk)
	}
}

// --- Task 8: typed API integration (real Manager + shard, mirrors hincr_test.go setup) ---

func TestBudgetEndToEnd(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(2, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer m.Stop()
	c := New(m, SingleShardDirectory(2))
	waitReady(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ns, coll := []byte("pacing"), []byte("li/42/total")

	if err := c.BudgetDefine(ctx, ns, coll, 1000, modeStrict); err != nil {
		t.Fatalf("define: %v", err)
	}
	g, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 600, []byte("lease-A1"))
	if err != nil || g != 600 {
		t.Fatalf("grant = %d, %v want 600", g, err)
	}
	if err := c.BudgetReport(ctx, ns, coll, []byte("lease-A1"), 250); err != nil {
		t.Fatalf("report: %v", err)
	}
	st, err := c.BudgetStat(ctx, ns, coll, true)
	if err != nil || st.Available != 400 || st.LeasedOut != 350 || st.Spent != 250 {
		t.Fatalf("stat = %+v, %v want avail400 leased350 spent250", st, err)
	}
	if st.Available+st.LeasedOut+st.Spent != st.Cap {
		t.Fatalf("INV violated through Raft: %+v", st)
	}
}

// --- Task 11: Stage-1 verification gate ---

// detRand is a tiny deterministic xorshift64 PRNG. Seeded from a constant so the fuzz interleaving is
// reproducible — we deliberately do NOT use math/rand seeded from the clock (a money datatype's
// invariant test must replay identically on every run and in CI).
type detRand struct{ s uint64 }

func newDetRand(seed uint64) *detRand {
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15 // avoid the xorshift fixed point at 0
	}
	return &detRand{s: seed}
}

func (r *detRand) next() uint64 {
	r.s ^= r.s << 13
	r.s ^= r.s >> 7
	r.s ^= r.s << 17
	return r.s
}

// intn returns a non-negative pseudo-random int in [0, n).
func (r *detRand) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

// TestBudgetConservationFuzz interleaves grant/report/return across several leaseIDs with random
// amounts — INCLUDING negative and oversized values to exercise the B4 guards and the STRICT
// saturation/clamp paths — and asserts INV-LOCAL (cap == available+leasedOut+spent), Spent <= Cap, and
// every bucket >= 0 after EVERY op. Deterministically seeded; replays identically.
func TestBudgetConservationFuzz(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	const capUnits = int64(10_000)
	if _, err := u.applyBudInit(initCmd(ns, coll, capUnits)); err != nil {
		t.Fatalf("init: %v", err)
	}
	r := newDetRand(12345)
	// A small fixed pool of leaseIDs so grant/report/return frequently target live leases, while random
	// targeting also hits absent/returned leases (budNoLease / budNoBudget paths).
	leaseIDs := []string{"L0", "L1", "L2", "L3", "L4", "L5", "L6", "L7"}

	checkInv := func(step int, op string) {
		p, found, err := u.getPool(ns, coll)
		if err != nil || !found {
			t.Fatalf("step %d (%s): getPool found=%v err=%v", step, op, found, err)
		}
		assertInv(t, p) // cap == avail+leased+spent AND avail>=0, leased>=0, spent<=cap
		if p.Spent > p.Cap {
			t.Fatalf("step %d (%s): Spent %d > Cap %d", step, op, p.Spent, p.Cap)
		}
		if p.Available < 0 || p.LeasedOut < 0 || p.Spent < 0 {
			t.Fatalf("step %d (%s): bucket < 0: %+v", step, op, p)
		}
	}

	for i := 0; i < 5000; i++ {
		lease := leaseIDs[r.intn(len(leaseIDs))]
		// amounts span [-2000, 12999]: negatives exercise the B4 negative-draw guard, values > available
		// exercise STRICT saturation / over-report clamp.
		amt := int64(r.intn(15000) - 2000)
		switch r.intn(4) {
		case 0, 1: // grant (weighted higher so leases exist to report/return against)
			if _, err := u.applyBudGrant(grantCmd(ns, coll, lease, amt)); err != nil {
				t.Fatalf("step %d grant: %v", i, err)
			}
			checkInv(i, "grant")
		case 2: // report a (possibly negative/oversized/stale) cumulative spent
			if _, err := u.applyBudReport(reportCmd(ns, coll, lease, amt)); err != nil {
				t.Fatalf("step %d report: %v", i, err)
			}
			checkInv(i, "report")
		case 3: // return, folding a (possibly negative/oversized) final cumulative
			if _, err := u.applyBudReturn(returnCmd(ns, coll, lease, amt)); err != nil {
				t.Fatalf("step %d return: %v", i, err)
			}
			checkInv(i, "return")
		}
	}
}

// TestGrantAfterReturnReusesIdRegrantsFreshQuantity pins the documented Stage-1 hazard (B5): after a
// BudgetReturn deletes the lease row, re-granting the SAME leaseID grants FRESH quantity — there is no
// settled-lease tombstone yet (Stage 3). This asserts the limitation so it can never be mistaken for
// idempotency; conservation still holds, it's the single-use-lease_id CALLER contract that's at stake.
func TestGrantAfterReturnReusesIdRegrantsFreshQuantity(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	_, _ = u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "A", 0)) // unspent 600 returns; lease row deleted
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // SAME id -> fresh grant (the hazard)
	if decodeGrant(r.Data) != 600 {
		t.Fatalf("expected fresh re-grant of 600 (Stage-1 limitation), got %d", decodeGrant(r.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p) // conservation holds; the single-use-id contract is the caller's responsibility
	if p.Available != 400 || p.LeasedOut != 600 {
		t.Fatalf("after re-grant: avail=%d leased=%d, want 400/600", p.Available, p.LeasedOut)
	}
}

// snapshotRoundTrip exercises the REAL base_sm.go snapshot path: PrepareSnapshot -> SaveSnapshot streams
// every key under the shard prefix into a buffer; a FRESH shardSM (same shardID, so identical prefix)
// installs it via RecoverFromSnapshot (clear + replay). Mirrors how the chaos snapshot tests drive
// SaveSnapshot/RecoverFromSnapshot, minus the network transport.
func snapshotRoundTrip(t *testing.T, src *shardSM) *shardSM {
	t.Helper()
	ctx, err := src.PrepareSnapshot()
	if err != nil {
		t.Fatalf("PrepareSnapshot: %v", err)
	}
	var buf bytes.Buffer
	if err := src.SaveSnapshot(ctx, &buf, nil); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	dst := newTestSM(t) // same shardID=2 => same 8-byte prefix, so recovered keys land identically
	if err := dst.RecoverFromSnapshot(&buf, nil); err != nil {
		t.Fatalf("RecoverFromSnapshot: %v", err)
	}
	return dst
}

// TestBudgetSurvivesSnapshotRestore proves budget state (it's money) survives snapshot/restore: base_sm
// snapshots the whole shard prefix, so the pool record + lease rows ride along. After a real
// round-trip the conservation probe must still hold and the buckets must be intact (B8).
func TestBudgetSurvivesSnapshotRestore(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))

	restored := snapshotRoundTrip(t, sm)

	res, err := restored.Lookup(budCheckQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("post-restore Lookup: %v", err)
	}
	chk := res.(budCheck)
	if !chk.Exists || !chk.OK || chk.Available != 400 || chk.LeasedOut != 600 {
		t.Fatalf("post-restore budCheck = %+v, want exists/OK avail400 leased600", chk)
	}
	// The lease row must survive too: a report against it folds normally (proves it's not just the pool).
	mustApply(t, restored, reportCmd(ns, coll, "A", 250))
	res2, _ := restored.Lookup(budStatQuery{NS: ns, Coll: coll})
	st := res2.(BudStat)
	if st.Spent != 250 || st.LeasedOut != 350 {
		t.Fatalf("post-restore report fold: spent=%d leased=%d, want 250/350", st.Spent, st.LeasedOut)
	}
}
