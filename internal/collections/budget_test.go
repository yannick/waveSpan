package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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

func TestPoolRecConfigRoundTripAndBackCompat(t *testing.T) {
	p := poolRec{Cap: 1000, Available: 1000, Epoch: 1, Mode: modeStrict, Rate: 50, Burst: 100, LastRefillMs: 1, Tokens: 100,
		SelfGuardMs: 500, MaxPauseMs: 2000, DefaultTTLMs: 3000, DedupRetryWindowMs: 30000}
	got, err := decodePool(encodePool(p))
	if err != nil || got != p {
		t.Fatalf("round-trip = %+v err=%v, want %+v", got, err, p)
	}
	// a 2a.1-era record (73 bytes: pacing tail only, no config) decodes config fields as 0
	short := encodePool(poolRec{Cap: 1000, Available: 1000, Epoch: 1, Mode: modeStrict})[:73]
	g2, err := decodePool(short)
	if err != nil || g2.SelfGuardMs != 0 || g2.MaxPauseMs != 0 || g2.DefaultTTLMs != 0 || g2.DedupRetryWindowMs != 0 {
		t.Fatalf("73-byte config back-compat: %+v err=%v", g2, err)
	}
}

// TestPoolRecSpentReportedRoundTripAndBackCompat pins the Stage-2.x observability tail: SpentReported
// round-trips, and a pre-2.x 105-byte record (full config, no observability tail) decodes it as 0.
func TestPoolRecSpentReportedRoundTripAndBackCompat(t *testing.T) {
	p := poolRec{Cap: 1000, Available: 700, LeasedOut: 100, Spent: 200, Epoch: 1, Mode: modeStrict, Burst: 1000,
		SelfGuardMs: maxClockSkewMs, MaxPauseMs: 2000, DefaultTTLMs: 600, DedupRetryWindowMs: minDedupRetryWindowMs,
		SpentReported: 150}
	got, err := decodePool(encodePool(p))
	if err != nil || got != p {
		t.Fatalf("round-trip = %+v err=%v, want %+v", got, err, p)
	}
	// a pre-2.x record (105 bytes: full config, no observability tail) decodes SpentReported as 0
	short := encodePool(p)[:105]
	g2, err := decodePool(short)
	if err != nil || g2.SpentReported != 0 {
		t.Fatalf("105-byte back-compat: SpentReported = %d err=%v, want 0", g2.SpentReported, err)
	}
	// the rest of the config still decodes (105 >= 105), so only the new tail defaults to 0
	if g2.DedupRetryWindowMs != minDedupRetryWindowMs {
		t.Fatalf("105-byte record lost config: %+v", g2)
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

// encodeLeaseStage1 writes EXACTLY the original Stage-1 lease record (amount|spent|epoch|chunk(holder))
// with no timing tail, proving the append-tolerance contract: a Stage-1 lease must decode under the
// Stage-2 decoder with GrantedMs/ReclaimNotBeforeMs/ExpiresMs = 0 (§3.3).
func encodeLeaseStage1(l leaseRec) []byte {
	b := make([]byte, 0, 8*3+4+len(l.Holder))
	var num [8]byte
	putI64(num[:], l.Amount)
	b = append(b, num[:]...)
	putI64(num[:], l.Spent)
	b = append(b, num[:]...)
	binary.BigEndian.PutUint64(num[:], l.Epoch)
	b = append(b, num[:]...)
	return appendChunk(b, l.Holder)
}

func TestLeaseRecTimingFieldsRoundTripAndBackCompat(t *testing.T) {
	l := leaseRec{Holder: []byte("node-7"), Amount: 600_000, Spent: 250_000, Epoch: 3,
		GrantedMs: 1_700_000_000_000, ReclaimNotBeforeMs: 1_700_000_004_100, ExpiresMs: 1_700_000_000_600}
	got, err := decodeLease(encodeLease(l))
	if err != nil {
		t.Fatalf("decodeLease: %v", err)
	}
	if string(got.Holder) != string(l.Holder) || got.Amount != l.Amount || got.Spent != l.Spent ||
		got.Epoch != l.Epoch || got.GrantedMs != l.GrantedMs ||
		got.ReclaimNotBeforeMs != l.ReclaimNotBeforeMs || got.ExpiresMs != l.ExpiresMs {
		t.Fatalf("round-trip = %+v, want %+v", got, l)
	}
	// a Stage-1 lease (no timing tail) decodes the three fields as 0
	old := encodeLeaseStage1(leaseRec{Holder: []byte("node-7"), Amount: 600_000, Spent: 250_000, Epoch: 3})
	g2, err := decodeLease(old)
	if err != nil || g2.GrantedMs != 0 || g2.ReclaimNotBeforeMs != 0 || g2.ExpiresMs != 0 {
		t.Fatalf("stage-1 lease back-compat: %+v err=%v", g2, err)
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
	// Stage-1 shape: no pacing (rate 0), burst == cap, no timing config.
	return initCmdFull(ns, coll, capacity, 0, capacity, 0, 0, 0, 0)
}

// initCmdFull builds an init key carrying the full Stage-2 config:
// mode(1) | cap(8) | rate(8) | burst(8) | selfGuard(8) | maxPause(8) | defaultTtl(8) | dedupRetryWindow(8).
func initCmdFull(ns, coll []byte, capacity, rate, burst, selfGuardMs, maxPauseMs, defaultTTLMs, dedupRetryWindowMs int64) command {
	k := make([]byte, 1+8*7)
	k[0] = modeStrict
	putI64(k[1:], capacity)
	putI64(k[9:], rate)
	putI64(k[17:], burst)
	putI64(k[25:], selfGuardMs)
	putI64(k[33:], maxPauseMs)
	putI64(k[41:], defaultTTLMs)
	putI64(k[49:], dedupRetryWindowMs)
	return command{Op: opBudInit, NS: ns, Coll: coll, Items: []item{{Key: k}}}
}

// initCmdPaced defines a paced pool with explicit rate/burst (timing config stays 0 -> non-expiring).
func initCmdPaced(ns, coll []byte, capacity, rate, burst int64) command {
	return initCmdFull(ns, coll, capacity, rate, burst, 0, 0, 0, 0)
}

// grant: leaseID in Items[0].Key; Val = amount(8) | grantedMs(8) | ttlOverride(8) | holder(rest). The
// tests use leaseID == holder string purely for brevity; in production they differ (leaseID is
// per-refill). Idem stays EMPTY. grantCmd is the non-paced shorthand (grantedMs=0, ttl=0).
func grantCmd(ns, coll []byte, holder string, amt int64) command {
	return grantCmdT(ns, coll, holder, amt, 0, 0)
}

// grantCmdT builds a grant carrying the leader-stamped grantedMs and a per-grant ttl override.
func grantCmdT(ns, coll []byte, holder string, amt, grantedMs, ttl int64) command {
	v := make([]byte, 24+len(holder))
	putI64(v[0:], amt)
	putI64(v[8:], grantedMs)
	putI64(v[16:], ttl)
	copy(v[24:], holder)
	return command{Op: opBudGrant, NS: ns, Coll: coll, Items: []item{{Key: []byte(holder), Val: v}}}
}

// mustGrant applies a paced/timed grant and returns the ProposeResult (failing the test on a hard error).
func mustGrant(t *testing.T, u *updateCtx, ns, coll []byte, holder string, amt, grantedMs, ttl int64) ProposeResult {
	t.Helper()
	r, err := u.applyBudGrant(grantCmdT(ns, coll, holder, amt, grantedMs, ttl))
	if err != nil {
		t.Fatalf("grant %s: %v", holder, err)
	}
	return r
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

// reportCmdH / returnCmdH build holder-bound report/return commands (Val = cumulative(8)|holder(rest)) so
// tests can exercise the holder-match guard. The plain reportCmd/returnCmd above carry NO holder (lenient).
func reportCmdH(ns, coll []byte, leaseID, holder string, cum int64) command {
	v := make([]byte, 8+len(holder))
	putI64(v, cum)
	copy(v[8:], holder)
	return command{Op: opBudReport, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}
func returnCmdH(ns, coll []byte, leaseID, holder string, cum int64) command {
	v := make([]byte, 8+len(holder))
	putI64(v, cum)
	copy(v[8:], holder)
	return command{Op: opBudReturn, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}

// expireCmd builds a leader-proposed forced-expiry command carrying the leader-stamped sweepNowMs.
func expireCmd(ns, coll []byte, leaseID string, sweepNowMs int64) command {
	v := make([]byte, 8)
	putI64(v, sweepNowMs)
	return command{Op: opBudExpire, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}

// reconcileCmd builds a controller-proposed Σ-acked reconcile carrying the authoritative trueAckedUnits.
func reconcileCmd(ns, coll []byte, trueAcked int64) command {
	v := make([]byte, 8)
	putI64(v, trueAcked)
	return command{Op: opBudReconcile, NS: ns, Coll: coll, Items: []item{{Key: nil, Val: v}}}
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

// TestHolderMatchOnReportAndReturn pins the Stage-2.x holder-binding guard: a Report/Return whose bound
// holder does not match the lease's grantee is rejected (budWrongHolder) and mutates NO accounting; a
// matching holder is accepted; an omitted (empty) holder stays lenient (back-compat). grantCmd records
// the holder == the leaseID string ("L"), so "EVIL" mismatches and "L" matches.
func TestHolderMatchOnReportAndReturn(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	_, _ = u.applyBudGrant(grantCmd(ns, coll, "L", 600)) // lease.Holder == "L"

	// wrong holder on report -> budWrongHolder, no accounting change.
	r, err := u.applyBudReport(reportCmdH(ns, coll, "L", "EVIL", 100))
	if err != nil {
		t.Fatalf("report(wrong holder): %v", err)
	}
	if string(r.Data) != string(budWrongHolder) {
		t.Fatalf("report wrong holder Data = %q, want BUDWRONGHOLDER", r.Data)
	}
	if p, _, _ := u.getPool(ns, coll); p.Spent != 0 || p.LeasedOut != 600 {
		t.Fatalf("wrong-holder report mutated pool: spent=%d leased=%d, want 0/600", p.Spent, p.LeasedOut)
	}

	// matching holder on report -> accepted (folds the spend).
	r2, err := u.applyBudReport(reportCmdH(ns, coll, "L", "L", 100))
	if err != nil {
		t.Fatalf("report(matching holder): %v", err)
	}
	if string(r2.Data) == string(budWrongHolder) {
		t.Fatal("matching-holder report rejected")
	}
	if p, _, _ := u.getPool(ns, coll); p.Spent != 100 {
		t.Fatalf("matching report spent=%d, want 100", p.Spent)
	}

	// omitted holder (empty) on report -> lenient, accepted.
	if r3, _ := u.applyBudReport(reportCmd(ns, coll, "L", 200)); string(r3.Data) == string(budWrongHolder) {
		t.Fatal("omitted-holder report wrongly rejected (back-compat broken)")
	}
	if p, _, _ := u.getPool(ns, coll); p.Spent != 200 {
		t.Fatalf("omitted-holder report spent=%d, want 200", p.Spent)
	}

	// wrong holder on return -> budWrongHolder, lease NOT settled.
	rr, err := u.applyBudReturn(returnCmdH(ns, coll, "L", "EVIL", 200))
	if err != nil {
		t.Fatalf("return(wrong holder): %v", err)
	}
	if string(rr.Data) != string(budWrongHolder) {
		t.Fatalf("return wrong holder Data = %q, want BUDWRONGHOLDER", rr.Data)
	}
	if _, found, _ := u.getLease(ns, coll, []byte("L")); !found {
		t.Fatal("wrong-holder return settled the lease (must not)")
	}

	// matching holder on return -> accepted (settles, credits the remainder).
	rr2, err := u.applyBudReturn(returnCmdH(ns, coll, "L", "L", 200))
	if err != nil {
		t.Fatalf("return(matching holder): %v", err)
	}
	if string(rr2.Data) == string(budWrongHolder) {
		t.Fatal("matching-holder return rejected")
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 800 || p.LeasedOut != 0 || p.Spent != 200 {
		t.Fatalf("after matching return: avail=%d leased=%d spent=%d, want 800/0/200", p.Available, p.LeasedOut, p.Spent)
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

// TestSpentReportedTracksReportsNotExpiry pins the Stage-2.x observability semantics: a holder report
// grows BOTH Spent and SpentReported, while a forced expiry's pessimistic DEBIT grows Spent ONLY — never
// SpentReported. The invariant SpentReported <= Spent holds throughout.
func TestSpentReportedTracksReportsNotExpiry(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000) // grantedMs=1000 ttl=3000

	// a report grows BOTH Spent and SpentReported by the delta.
	if _, err := u.applyBudReport(reportCmd(ns, coll, "L", 40)); err != nil {
		t.Fatalf("report: %v", err)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Spent != 40 || p.SpentReported != 40 {
		t.Fatalf("after report: spent=%d spentReported=%d, want 40/40", p.Spent, p.SpentReported)
	}

	// forced expiry DEBITS the remaining 60 into Spent (un-attested) but NOT SpentReported.
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	p, _, _ = u.getPool(ns, coll)
	if p.Spent != 100 {
		t.Fatalf("after expiry: spent=%d, want 100 (debit remainder)", p.Spent)
	}
	if p.SpentReported != 40 {
		t.Fatalf("after expiry: spentReported=%d, want 40 (forced expiry must not bump it)", p.SpentReported)
	}
	if p.SpentReported > p.Spent {
		t.Fatalf("invariant broken: SpentReported %d > Spent %d", p.SpentReported, p.Spent)
	}
}

// TestSpentReportedGrowsOnReturnFold pins that an ATTESTED graceful Return's final-report fold grows
// SpentReported alongside Spent (the holder attested the spend), unlike a forced expiry.
func TestSpentReportedGrowsOnReturnFold(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	if _, err := u.applyBudReport(reportCmd(ns, coll, "L", 30)); err != nil {
		t.Fatalf("report: %v", err)
	}
	// return folds a final cumulative of 70: Spent and SpentReported both advance to 70.
	if _, err := u.applyBudReturn(returnCmd(ns, coll, "L", 70)); err != nil {
		t.Fatalf("return: %v", err)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Spent != 70 || p.SpentReported != 70 {
		t.Fatalf("after return-fold: spent=%d spentReported=%d, want 70/70", p.Spent, p.SpentReported)
	}
}

// --- §3.8: external Σ-acked reconciliation (BudgetReconcile) — recover forced-expiry stranding ---

// reconciledRecovered decodes the int64 recovered amount an applyBudReconcile success returns in Data.
func reconciledRecovered(t *testing.T, r ProposeResult) int64 {
	t.Helper()
	if len(r.Data) != 8 {
		t.Fatalf("reconcile result Data = %x, want 8-byte recovered int64", r.Data)
	}
	return getI64(r.Data)
}

// TestReconcileRecoversStranding is the canonical recovery: a forced expiry pessimistically DEBITS the
// whole remainder to spent (un-attested holder), stranding the genuinely-unspent portion as underspend.
// The controller's authoritative Σ-acked total (30) re-credits spent down to its true value, recovering 70
// of stranded headroom back to available — WITHOUT overspend. The conservation invariant still holds.
func TestReconcileRecoversStranding(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	if _, err := u.applyBudReport(reportCmd(ns, coll, "L", 30)); err != nil {
		t.Fatalf("report: %v", err)
	}
	// forced expiry DEBITS the un-attested 70 into spent: spent=100, available=900, SpentReported stays 30.
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Spent != 100 || p.Available != 900 || p.LeasedOut != 0 || p.SpentReported != 30 {
		t.Fatalf("post-expire = spent%d avail%d leased%d reported%d, want 100/900/0/30", p.Spent, p.Available, p.LeasedOut, p.SpentReported)
	}
	// reconcile with the external ground-truth acked total (30): re-credit spent to 30, recover 70.
	r, err := u.applyBudReconcile(reconcileCmd(ns, coll, 30))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := reconciledRecovered(t, r); got != 70 {
		t.Fatalf("recovered = %d, want 70 (100 booked - 30 true)", got)
	}
	p, _, _ = u.getPool(ns, coll)
	assertInv(t, p) // budCheck.OK: cap == available+leasedOut+spent, no bucket out of range
	if p.Spent != 30 || p.Available != 970 || p.LeasedOut != 0 {
		t.Fatalf("post-reconcile = spent%d avail%d leased%d, want 30/970/0", p.Spent, p.Available, p.LeasedOut)
	}
}

// TestReconcileClampsFloorAtSpentReported pins the SAFETY floor: a trueAcked BELOW the provably-reported
// spend is clamped UP to SpentReported — reconcile can never under-credit below what holders attested
// (which would let already-reported-spent budget be granted and served again).
func TestReconcileClampsFloorAtSpentReported(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	if _, err := u.applyBudReport(reportCmd(ns, coll, "L", 40)); err != nil { // attest 40
		t.Fatalf("report: %v", err)
	}
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)); err != nil { // DEBIT -> spent=100
		t.Fatalf("expire: %v", err)
	}
	// controller (wrongly/stale) claims only 10 acked — below the attested floor of 40. Clamp UP to 40.
	r, err := u.applyBudReconcile(reconcileCmd(ns, coll, 10))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := reconciledRecovered(t, r); got != 60 { // 100 - clamp(10,40,1000)=40 -> recovered 60
		t.Fatalf("recovered = %d, want 60 (clamped to SpentReported floor 40)", got)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Spent != 40 || p.Available != 960 {
		t.Fatalf("post-reconcile = spent%d avail%d, want spent40 avail960 (floor at SpentReported)", p.Spent, p.Available)
	}
}

// TestReconcileCannotExceedCap pins the Cap ceiling: a trueAcked ABOVE cap is clamped to cap (spent can
// never exceed the pool). With no outstanding leases, available lands at 0 and conservation holds.
func TestReconcileCannotExceedCap(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	// grant + report + return everything so leasedOut=0, then reconcile with an absurd acked total.
	mustGrant(t, u, ns, coll, "L", 100, 0, 0)
	if _, err := u.applyBudReturn(returnCmd(ns, coll, "L", 100)); err != nil { // spent=100, leased=0, avail=900
		t.Fatalf("return: %v", err)
	}
	r, err := u.applyBudReconcile(reconcileCmd(ns, coll, 5000)) // 5000 > cap 1000
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := reconciledRecovered(t, r); got != 100-1000 { // recovered = 100 - clamp(5000,_,1000)=1000 -> -900
		t.Fatalf("recovered = %d, want -900 (acked clamped to cap raises spent)", got)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Spent != 1000 || p.Available != 0 || p.LeasedOut != 0 {
		t.Fatalf("post-reconcile = spent%d avail%d leased%d, want 1000/0/0 (clamped at cap)", p.Spent, p.Available, p.LeasedOut)
	}
}

// TestReconcileWithOutstandingLeasesNeverNegativeAvailable pins the never-negative rule: when the
// reconciled spend plus the still-outstanding leasedOut exceeds cap, Available is floored at 0 (never
// negative) and LeasedOut is NOT lowered — the transient over-commit drains as those leases settle.
func TestReconcileWithOutstandingLeasesNeverNegativeAvailable(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	// hold 600 out on a live lease (leasedOut=600, available=400, spent=0).
	mustGrant(t, u, ns, coll, "L", 600, 0, 0)
	p, _, _ := u.getPool(ns, coll)
	if p.LeasedOut != 600 || p.Available != 400 {
		t.Fatalf("pre-reconcile = leased%d avail%d, want 600/400", p.LeasedOut, p.Available)
	}
	// controller's external ledger says 700 acked already (holders served but never reported). target=700;
	// 700 + 600 leasedOut = 1300 > cap 1000 -> available would be -300, must floor at 0 (over-commit drains).
	if _, err := u.applyBudReconcile(reconcileCmd(ns, coll, 700)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	p, _, _ = u.getPool(ns, coll)
	if p.Available != 0 {
		t.Fatalf("available = %d, want 0 (floored, never negative)", p.Available)
	}
	if p.LeasedOut != 600 {
		t.Fatalf("leasedOut = %d, want 600 (over-commit left to drain, not lowered)", p.LeasedOut)
	}
	if p.Spent != 700 {
		t.Fatalf("spent = %d, want 700 (reconciled to true acked)", p.Spent)
	}
	if p.Available < 0 {
		t.Fatal("available went negative — the never-negative rule was violated")
	}
}

// TestReconcileNoBudget pins that reconcile against a pool that does not exist returns budNoBudget (B9),
// not an error and not a stray pool.
func TestReconcileNoBudget(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("nope")
	r, err := u.applyBudReconcile(reconcileCmd(ns, coll, 50))
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !bytes.Equal(r.Data, budNoBudget) {
		t.Fatalf("reconcile no-budget Data = %q, want budNoBudget", r.Data)
	}
	if _, found, _ := u.getPool(ns, coll); found {
		t.Fatal("reconcile created a stray pool")
	}
}

// findOp returns the LAST StoreOp in u.ops whose key equals key (the effective write, since later ops
// win). Budget apply funcs append index/row writes to u.ops without flushing, so tests inspect them here.
func findOp(u *updateCtx, key []byte) (storage.StoreOp, bool) {
	for i := len(u.ops) - 1; i >= 0; i-- {
		if bytes.Equal(u.ops[i].Key, key) {
			return u.ops[i], true
		}
	}
	return storage.StoreOp{}, false
}

// --- Task 2b.2: timed grant writes the reclaim deadline + shard-level expiry index ---

func TestGrantWritesExpiryIndex(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	// timed budget via per-grant ttl override: selfGuard>=skew, maxPause=2000, dedup>=floor.
	if _, err := u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 2000, 0, minDedupRetryWindowMs)); err != nil {
		t.Fatalf("init: %v", err)
	}
	const grantedMs, ttl = int64(1_000_000), int64(3000)
	mustGrant(t, u, ns, coll, "L", 100, grantedMs, ttl)
	// the lease stores the REPLICATED reclaim deadline = grantedMs + ttl + 3*skew + maxPause
	l, found, _ := u.getLease(ns, coll, []byte("L"))
	if !found {
		t.Fatal("lease row missing")
	}
	wantReclaim := grantedMs + ttl + 3*maxClockSkewMs + 2000
	if l.ReclaimNotBeforeMs != wantReclaim || l.GrantedMs != grantedMs || l.ExpiresMs != grantedMs+ttl {
		t.Fatalf("lease timing = granted %d reclaim %d expires %d, want %d / %d / %d",
			l.GrantedMs, l.ReclaimNotBeforeMs, l.ExpiresMs, grantedMs, wantReclaim, grantedMs+ttl)
	}
	// the shard-level expiry index carries a be(reclaim)|chunk(ns)|chunk(coll)|leaseID entry (value-less)
	if op, ok := findOp(u, u.s.budExpKey(wantReclaim, ns, coll, []byte("L"))); !ok || op.Delete {
		t.Fatalf("expiry index entry not written (ok=%v delete=%v)", ok, op.Delete)
	}
}

// ttl==0 (non-expiring, Stage-1 behavior) writes NO expiry-index entry and leaves the timing fields 0.
func TestUntimedGrantWritesNoExpiryIndex(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	mustGrant(t, u, ns, coll, "L", 100, 1_000_000, 0)
	l, _, _ := u.getLease(ns, coll, []byte("L"))
	if l.ReclaimNotBeforeMs != 0 || l.ExpiresMs != 0 {
		t.Fatalf("non-timed lease has timing: reclaim=%d expires=%d", l.ReclaimNotBeforeMs, l.ExpiresMs)
	}
	// scan u.ops for ANY subBudExp write
	space := u.s.budExpSpace()
	for _, op := range u.ops {
		if len(op.Key) >= len(space) && bytes.Equal(op.Key[:len(space)], space) {
			t.Fatalf("non-timed grant wrote an expiry index entry: %x", op.Key)
		}
	}
}

// Gap#2: a retried Draw (same leaseID) hits the idempotency branch and must echo BYTE-IDENTICAL timing
// reconstructed from the stored lease + pool, NOT the retry's grantedMs/ttl (which apply ignores).
func TestIdempotentTimedGrantEchoesSameTiming(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 2000, 0, minDedupRetryWindowMs))
	r1 := mustGrant(t, u, ns, coll, "L", 100, 1_000_000, 3000)
	r2 := mustGrant(t, u, ns, coll, "L", 100, 9_999_999, 7777) // retry: different grantedMs/ttl must be ignored
	if !bytes.Equal(r1.Data, r2.Data) {
		t.Fatalf("idempotent timing echo not byte-identical:\n r1=%x\n r2=%x", r1.Data, r2.Data)
	}
	g := decodeGrantResult(r2.Data)
	if g.Granted != 100 || g.GrantedMs != 1_000_000 || g.TTLMs != 3000 || g.SelfGuardMs != maxClockSkewMs || g.MaxPauseMs != 2000 {
		t.Fatalf("echo = %+v, want granted100 granted@1e6 ttl3000 guard500 pause2000", g)
	}
}

// --- Task 2b.4: opBudExpire DEBIT settlement (the money-critical path) ---

// reclaimOf is the replicated reclaim deadline a timed grant stamps: grantedMs + ttl + 3*skew + maxPause.
func reclaimOf(grantedMs, ttl, maxPause int64) int64 {
	return grantedMs + ttl + 3*maxClockSkewMs + maxPause
}

// timedBudget defines a properly-configured timed budget (selfGuard>=skew, dedup>=floor) so the I2 grant
// gate admits a timed lease — maxPause is explicit so tests can compute the reclaim deadline.
func timedBudget(u *updateCtx, ns, coll []byte, capUnits, maxPause int64) {
	_, _ = u.applyBudInit(initCmdFull(ns, coll, capUnits, 0, capUnits, maxClockSkewMs, maxPause, 0, minDedupRetryWindowMs))
}

func TestApplyBudExpireDebits(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000) // grantedMs=1000 ttl=3000
	_, _ = u.applyBudReport(reportCmd(ns, coll, "L", 50))
	// expire at a sweepNow past the reclaim deadline
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	// DEBIT: the unreported 50 is booked as spent, NOT returned to available
	if p.Spent != 100 || p.Available != 900 || p.LeasedOut != 0 {
		t.Fatalf("expire debit: spent=%d avail=%d leased=%d want 100/900/0", p.Spent, p.Available, p.LeasedOut)
	}
	if _, found, _ := u.getLease(ns, coll, []byte("L")); found {
		t.Fatal("lease row not deleted")
	}
	tr, found, _ := u.getTomb(ns, coll, []byte("L"))
	if !found || tr.Reason != reasonExpire || tr.FinalSpent != 50 {
		t.Fatalf("tombstone = %+v found=%v, want reasonExpire finalSpent50", tr, found)
	}
	if op, ok := findOp(u, u.s.budExpKey(reclaimOf(1000, 3000, 2000), ns, coll, []byte("L"))); !ok || !op.Delete {
		t.Fatalf("expiry index not deleted (ok=%v delete=%v)", ok, op.Delete)
	}
}

// sweepNow BEFORE the replicated reclaim deadline -> no-op (a renewed lease's later deadline; the sweep
// saw a stale index): lease intact, no tombstone, pool untouched.
func TestApplyBudExpireStaleSkips(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	reclaim := reclaimOf(1000, 3000, 2000)
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", reclaim-1)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.LeasedOut != 100 || p.Spent != 0 {
		t.Fatalf("stale expire mutated pool: leased=%d spent=%d want 100/0", p.LeasedOut, p.Spent)
	}
	if _, found, _ := u.getLease(ns, coll, []byte("L")); !found {
		t.Fatal("stale expire deleted the lease")
	}
	if _, settled, _ := u.getTomb(ns, coll, []byte("L")); settled {
		t.Fatal("stale expire wrote a tombstone")
	}
}

// expire (DEBIT), then a late graceful return on the same lease -> tombstone no-op (single settlement;
// the late return must NOT credit on top of the debit).
func TestExpireThenReturnIdempotent(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	_, _ = u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)) // DEBIT 100
	p1, _, _ := u.getPool(ns, coll)
	r, err := u.applyBudReturn(returnCmd(ns, coll, "L", 30))
	if err != nil {
		t.Fatalf("return after expire: %v", err)
	}
	if r.Value != 0 {
		t.Fatalf("return after expire credited %d, want 0 (tombstone no-op)", r.Value)
	}
	p2, _, _ := u.getPool(ns, coll)
	assertInv(t, p2)
	if p2 != p1 || p2.Spent != 100 || p2.Available != 900 {
		t.Fatalf("double settlement: %+v -> %+v (want spent100 avail900 unchanged)", p1, p2)
	}
}

// return (CREDIT), then a late forced expiry on the same lease -> tombstone no-op (must NOT debit on top
// of the credit; the graceful settlement stands).
func TestReturnThenExpireIdempotent(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	timedBudget(u, ns, coll, 1000, 2000)
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)
	_, _ = u.applyBudReport(reportCmd(ns, coll, "L", 30))
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "L", 40)) // CREDIT remainder 60
	p1, _, _ := u.getPool(ns, coll)
	if _, err := u.applyBudExpire(expireCmd(ns, coll, "L", 9_999_999)); err != nil {
		t.Fatalf("expire after return: %v", err)
	}
	p2, _, _ := u.getPool(ns, coll)
	assertInv(t, p2)
	if p2 != p1 || p2.Spent != 40 || p2.Available != 960 || p2.LeasedOut != 0 {
		t.Fatalf("late expire mutated settlement: %+v -> %+v (want spent40 avail960 leased0)", p1, p2)
	}
	tr, found, _ := u.getTomb(ns, coll, []byte("L"))
	if !found || tr.Reason != reasonReturn {
		t.Fatalf("late expire changed tombstone reason: %+v", tr)
	}
}

// budExpiryDueQuery scans the FLUSHED shard-level index (real Update->BatchRC path, then a snapshot
// Lookup) and returns due leases at or before NowMs — proving the index is persisted, not just queued.
func TestBudExpiryDueQueryScans(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("p"), []byte("c")
	mustApply(t, sm, initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 2000, 0, minDedupRetryWindowMs))
	mustApply(t, sm, grantCmdT(ns, coll, "L", 100, 1000, 3000))
	reclaim := reclaimOf(1000, 3000, 2000)
	res, err := sm.Lookup(budExpiryDueQuery{NowMs: reclaim - 1, Limit: 0})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if due, _ := res.([]dueBudLease); len(due) != 0 {
		t.Fatalf("premature due = %d, want 0", len(due))
	}
	res, _ = sm.Lookup(budExpiryDueQuery{NowMs: reclaim, Limit: 0})
	due, _ := res.([]dueBudLease)
	if len(due) != 1 || string(due[0].LeaseID) != "L" || string(due[0].NS) != "p" ||
		string(due[0].Coll) != "c" || due[0].ReclaimMs != reclaim {
		t.Fatalf("due = %+v, want one L/p/c@%d", due, reclaim)
	}
}

// --- Task 2b.3: tombstone settlement symmetry on graceful Return ---

func TestReturnWritesTombstoneAndCredits(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 2000, 0, minDedupRetryWindowMs))
	mustGrant(t, u, ns, coll, "L", 100, 1_000_000, 3000) // timed lease
	_, _ = u.applyBudReport(reportCmd(ns, coll, "L", 30))
	// graceful return folds final spent 40 -> CREDITS the unspent 60 back to available
	r, err := u.applyBudReturn(returnCmd(ns, coll, "L", 40))
	if err != nil {
		t.Fatalf("return: %v", err)
	}
	if r.Value != 60 {
		t.Fatalf("return remainder = %d, want 60", r.Value)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 960 || p.LeasedOut != 0 || p.Spent != 40 { // 1000 -100 grant +60 unspent credit
		t.Fatalf("after return: avail=%d leased=%d spent=%d want 960/0/40", p.Available, p.LeasedOut, p.Spent)
	}
	if _, found, _ := u.getLease(ns, coll, []byte("L")); found {
		t.Fatal("lease row not deleted")
	}
	tr, found, _ := u.getTomb(ns, coll, []byte("L"))
	if !found || tr.FinalSpent != 40 || tr.Reason != reasonReturn {
		t.Fatalf("tombstone = %+v found=%v, want finalSpent40 reasonReturn", tr, found)
	}
	wantReclaim := int64(1_000_000) + 3000 + 3*maxClockSkewMs + 2000
	if op, ok := findOp(u, u.s.budExpKey(wantReclaim, ns, coll, []byte("L"))); !ok || !op.Delete {
		t.Fatalf("expiry index entry not deleted on return (ok=%v delete=%v)", ok, op.Delete)
	}
}

// A timed lease that has been returned is SETTLED: re-granting the same leaseID returns budSettled, never
// a fresh grant (B5 closure for the timed path, contrast the Stage-1 untimed hazard test below).
func TestGrantAfterReturnSettledNoRegrant(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 2000, 0, minDedupRetryWindowMs))
	mustGrant(t, u, ns, coll, "L", 100, 1_000_000, 3000)
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "L", 0)) // settle: credit 100 back
	r, _ := u.applyBudGrant(grantCmdT(ns, coll, "L", 100, 2_000_000, 3000))
	if string(r.Data) != string(budSettled) {
		t.Fatalf("re-grant after settle Data = %q, want BUDSETTLED", r.Data)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 1000 || p.LeasedOut != 0 {
		t.Fatalf("settled re-grant changed pool: avail=%d leased=%d want 1000/0", p.Available, p.LeasedOut)
	}
}

// --- Task 2a.2: token-bucket pacing ---

func TestApplyBudGrantPaced(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	// cap 1000, rate 100 u/s, burst 100 -> seeded tokens=100
	if _, err := u.applyBudInit(initCmdPaced(ns, coll, 1000, 100, 100)); err != nil {
		t.Fatalf("init: %v", err)
	}
	// first paced grant at grantedMs=1000: lazy-init lastRefillMs=1000, accrued=0, tokens=100 (seeded burst)
	r := mustGrant(t, u, ns, coll, "A", 80, 1000, 0)
	if decodeGrant(r.Data) != 80 {
		t.Fatalf("paced grant=%d want 80 (from seeded burst)", decodeGrant(r.Data))
	}
	// tokens now 20; a 50-draw saturates to 20 even though available is huge
	r2 := mustGrant(t, u, ns, coll, "B", 50, 1000, 0)
	if decodeGrant(r2.Data) != 20 {
		t.Fatalf("paced grant=%d want 20 (token-bound)", decodeGrant(r2.Data))
	}
	// 1s later: +100 tokens accrued, capped at burst=100
	r3 := mustGrant(t, u, ns, coll, "C", 100, 2000, 0)
	if decodeGrant(r3.Data) != 100 {
		t.Fatalf("after 1s grant=%d want 100", decodeGrant(r3.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
}

// rate=0 => Stage-1 behavior: grant=min(amount,available), tokens ignored entirely.
func TestApplyBudGrantRateZeroUnchanged(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000)) // rate 0
	r := mustGrant(t, u, ns, coll, "A", 900, 1000, 0)
	if decodeGrant(r.Data) != 900 { // not token-bound (no pacing)
		t.Fatalf("rate-0 grant=%d want 900", decodeGrant(r.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Tokens != 0 || p.LastRefillMs != 0 { // pacing state untouched when rate==0
		t.Fatalf("rate-0 touched pacing state: tokens=%d lastRefill=%d", p.Tokens, p.LastRefillMs)
	}
}

// A huge rate with lazy-init must NOT accrue rate*grantedMs on the first grant (lastRefillMs starts 0).
func TestPacingNoOverflow(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmdPaced(ns, coll, 1_000_000, 1_000_000_000, 100)) // burst 100
	// grantedMs is a real epoch-ms value; lazy-init makes elapsed=0, so the bucket is just the seeded burst.
	r := mustGrant(t, u, ns, coll, "A", 1_000_000, 1_700_000_000_000, 0)
	if decodeGrant(r.Data) != 100 { // seeded burst, NOT rate*grantedMs
		t.Fatalf("lazy-init overflow: grant=%d want 100", decodeGrant(r.Data))
	}
}

// rate > burst must still accrue (I4: multiply-before-divide; the (burst/rate)*1000 form would wedge at 0).
func TestPacingRateAboveBurst(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmdPaced(ns, coll, 10_000, 200, 100)) // rate 200 > burst 100
	_ = mustGrant(t, u, ns, coll, "A", 100, 1000, 0)                // drains seeded tokens to 0
	// 500ms later: accrued = 200*500/1000 = 100 -> a 50-draw must succeed (pacing did not wedge at 0)
	r := mustGrant(t, u, ns, coll, "B", 50, 1500, 0)
	if decodeGrant(r.Data) != 50 {
		t.Fatalf("rate>burst wedged: grant=%d want 50", decodeGrant(r.Data))
	}
}

// --- Task 2a.3: admission guards + per-budget timing config ---

func TestBudgetDefineRejectsOverflowBurst(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	r, _ := u.applyBudInit(initCmdFull(ns, coll, 1000, 100, pacingBurstCeil+1, 0, 0, 0, 0))
	if string(r.Data) != string(budBadParam) {
		t.Fatalf("overflow burst Data=%q want BUDBADPARAM", r.Data)
	}
	if _, found, _ := u.getPool(ns, coll); found {
		t.Fatal("rejected init created a pool")
	}
}

func TestBudgetDefineRejectsLowSelfGuard(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	// defaultTtl > 0 with self_guard below the clock-skew bound is rejected (I2: TTL-gated, not rate-gated).
	r, _ := u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs-1, 0, 5000, minDedupRetryWindowMs))
	if string(r.Data) != string(budBadParam) {
		t.Fatalf("low self_guard Data=%q want BUDBADPARAM", r.Data)
	}
}

func TestBudgetDefineRejectsLowDedupWindow(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	// defaultTtl > 0 with dedup window below the RPC-retry budget is rejected (I3).
	r, _ := u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, maxClockSkewMs, 0, 5000, minDedupRetryWindowMs-1))
	if string(r.Data) != string(budBadParam) {
		t.Fatalf("low dedup window Data=%q want BUDBADPARAM", r.Data)
	}
}

// I2 authoritative gate: a per-grant ttlOverride makes ANY budget timed, so even a pool that passed
// Define with self_guard=0 (because defaultTtl=0) must reject a timed grant at apply.
func TestTimedGrantRejectsSmallSelfGuard(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	if _, err := u.applyBudInit(initCmdFull(ns, coll, 1000, 0, 1000, 0, 0, 0, 0)); err != nil {
		t.Fatalf("init: %v", err) // defaultTtl=0 -> passes Define even with self_guard=0
	}
	r := mustGrant(t, u, ns, coll, "A", 100, 1000, 5000) // ttlOverride=5000 makes the lease timed
	if string(r.Data) != string(budBadParam) {
		t.Fatalf("timed grant on small self_guard Data=%q want BUDBADPARAM", r.Data)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Available != 1000 { // the rejected grant must not have debited
		t.Fatalf("rejected timed grant debited available to %d", p.Available)
	}
}

// Gap#1: every budget sentinel returned in ProposeResult.Data must be length-distinguishable from a
// success encodeGrantResult payload, so Collections.BudgetGrant can switch sentinels first and never
// misdecode one as a grant. (The I2 gate makes budBadParam reachable on the grant path.)
func TestGrantSentinelsDistinctFromSuccess(t *testing.T) {
	success := encodeGrantResult(GrantResult{Granted: 123, GrantedMs: 456})
	for _, s := range [][]byte{budNoCapacity, budNoBudget, budBadParam, budBadMode, budExists, budNoLease} {
		if len(s) == len(success) {
			t.Fatalf("sentinel %q has the success length %d -> would be misdecoded as a grant", s, len(success))
		}
	}
	if got := decodeGrantResult(success); got.Granted != 123 || got.GrantedMs != 456 {
		t.Fatalf("decodeGrantResult round-trip = %+v, want 123/456", got)
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

	if err := c.BudgetDefine(ctx, ns, coll, 1000, modeStrict, BudgetConfig{}); err != nil {
		t.Fatalf("define: %v", err)
	}
	g, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 600, []byte("lease-A1"), 0)
	if err != nil || g.Granted != 600 {
		t.Fatalf("grant = %+v, %v want 600", g, err)
	}
	if err := c.BudgetReport(ctx, ns, coll, []byte("lease-A1"), []byte("node-A"), 250); err != nil {
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

// TestBudgetReconcileEndToEnd drives the §3.8 reconcile path through Raft: a timed lease is force-expired
// (DEBIT strands the unspent remainder), then the controller's typed BudgetReconcile re-credits spent to
// the external Σ-acked total — recovering the stranded headroom, with the conservation invariant (budCheck.OK)
// still holding through the full consensus path, and the recovered amount returned.
func TestBudgetReconcileEndToEnd(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ns, coll := []byte("pacing"), []byte("li/reconcile")

	// timed budget; grant 600, attest 100, then let the lease force-expire (DEBIT the unreported 500).
	cfg := BudgetConfig{Rate: 1_000_000, Burst: 1000, SelfGuardMs: maxClockSkewMs, MaxPauseMs: 0,
		DefaultTTLMs: 600, DedupRetryWindowMs: minDedupRetryWindowMs}
	if err := c.BudgetDefine(ctx, ns, coll, 1000, modeStrict, cfg); err != nil {
		t.Fatalf("define: %v", err)
	}
	if _, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 600, []byte("lease-R"), 0); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if err := c.BudgetReport(ctx, ns, coll, []byte("lease-R"), []byte("node-A"), 100); err != nil {
		t.Fatalf("report: %v", err)
	}
	// wait for the leader's sweep to force-expire the lease (DEBIT): leasedOut -> 0, spent -> 600.
	deadline := time.Now().Add(20 * time.Second)
	var st BudStat
	for {
		st, _ = c.BudgetStat(ctx, ns, coll, true)
		if st.LeasedOut == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease never auto-expired (leased=%d spent=%d)", st.LeasedOut, st.Spent)
		}
		time.Sleep(150 * time.Millisecond)
	}
	if st.Spent != 600 || st.Available != 400 || st.SpentReported != 100 {
		t.Fatalf("post-expire = spent%d avail%d reported%d, want 600/400/100", st.Spent, st.Available, st.SpentReported)
	}
	// controller reconciles to the external ground-truth acked total (250): recover 600-250 = 350.
	recovered, err := c.BudgetReconcile(ctx, ns, coll, 250)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if recovered != 350 {
		t.Fatalf("recovered = %d, want 350 (600 debited - 250 true acked)", recovered)
	}
	st, _ = c.BudgetStat(ctx, ns, coll, true)
	if st.Spent != 250 || st.Available != 750 || st.LeasedOut != 0 {
		t.Fatalf("post-reconcile = spent%d avail%d leased%d, want 250/750/0", st.Spent, st.Available, st.LeasedOut)
	}
	// budCheck.OK must still hold through the full Raft path after reconcile.
	chk, err := c.read(ctx, ns, coll, budCheckQuery{NS: ns, Coll: coll}, true)
	if err != nil {
		t.Fatalf("budCheck: %v", err)
	}
	if bc, _ := chk.(budCheck); !bc.OK {
		t.Fatalf("budCheck not OK after reconcile: %+v", bc)
	}
	// reconcile against an undefined pool -> ErrBudgetNotFound.
	if _, err := c.BudgetReconcile(ctx, ns, []byte("nope"), 10); err != ErrBudgetNotFound {
		t.Fatalf("reconcile undefined pool err = %v, want ErrBudgetNotFound", err)
	}
}

// --- Task 2b.5: lease-expiry sweep (real shard, mirrors TestSetTTLExpiry + TestBudgetEndToEnd) ---

// TestBudgetLeaseExpirySweep drives the full server expiry path through Raft: a paced+timed budget is
// granted via the typed API, then the leader's sweepOnce second pass auto-expires the lease once its
// replicated reclaim deadline passes. The settlement is a DEBIT (the whole grant booked as spent, nothing
// returned to available, since no report ever arrived), conservation holds, and the settled leaseID can
// never be re-granted (tombstone).
func TestBudgetLeaseExpirySweep(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ns, coll := []byte("pacing"), []byte("li/expire")

	// paced + timed: ttl 600ms, selfGuard 500 (=skew), maxPause 0 -> reclaim ~ grant + 600 + 1500 = ~2.1s.
	cfg := BudgetConfig{Rate: 1_000_000, Burst: 1000, SelfGuardMs: maxClockSkewMs, MaxPauseMs: 0,
		DefaultTTLMs: 600, DedupRetryWindowMs: minDedupRetryWindowMs}
	if err := c.BudgetDefine(ctx, ns, coll, 1000, modeStrict, cfg); err != nil {
		t.Fatalf("define: %v", err)
	}
	g, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 600, []byte("lease-X"), 0) // ttl from DefaultTTLMs
	if err != nil || g.Granted != 600 {
		t.Fatalf("grant = %+v, %v want 600", g, err)
	}
	if g.TTLMs != 600 || g.SelfGuardMs != maxClockSkewMs { // the effective timing is echoed back
		t.Fatalf("grant echo = %+v, want ttl600 guard500", g)
	}
	if st, _ := c.BudgetStat(ctx, ns, coll, true); st.LeasedOut != 600 {
		t.Fatalf("pre-expire leased=%d want 600", st.LeasedOut)
	}

	deadline := time.Now().Add(20 * time.Second)
	var st BudStat
	for {
		st, err = c.BudgetStat(ctx, ns, coll, true)
		if err == nil && st.LeasedOut == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease never auto-expired (last stat leased=%d spent=%d)", st.LeasedOut, st.Spent)
		}
		time.Sleep(150 * time.Millisecond)
	}
	// DEBIT settlement: the whole 600 is booked as spent, available untouched (NOT credited back).
	if st.Spent != 600 || st.Available != 400 || st.LeasedOut != 0 {
		t.Fatalf("expired settlement = spent%d avail%d leased%d want 600/400/0 (DEBIT, never credit)",
			st.Spent, st.Available, st.LeasedOut)
	}
	if st.Available+st.LeasedOut+st.Spent != st.Cap {
		t.Fatalf("conservation violated after expiry: %+v", st)
	}
	chk, err := c.read(ctx, ns, coll, budCheckQuery{NS: ns, Coll: coll}, true)
	if err != nil {
		t.Fatalf("budCheck: %v", err)
	}
	if bc, _ := chk.(budCheck); !bc.OK {
		t.Fatalf("budCheck not OK after expiry: %+v", bc)
	}
	// the settled leaseID is single-use-forever: re-granting it is refused by the tombstone.
	if _, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 100, []byte("lease-X"), 0); err != ErrLeaseSettled {
		t.Fatalf("re-grant settled lease err = %v, want ErrLeaseSettled", err)
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
		if p.SpentReported > p.Spent { // Stage-2.x invariant: only reports/attested returns bump SpentReported
			t.Fatalf("step %d (%s): SpentReported %d > Spent %d", step, op, p.SpentReported, p.Spent)
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

// fuzzLease models the harness's GROUND TRUTH for one timed lease in the conservation fuzz: the granted
// amount, what the holder has actually SERVED (un-attested local spend, the impressions the adserver
// really delivered), what it has REPORTED to the server so far, and the replicated reclaim deadline. The
// served total is the externally-acked ledger entry — distinct from the pool's internal spent.
type fuzzLease struct {
	granted   int64
	served    int64 // ground-truth impressions delivered (>= reported; the excess is un-attested)
	reported  int64 // last cumulative attested to the server
	reclaimMs int64 // replicated forced-expiry deadline (grantedMs + ttl + 3*skew + maxPause)
}

// TestBudgetConservationFuzzTimed is the Stage-2e (Task 2e.1) timed extension of the conservation fuzz: it
// drives the REAL shard SM (so it can run the actual budExpiryDueQuery + opBudExpire sweep path, not an
// overlay) and interleaves TIMED grants (random ttl, leader-stamped grantedMs from a deterministic
// counter), un-attested SERVE steps, partial REPORTs, graceful RETURNs, and forced EXPIRY SWEEPS (advance
// the stamped clock, scan the shard expiry index, apply opBudExpire). After EVERY op it asserts both:
//
//	(1) budCheck.OK — the internal equality cap == available + leasedOut + spent (+ no bucket < 0); and
//	(2) Σ served (the EXTERNAL acked ledger) <= cap — the ground-truth bound the equality alone CANNOT
//	    enforce. A forced expiry DEBITS the whole remainder (served-but-unreported + genuinely-unspent), so
//	    consumed budget is never recycled; were it to CREDIT instead (the C2 debit->credit regression),
//	    un-attested-served budget would return to available, be re-granted, re-served, and Σ served would
//	    eventually exceed cap while the equality (1) still held. This test is the regression's tripwire.
func TestBudgetConservationFuzzTimed(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("p"), []byte("c")
	const capUnits = int64(10_000)
	const maxPause = int64(1000)
	// Timed budget: selfGuard == skew (passes the I2 gate), dedup >= floor; per-grant ttl drives timing.
	mustApply(t, sm, initCmdFull(ns, coll, capUnits, 0, capUnits, maxClockSkewMs, maxPause, 0, minDedupRetryWindowMs))

	r := newDetRand(0xB0DDE7)
	clock := int64(1_000_000) // deterministic leader-stamped clock; never time.Now()
	leaseSeq := 0
	live := map[string]*fuzzLease{} // leaseID -> ground truth for un-settled leases
	served := map[string]int64{}    // leaseID -> total impressions delivered (acked ledger, all leases)
	var totalServed int64           // Σ served across ALL lease IDs ever

	liveIDs := func() []string {
		ids := make([]string, 0, len(live))
		for id := range live {
			ids = append(ids, id)
		}
		return ids
	}

	checkInv := func(step int, op string) {
		res, err := sm.Lookup(budCheckQuery{NS: ns, Coll: coll})
		if err != nil {
			t.Fatalf("step %d (%s): budCheck lookup: %v", step, op, err)
		}
		chk := res.(budCheck)
		if !chk.Exists || !chk.OK {
			t.Fatalf("step %d (%s): INV-LOCAL violated: %+v", step, op, chk)
		}
		// The ground-truth bound: total externally-acked impressions never exceed the cap.
		if totalServed > capUnits {
			t.Fatalf("step %d (%s): Σ served %d > cap %d (overspend!) chk=%+v", step, op, totalServed, capUnits, chk)
		}
		// Stage-2.x: SpentReported only ever advances by an actual report/attested return (never a forced
		// expiry's debit), so it can never exceed Spent — the gap is the recoverable stranding.
		if chk.SpentReported > chk.Spent {
			t.Fatalf("step %d (%s): SpentReported %d > Spent %d chk=%+v", step, op, chk.SpentReported, chk.Spent, chk)
		}
	}

	for i := 0; i < 6000; i++ {
		// Occasionally advance the leader clock (models real-time passing between proposes).
		if r.intn(3) == 0 {
			clock += int64(r.intn(4000))
		}
		switch r.intn(6) {
		case 0, 1: // GRANT a fresh timed lease (fresh id: settled ids tombstone, never re-grant).
			id := fmt.Sprintf("L%d", leaseSeq)
			leaseSeq++
			amt := int64(r.intn(4000) - 500) // negatives exercise the B4 guard
			ttl := int64(1000 + r.intn(8000))
			res := mustApply(t, sm, grantCmdT(ns, coll, id, amt, clock, ttl))
			if len(res.Data) == 40 { // a success grant result (sentinels are never 40 bytes)
				g := decodeGrantResult(res.Data)
				if g.Granted > 0 {
					live[id] = &fuzzLease{granted: g.Granted, reclaimMs: reclaimOf(clock, ttl, maxPause)}
				}
			}
			checkInv(i, "grant")
		case 2: // SERVE: the holder delivers impressions locally (un-attested) up to its granted amount.
			if ids := liveIDs(); len(ids) > 0 {
				id := ids[r.intn(len(ids))]
				fl := live[id]
				if room := fl.granted - fl.served; room > 0 {
					d := int64(r.intn(int(room) + 1))
					fl.served += d
					served[id] += d
					totalServed += d
				}
			}
			checkInv(i, "serve")
		case 3: // REPORT the current cumulative served (attest part of what was delivered).
			if ids := liveIDs(); len(ids) > 0 {
				id := ids[r.intn(len(ids))]
				fl := live[id]
				mustApply(t, sm, reportCmd(ns, coll, id, fl.served))
				fl.reported = fl.served
			}
			checkInv(i, "report")
		case 4: // RETURN gracefully: attest the final served and CREDIT the true unspent remainder.
			if ids := liveIDs(); len(ids) > 0 {
				id := ids[r.intn(len(ids))]
				fl := live[id]
				mustApply(t, sm, returnCmd(ns, coll, id, fl.served))
				delete(live, id)
			}
			checkInv(i, "return")
		case 5: // EXPIRY SWEEP: advance the clock past some deadlines, scan the index, force-expire (DEBIT).
			clock += int64(r.intn(6000))
			res, err := sm.Lookup(budExpiryDueQuery{NowMs: clock, Limit: 0})
			if err != nil {
				t.Fatalf("step %d expiry due query: %v", i, err)
			}
			for _, due := range res.([]dueBudLease) {
				mustApply(t, sm, expireCmd(due.NS, due.Coll, string(due.LeaseID), clock))
				delete(live, string(due.LeaseID))
			}
			checkInv(i, "sweep")
		}
	}

	// Final drain: sweep everything still live far in the future, then re-assert the ground-truth bound.
	clock += 1 << 40
	res, _ := sm.Lookup(budExpiryDueQuery{NowMs: clock, Limit: 0})
	for _, due := range res.([]dueBudLease) {
		mustApply(t, sm, expireCmd(due.NS, due.Coll, string(due.LeaseID), clock))
	}
	checkInv(6000, "final-drain")
	if totalServed == 0 {
		t.Fatal("fuzz served nothing — the acked-ledger bound was never exercised")
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
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "A", 0))  // unspent 600 returns; lease row deleted
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
