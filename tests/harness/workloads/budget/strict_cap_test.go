//go:build budgetsoak

package budget

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/tests/harness/checker"
)

// --- real in-process Raft shard (the money-authoritative BudgetService) ---

const (
	budNS       = "ad"
	budTTLMs    = int64(2000)
	budGuardMs  = int64(500) // == maxClockSkewMs (passes the I2 grant gate)
	budPauseMs  = int64(500)
	budDedupMs  = int64(30_000)
	budSweepDur = 200 * time.Millisecond
)

// realShard spins a single-node in-process Raft shard and returns a Collections handle plus a cleanup.
func realShard(t *testing.T) (*collections.Collections, func()) {
	t.Helper()
	mem := storage.NewMemStore()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()

	var m *collections.Manager
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err = collections.NewManagerWithOptions(t.TempDir(), addr, mem, collections.Options{
			Tunables: collections.Tunables{SweepEvery: budSweepDur},
		})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewManager never succeeded: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := m.StartShard(2, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	cols := collections.New(m, collections.SingleShardDirectory(2))

	// wait for a leader (a committed no-op SAdd succeeds once the shard is ready).
	rdy := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, perr := cols.SAdd(ctx, []byte("__probe__"), []byte("__probe__"))
		cancel()
		if perr == nil {
			break
		}
		if time.Now().After(rdy) {
			m.Stop()
			_ = mem.Close()
			t.Fatalf("shard never became ready: %v", perr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return cols, func() { m.Stop(); _ = mem.Close() }
}

// defineBudget defines one shared STRICT timed budget on the shard.
func defineBudget(t *testing.T, cols *collections.Collections, coll []byte, cap int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cols.BudgetDefine(ctx, []byte(budNS), coll, cap, 1 /*modeStrict*/, collections.BudgetConfig{
		SelfGuardMs: budGuardMs, MaxPauseMs: budPauseMs, DefaultTTLMs: budTTLMs, DedupRetryWindowMs: budDedupMs,
	}); err != nil {
		t.Fatalf("BudgetDefine: %v", err)
	}
}

// defineBudgetTTL defines a STRICT timed budget with an explicit (short) TTL and no holder pause budget, so
// an abandoned lease force-expires (DEBIT) quickly — used by the reconciliation scenarios.
func defineBudgetTTL(t *testing.T, cols *collections.Collections, coll []byte, cap, ttlMs int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cols.BudgetDefine(ctx, []byte(budNS), coll, cap, 1 /*modeStrict*/, collections.BudgetConfig{
		SelfGuardMs: budGuardMs, MaxPauseMs: 0, DefaultTTLMs: ttlMs, DedupRetryWindowMs: budDedupMs,
	}); err != nil {
		t.Fatalf("BudgetDefine: %v", err)
	}
}

// collBackend adapts the real Collections budget API to budgetBackend.
type collBackend struct {
	cols   *collections.Collections
	coll   []byte
	holder []byte
}

func (b *collBackend) grant(leaseID string, amount int64) (grantEcho, backendError) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gr, err := b.cols.BudgetGrant(ctx, []byte(budNS), b.coll, b.holder, amount, []byte(leaseID), 0)
	switch {
	case err == nil:
		return grantEcho{
			granted: gr.Granted, grantedMs: gr.GrantedMs, ttlMs: gr.TTLMs,
			selfGuardMs: gr.SelfGuardMs, maxPauseMs: gr.MaxPauseMs,
		}, errNone
	case err == collections.ErrNoCapacity:
		return grantEcho{granted: 0}, errNone
	case err == collections.ErrLeaseSettled:
		return grantEcho{}, errSettled
	default:
		return grantEcho{}, errOther
	}
}

func (b *collBackend) report(leaseID string, cumulative int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.cols.BudgetReport(ctx, []byte(budNS), b.coll, []byte(leaseID), b.holder, cumulative)
}

func (b *collBackend) ret(leaseID string, finalSpent int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = b.cols.BudgetReturn(ctx, []byte(budNS), b.coll, []byte(leaseID), b.holder, finalSpent)
}

// ledger is the harness's concurrency-safe ground-truth sink of acked impressions.
type ledger struct {
	mu sync.Mutex
	ev []checker.SpendEvent
}

func (l *ledger) record(e checker.SpendEvent) {
	l.mu.Lock()
	l.ev = append(l.ev, e)
	l.mu.Unlock()
}

func (l *ledger) snapshot() []checker.SpendEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]checker.SpendEvent(nil), l.ev...)
}

// realClock tracks real (UnixMilli) time; boot==mono==wall (no skew, no suspend) — the soak's nemeses are
// real sleeps (boot advances) and crashes, which stay within the safety budget so the positive run is clean.
type realClock struct{}

func (realClock) boot() int64 { return time.Now().UnixMilli() }
func (realClock) mono() int64 { return time.Now().UnixMilli() }
func (realClock) wall() int64 { return time.Now().UnixMilli() }

// --- the positive soak: N concurrent holders, real shard, combined nemeses ---

// TestStrictCapSoak is the §16 mandate: N concurrent faithful holders Spend against ONE shared STRICT
// budget on a REAL Raft shard under bounded clock-skew (within budget) + pause/resume + holder-crash +
// refill-partition, while the server's real sweep force-expires (DEBITS) abandoned leases. Both the
// budget-strict-cap (Σ acked ≤ cap) and budget-lease-disjointness checkers must be CLEAN.
func TestStrictCapSoak(t *testing.T) {
	cols, stop := realShard(t)
	defer stop()
	coll := []byte("li/soak/total")
	const cap = int64(200_000)
	defineBudget(t, cols, coll, cap)

	lg := &ledger{}
	const nHolders = 6
	var wg sync.WaitGroup
	deadline := time.Now().Add(4 * time.Second)
	for i := 0; i < nHolders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			h := &holder{
				id: "node-" + itoa(idx), be: &collBackend{cols: cols, coll: coll, holder: []byte("node-" + itoa(idx))},
				clk: realClock{}, rec: lg.record, chunkUnits: 400,
			}
			crash := idx == nHolders-1 // one holder crashes mid-run (stops without Return)
			partitionAt := 80          // one holder's refills are partitioned for a window
			for n := 0; time.Now().Before(deadline); n++ {
				if crash && n == 60 {
					return // crash: stop spending, leave the lease to forced-expiry (DEBIT)
				}
				if idx == 0 && n >= partitionAt && n < partitionAt+20 {
					// refill-partition: skip refills (drain the cache → serve nothing → underspend, safe).
					_ = h.spend(1)
					time.Sleep(time.Millisecond)
					continue
				}
				if idx == 1 && n == 100 {
					time.Sleep(time.Duration(budPauseMs)*time.Millisecond + 200*time.Millisecond) // pause past budget → self-fence
				}
				if !h.spend(1) {
					h.refill()
				}
				if n%200 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
			if !crash {
				h.returnAll() // graceful shutdown attests true spend, credits genuinely-unspent budget
			}
		}(i)
	}
	wg.Wait()

	ev := lg.snapshot()
	if len(ev) == 0 {
		t.Fatal("soak recorded no impressions")
	}
	var total int64
	for _, e := range ev {
		total += e.Units
	}
	if cv := (checker.BudgetCapChecker{Cap: cap}).Check(ev); len(cv) != 0 {
		t.Fatalf("CAP VIOLATION in clean soak: %+v", cv[0])
	}
	if dv := (checker.LeaseDisjointnessChecker{}).Check(ev); len(dv) != 0 {
		t.Fatalf("DISJOINTNESS VIOLATION in clean soak: %+v", dv[0])
	}
	t.Logf("soak clean: %d acked impressions, Σ=%d ≤ cap=%d (%d holders, real sweep + combined nemeses)",
		len(ev), total, cap, nHolders)
}

// --- NEGATIVE CONTROLS (must go RED; each guarded by a knob; runnable on demand under -tags budgetsoak) ---

// newScenarioHolder builds a single deterministic holder with an injected fakeClock aligned to the leader.
func newScenarioHolder(t *testing.T, cols *collections.Collections, coll []byte, k holderKnobs, lg *ledger, chunk int64) (*holder, *fakeClock) {
	clk := newFakeClock(0)
	h := &holder{
		id: "node-X", be: &collBackend{cols: cols, coll: coll, holder: []byte("node-X")},
		clk: clk, knobs: k, rec: lg.record, chunkUnits: chunk, alignClock: true,
	}
	return h, clk
}

// TestNegControlDebitToCredit (b): forced expiry DEBITS vs CREDITS. The holder serves 60 impressions
// un-attested, then a suspend past the budget self-fences and abandons the chunk. POSITIVE: it is dropped,
// the server keeps the budget locked (later DEBITED), so a re-draw gets nothing → Σ acked stays ≤ cap.
// NEGATIVE (creditOnReclaim): the chunk is gracefully RETURNED attesting only the 0 reported — the server
// CREDITS the 60 un-attested-served units back to available, they are re-granted and re-served, and Σ acked
// blows past cap. The equality probe (cap == avail+leased+spent) stays TRUE throughout — this is the C2
// overspend it cannot see, and the budget-strict-cap checker is the only thing that catches it.
func TestNegControlDebitToCredit(t *testing.T) {
	run := func(credit bool) ([]checker.CapViolation, int64) {
		cols, stop := realShard(t)
		defer stop()
		coll := []byte("li/b/total")
		const cap = int64(100)
		defineBudget(t, cols, coll, cap)
		lg := &ledger{}
		h, clk := newScenarioHolder(t, cols, coll, holderKnobs{creditOnReclaim: credit}, lg, 100)

		if !h.refill() { // draw the whole pool (100)
			t.Fatal("initial refill failed")
		}
		for i := 0; i < 60; i++ { // serve 60 impressions, un-attested (no refill between → reported stays 0)
			if !h.spend(1) {
				t.Fatalf("serve %d failed", i)
			}
			clk.advance(1)
		}
		clk.freeze()                  // suspend
		clk.advance(budPauseMs + 100) // boot jumps past the pause budget
		clk.thaw()
		_ = h.spend(1) // self-fence fires → abandonCur (drop OR credit-return per knob)
		// a re-draw: POSITIVE gets 0 (budget locked), NEGATIVE gets 100 (credited back).
		if h.refill() {
			for i := 0; i < 100 && h.spend(1); i++ {
				clk.advance(1)
			}
		}
		ev := lg.snapshot()
		var total int64
		for _, e := range ev {
			total += e.Units
		}
		return (checker.BudgetCapChecker{Cap: cap}).Check(ev), total
	}

	posViol, posTotal := run(false)
	if len(posViol) != 0 {
		t.Fatalf("POSITIVE (debit) unexpectedly RED: %+v", posViol)
	}
	t.Logf("POSITIVE (debit-on-expire): Σ acked=%d ≤ cap=100, checker GREEN", posTotal)

	negViol, negTotal := run(true)
	if len(negViol) == 0 {
		t.Fatalf("NEGATIVE CONTROL (debit→credit) FAILED TO GO RED: Σ acked=%d — the credit path did not overspend", negTotal)
	}
	t.Logf("NEGATIVE (debit→credit): Σ acked=%d > cap=100, checker RED: %s", negTotal, negViol[0].Detail)
}

// TestNegControlFreshnessGate (a): the §16 edit #2 freshness gate. A grant sits in flight 3000ms (transit >
// self_guard). POSITIVE: the gate rejects the stale grant and redraws a fresh one, anchoring the stop
// deadline correctly → every impression is served strictly before the lease's reclaim deadline. NEGATIVE
// (disableFreshness): the stale grant is accepted, the deadline is anchored on a stale receipt and lands
// PAST reclaimNotBeforeMs → the holder serves impressions in the grantor's reclaim window → the
// budget-lease-disjointness checker goes RED. (The cap checker stays GREEN: debit-on-forced-expiry is the
// single-cluster overspend backstop — see the soak notes; this control is load-bearing for window
// disjointness / underspend, which is what the gate protects.)
func TestNegControlFreshnessGate(t *testing.T) {
	run := func(disable bool) []checker.DisjointViolation {
		cols, stop := realShard(t)
		defer stop()
		coll := []byte("li/a/total")
		defineBudget(t, cols, coll, 1000)
		lg := &ledger{}
		h, clk := newScenarioHolder(t, cols, coll, holderKnobs{disableFreshness: disable}, lg, 200)
		h.transitDelayMs = 3000 // the first grant sits in flight 3s (> self_guard 500)

		if !h.refill() {
			// POSITIVE may exhaust redraw attempts if every draw looks stale; here only the FIRST draw carries
			// transit, so the redraw is fresh and installs. A failure here would be a liveness issue, not safety.
			t.Fatal("refill failed (no chunk installed)")
		}
		// Serve up to the chunk's deadline, advancing real (boot) time 200ms per step so late serves cross the
		// reclaim deadline iff the deadline was anchored stale.
		for i := 0; i < 30; i++ {
			if !h.spend(1) {
				break
			}
			clk.advance(300)
		}
		return (checker.LeaseDisjointnessChecker{}).Check(lg.snapshot())
	}

	if dv := run(false); len(dv) != 0 {
		t.Fatalf("POSITIVE (freshness gate on) unexpectedly RED: %+v", dv[0])
	}
	t.Log("POSITIVE (freshness gate on): no serve past reclaim, disjointness GREEN")

	dv := run(true)
	if len(dv) == 0 {
		t.Fatal("NEGATIVE CONTROL (freshness gate off) FAILED TO GO RED: no past-reclaim serve detected")
	}
	t.Logf("NEGATIVE (freshness gate off): disjointness RED: %s", dv[0].Detail)
}

// TestNegControlMonotonicSuspend (c): CLOCK_BOOTTIME vs CLOCK_MONOTONIC under the SUSPEND nemesis. The
// holder serves a few impressions, then a suspend freezes CLOCK_MONOTONIC while CLOCK_BOOTTIME advances
// 5000ms past the lease's reclaim deadline. POSITIVE (boottime): the self-fence sees the 5000ms gap and
// drops the chunk — it serves NOTHING on resume. NEGATIVE (useMonotonic): the frozen clock shows ~0ms
// elapsed, the fence does NOT fire and the deadline is not reached, so the holder RESUMES serving a lease
// the grantor already reclaimed → it serves at real time past reclaimNotBeforeMs → disjointness RED. This
// is the §2 C1 hole: time.Now()'s monotonic clock freezes on suspend and silently breaks the fence.
func TestNegControlMonotonicSuspend(t *testing.T) {
	run := func(useMono bool) ([]checker.DisjointViolation, bool) {
		cols, stop := realShard(t)
		defer stop()
		coll := []byte("li/c/total")
		defineBudget(t, cols, coll, 1000)
		lg := &ledger{}
		h, clk := newScenarioHolder(t, cols, coll, holderKnobs{useMonotonic: useMono}, lg, 200)

		if !h.refill() {
			t.Fatal("refill failed")
		}
		for i := 0; i < 5; i++ { // serve a few impressions well within the window
			if !h.spend(1) {
				t.Fatalf("serve %d failed", i)
			}
			clk.advance(10)
		}
		before := len(lg.snapshot())
		clk.freeze()         // SUSPEND: CLOCK_MONOTONIC stops
		clk.advance(5000)    // CLOCK_BOOTTIME advances 5s, past ttl+guard and past reclaimNotBefore
		clk.thaw()           // resume
		served := h.spend(1) // POSITIVE self-fences (false); NEGATIVE serves on the reclaimed lease (true)
		after := lg.snapshot()
		servedPastResume := len(after) > before && served
		return (checker.LeaseDisjointnessChecker{}).Check(after), servedPastResume
	}

	dvPos, servedPos := run(false)
	if servedPos {
		t.Fatal("POSITIVE (boottime) served on resume — the self-fence did not fire")
	}
	if len(dvPos) != 0 {
		t.Fatalf("POSITIVE (boottime) unexpectedly RED: %+v", dvPos[0])
	}
	t.Log("POSITIVE (boottime self-fence): served nothing on resume, disjointness GREEN")

	dvNeg, servedNeg := run(true)
	if !servedNeg || len(dvNeg) == 0 {
		t.Fatalf("NEGATIVE CONTROL (CLOCK_MONOTONIC) FAILED TO GO RED: servedOnResume=%v violations=%d", servedNeg, len(dvNeg))
	}
	t.Logf("NEGATIVE (CLOCK_MONOTONIC under suspend): self-fence did not fire, disjointness RED: %s", dvNeg[0].Detail)
}

// --- §3.8: External Σ-acked reconciliation (recover forced-expiry stranding) ---

// ledgerTotal sums the harness's ground-truth acked impressions — the SAME figure the BudgetCapChecker
// walks, and the AUTHORITATIVE external Σ-acked total a correct controller feeds to BudgetReconcile.
func ledgerTotal(lg *ledger) int64 {
	var sum int64
	for _, e := range lg.snapshot() {
		sum += e.Units
	}
	return sum
}

// waitLeasedZero polls until the pool's leasedOut drains to 0 (the leader's sweep force-expired the
// stranded lease, DEBITING its whole remainder to spent), returning the post-expiry stat.
func waitLeasedZero(t *testing.T, cols *collections.Collections, coll []byte) collections.BudStat {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		st, err := cols.BudgetStat(ctx, []byte(budNS), coll, true)
		cancel()
		if err == nil && st.LeasedOut == 0 {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease never force-expired (leased=%d spent=%d)", st.LeasedOut, st.Spent)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// TestReconcileRecoversStrandingSoak is the Part-B reconciliation scenario on a REAL Raft shard: a holder
// strands budget (serves 600 acked impressions but reports only 200, then crashes; the server force-expires
// the lease and DEBITS the whole grant to spent — leaving available at 0 with 400 of genuinely-unspent
// headroom STRANDED). A controller then reconciles to the harness's GROUND-TRUTH Σ-acked total (the same
// ledger the BudgetCapChecker uses). Assertions: (a) the budget-strict-cap checker stays GREEN across the
// reconcile (Σ acked ≤ cap — reconcile introduces NO overspend); (b) RECOVERY — available rises from 0 and a
// fresh lease can deliver further toward the true remaining cap (the stranded headroom is actually
// recovered, not merely left safe).
func TestReconcileRecoversStrandingSoak(t *testing.T) {
	cols, stop := realShard(t)
	defer stop()
	coll := []byte("li/reconcile/total")
	const cap = int64(1000)
	defineBudgetTTL(t, cols, coll, cap, 300) // short TTL → quick forced expiry

	lg := &ledger{}
	be := &collBackend{cols: cols, coll: coll, holder: []byte("node-R")}

	// 1. draw the whole pool, serve 600 acked impressions (ground truth) but report only 200 (under-attested).
	g, e := be.grant("lease-1", cap)
	if e != errNone || g.granted != cap {
		t.Fatalf("initial grant = %d err=%v, want %d", g.granted, e, cap)
	}
	const served, reported = int64(600), int64(200)
	for i := int64(0); i < served; i++ {
		lg.record(checker.SpendEvent{Holder: "node-R", LeaseID: "lease-1", Units: 1, AtMs: time.Now().UnixMilli()})
	}
	be.report("lease-1", reported)

	// 2. holder crashes (no return); the server force-expires the lease → DEBIT the whole grant to spent.
	st := waitLeasedZero(t, cols, coll)
	if st.Spent != cap || st.Available != 0 || st.SpentReported != reported {
		t.Fatalf("post-expire = spent%d avail%d reported%d, want %d/0/%d (DEBIT strands headroom)",
			st.Spent, st.Available, st.SpentReported, cap, reported)
	}
	// checker is GREEN pre-reconcile (Σ acked 600 ≤ cap) — the stranding is bounded UNDERSPEND, the safe side.
	if cv := (checker.BudgetCapChecker{Cap: cap}).Check(lg.snapshot()); len(cv) != 0 {
		t.Fatalf("CAP VIOLATION before reconcile: %+v", cv[0])
	}

	// 3. controller reconciles to the GROUND-TRUTH external Σ-acked total (600) — the authoritative ledger.
	trueAcked := ledgerTotal(lg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	recovered, err := cols.BudgetReconcile(ctx, []byte(budNS), coll, trueAcked)
	cancel()
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if recovered != cap-served { // 1000 debited - 600 true acked = 400 recovered
		t.Fatalf("recovered = %d, want %d", recovered, cap-served)
	}
	st, _ = cols.BudgetStat(context.Background(), []byte(budNS), coll, true)
	if st.Spent != served || st.Available != cap-served {
		t.Fatalf("post-reconcile = spent%d avail%d, want %d/%d (stranding recovered)", st.Spent, st.Available, served, cap-served)
	}

	// 4. RECOVERY: a fresh lease can now deliver further toward the true remaining cap (available rose 0→400).
	g2, e2 := be.grant("lease-2", cap)
	if e2 != errNone || g2.granted != cap-served {
		t.Fatalf("post-reconcile grant = %d err=%v, want %d (recovered headroom)", g2.granted, e2, cap-served)
	}
	for i := int64(0); i < g2.granted; i++ {
		lg.record(checker.SpendEvent{Holder: "node-R", LeaseID: "lease-2", Units: 1, AtMs: time.Now().UnixMilli()})
	}
	be.report("lease-2", g2.granted)

	// 5. checker STILL GREEN after delivering the recovered headroom: Σ acked = 600 + 400 = 1000 ≤ cap.
	if cv := (checker.BudgetCapChecker{Cap: cap}).Check(lg.snapshot()); len(cv) != 0 {
		t.Fatalf("CAP VIOLATION after reconcile+redeliver: %+v", cv[0])
	}
	t.Logf("reconcile recovered %d stranded units; Σ acked=%d ≤ cap=%d, checker GREEN", recovered, ledgerTotal(lg), cap)
}

// TestNegControlReconcileOvercredits is the §3.8 NEGATIVE CONTROL (must go RED). The reconcile is only
// money-safe when fed the AUTHORITATIVE external Σ-acked total. The buggy controller instead reconciles to
// the server's ATTESTED figure (SpentReported) — ignoring the served-but-unreported impressions the external
// ledger DOES know about. That credits back an INFLATED headroom (the un-attested-served units), which is
// re-granted and re-served, so Σ acked blows past cap. The equality probe (cap == avail+leased+spent) stays
// TRUE throughout — this is exactly the C2-class overspend it cannot see, and the budget-strict-cap checker
// is the only thing that catches it. POSITIVE (reconcile to the ground-truth ledger) stays GREEN; NEGATIVE
// (reconcile to the under-attested SpentReported) goes RED — proving the controller MUST feed the true
// external Σ-acked total (the SpentReported floor alone does NOT make a wrong input safe).
func TestNegControlReconcileOvercredits(t *testing.T) {
	run := func(buggy bool) ([]checker.CapViolation, int64) {
		cols, stop := realShard(t)
		defer stop()
		coll := []byte("li/reconcile-neg/total")
		const cap = int64(1000)
		defineBudgetTTL(t, cols, coll, cap, 300)
		lg := &ledger{}
		be := &collBackend{cols: cols, coll: coll, holder: []byte("node-R")}

		g, e := be.grant("lease-1", cap)
		if e != errNone || g.granted != cap {
			t.Fatalf("initial grant = %d err=%v", g.granted, e)
		}
		const served, reported = int64(600), int64(200)
		for i := int64(0); i < served; i++ {
			lg.record(checker.SpendEvent{Holder: "node-R", LeaseID: "lease-1", Units: 1, AtMs: time.Now().UnixMilli()})
		}
		be.report("lease-1", reported)
		st := waitLeasedZero(t, cols, coll) // forced expiry DEBITS → spent=cap, spentReported=200

		// The controller's reconcile figure: ground-truth Σ-acked (correct) vs the under-attested SpentReported
		// (the bug — it forgets the 400 served-but-unreported units).
		figure := ledgerTotal(lg) // 600 (authoritative)
		if buggy {
			figure = st.SpentReported // 200 (under-attested → over-credits the 400 un-attested-served units)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := cols.BudgetReconcile(ctx, []byte(budNS), coll, figure); err != nil {
			cancel()
			t.Fatalf("reconcile: %v", err)
		}
		cancel()

		// A fresh holder draws whatever headroom the reconcile left and serves it all (acked).
		g2, _ := be.grant("lease-2", cap)
		for i := int64(0); i < g2.granted; i++ {
			lg.record(checker.SpendEvent{Holder: "node-R", LeaseID: "lease-2", Units: 1, AtMs: time.Now().UnixMilli()})
		}
		be.report("lease-2", g2.granted)
		return (checker.BudgetCapChecker{Cap: cap}).Check(lg.snapshot()), ledgerTotal(lg)
	}

	posViol, posTotal := run(false)
	if len(posViol) != 0 {
		t.Fatalf("POSITIVE (reconcile to ground-truth) unexpectedly RED: %+v", posViol[0])
	}
	t.Logf("POSITIVE (reconcile to true Σ-acked): Σ acked=%d ≤ cap=1000, checker GREEN", posTotal)

	negViol, negTotal := run(true)
	if len(negViol) == 0 {
		t.Fatalf("NEGATIVE CONTROL (reconcile to under-attested figure) FAILED TO GO RED: Σ acked=%d — no overspend", negTotal)
	}
	t.Logf("NEGATIVE (reconcile to under-attested SpentReported): Σ acked=%d > cap=1000, checker RED: %s", negTotal, negViol[0].Detail)
}

// TestSuspendNemesisSelfFences is the positive suspend-nemesis assertion (separate from the (c) negative
// control): with the boottime clock, a suspend that advances CLOCK_BOOTTIME past the pause budget makes the
// holder self-fence and serve NOTHING on resume (it drops the presumed-reclaimed lease).
func TestSuspendNemesisSelfFences(t *testing.T) {
	cols, stop := realShard(t)
	defer stop()
	coll := []byte("li/fence/total")
	defineBudget(t, cols, coll, 1000)
	lg := &ledger{}
	h, clk := newScenarioHolder(t, cols, coll, holderKnobs{}, lg, 200)

	if !h.refill() {
		t.Fatal("refill failed")
	}
	for i := 0; i < 5; i++ {
		if !h.spend(1) {
			t.Fatalf("serve %d failed", i)
		}
		clk.advance(10)
	}
	before := len(lg.snapshot())
	clk.freeze()
	clk.advance(budPauseMs + 1000) // suspend past the pause budget (boottime advances)
	clk.thaw()
	if h.spend(1) {
		t.Fatal("holder served on resume — self-fence did not fire")
	}
	if len(lg.snapshot()) != before {
		t.Fatal("holder recorded an impression during the fenced window")
	}
	t.Log("suspend nemesis: boottime self-fence fired, holder served nothing on resume")
}
