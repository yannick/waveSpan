//go:build budgetsoak

// Package budget is the Stage-2e LeasedBudget correctness soak (design/35 §7, the §16 mandate). It drives N
// faithful node-cache HOLDERS Spending against ONE shared STRICT budget on a REAL in-process Raft shard
// (collections.Manager + StartShard — the same money-authoritative path the server integration tests use),
// under a combined nemesis (bounded clock-skew + pause/resume past TTL + holder-crash + refill-partition)
// plus a SUSPEND nemesis (freeze CLOCK_MONOTONIC while CLOCK_BOOTTIME advances past the guard). The
// budget-strict-cap checker asserts Σ externally-ACKED impressions ≤ cap — the harness's own ground-truth
// ledger, NOT the pool's internal `spent` (which the equality probe already covers and which alone cannot
// catch a debit→credit overspend, C2).
//
// NODE-CACHE-DRIVING APPROACH (b): a FAITHFUL IN-HARNESS HOLDER, not the real sdk/go cache. Rationale: the
// sdk/go (and wavespan-sdk) caches live in separate modules the server module can't import without module
// plumbing, AND — decisively — their suspend-aware clock (`nowMono`) and wall clock are internal, not
// injectable, so the SUSPEND nemesis and the CLOCK_MONOTONIC negative control would be untestable through
// the real cache's public API. This holder reproduces the load-bearing safety behaviors of sdk/go's
// budgetCell EXACTLY — serve-then-report cumulative, self-fence drop-WITHOUT-report on a pause past the
// budget (I-1), a stop deadline anchored on a suspend-aware (boottime) clock, single-cur/next double buffer,
// and the freshness gate — but against the REAL BudgetService and with INJECTABLE clocks so each nemesis is
// deterministic. The grant/report/return calls hit the real Raft shard; nothing about the money authority
// is faked.
package budget

import (
	"github.com/yannick/wavespan/tests/harness/checker"
)

// maxClockSkewMs mirrors internal/collections.maxClockSkewMs (the HLC mesh bound). The grantor's replicated
// reclaim deadline is grantedMs + ttl + 3*maxClockSkewMs + maxPause, so the holder/ledger must use the same
// constant to reconstruct it for the disjointness checker.
const maxClockSkewMs int64 = 500

// grantEcho is the timing a refill Draw echoes back (mirrors collections.GrantResult). granted == 0 means
// the pool had no capacity (a normal back-pressure result, not an error).
type grantEcho struct {
	granted     int64
	grantedMs   int64
	ttlMs       int64
	selfGuardMs int64
	maxPauseMs  int64
}

// errLeaseSettled is returned by a backend grant when the lease_id is already tombstoned (server §3.7); the
// holder mints a fresh nonce and redraws.
type backendError int

const (
	errNone backendError = iota
	errSettled
	errOther
)

// budgetBackend is the REAL money-authoritative BudgetService the holder draws from. The soak wires it to a
// real in-process Raft shard (see strict_cap_test.go); nothing here is faked.
type budgetBackend interface {
	grant(leaseID string, amount int64) (grantEcho, backendError)
	report(leaseID string, cumulative int64) // best-effort attestation (failures only delay it; safety is
	//                                           backed by the server's forced-expiry DEBIT, §3.5/I-1)
	ret(leaseID string, finalSpent int64) // graceful return: server CREDITS the attested remainder
}

// clock is the holder's injectable time source. boot() is the suspend-INCLUSIVE real clock (CLOCK_BOOTTIME
// analog) — the true wall-time an impression is served, and what the holder SHOULD use for the fence/stop.
// mono() is the suspend-FROZEN clock (CLOCK_MONOTONIC analog) — what the (c) negative control wrongly uses.
// wall() is the holder's wall clock for the freshness gate (may carry injected skew/transit). All in ms.
type clock interface {
	boot() int64
	mono() int64
	wall() int64
}

// holderKnobs selects the faithful (all-false) behavior or exactly one negative control.
type holderKnobs struct {
	useMonotonic     bool // (c) decide the fence/stop on the FROZEN-on-suspend clock instead of boottime
	disableFreshness bool // (a) accept stale grants (skip the §16 edit #2 freshness gate)
	creditOnReclaim  bool // (b) on abandoning a chunk, gracefully RETURN (CREDIT) instead of dropping it for
	//                       the server to force-expire (DEBIT) — i.e. model forced expiry crediting (C2)
}

// leaseChunk is one drawn lease in the holder cache: remaining/amount, served (ground-truth impressions
// delivered) vs reported (last attested cumulative), its stop deadline in the holder's CHOSEN clock domain,
// and the grantor's REPLICATED reclaimNotBeforeMs (grant-leader ms) for the disjointness checker.
type leaseChunk struct {
	leaseID           string
	remaining, amount int64
	served, reported  int64
	deadline          int64 // in the chosen-clock domain (boot or mono per knob)
	reclaimMs         int64 // grantedMs + ttl + 3*skew + maxPause (grant-leader ms)
}

// holder is a faithful in-harness node-cache holder (see package doc). One holder == one adserver node.
type holder struct {
	id    string
	be    budgetBackend
	clk   clock
	knobs holderKnobs
	rec   func(checker.SpendEvent)

	chunkUnits int64

	cur, next  *leaseChunk
	nonce      int
	lastSeen   int64 // last served time in the CHOSEN clock domain (0 = un-anchored)
	maxPauseMs int64 // echoed self-fence budget (0 = no fence)

	// alignClock (deterministic scenarios only): on each successful grant, resync the injected fakeClock to
	// the leader-stamped grantedMs so the holder's clock domain and the server's reclaim deadline share one
	// time base (a perfectly-synced holder); nemeses then inject deviations explicitly.
	alignClock bool
	// transitDelayMs models a grant that sat in flight: added to the holder's clocks right after alignment on
	// the NEXT grant (the (a) freshness scenario); consumed once.
	transitDelayMs int64
}

// chosenNow returns the clock the holder makes fence/stop decisions on: boottime (correct) or the
// suspend-frozen monotonic clock (the (c) negative control).
func (h *holder) chosenNow() int64 {
	if h.knobs.useMonotonic {
		return h.clk.mono()
	}
	return h.clk.boot()
}

// spend tries to serve n units from the cache with NO RPC (the §4.1 fast path, in order): self-fence, drop
// past-deadline cur (promote next), then decrement. It records an externally-acked SpendEvent ONLY when the
// impression is actually served, stamping the REAL (boottime) time and the lease's reclaim deadline. Returns
// true iff served. A miss triggers a refill via the caller.
func (h *holder) spend(n int64) bool {
	nowC := h.chosenNow()
	// (a) self-fence: a gap past the pause budget means the lease may have been reclaimed during a
	// suspend/pause — drop cur/next and serve NOTHING, with NO report (I-1: the un-attested tail is the
	// server's to DEBIT on forced expiry).
	if h.maxPauseMs > 0 && h.lastSeen != 0 && nowC-h.lastSeen > h.maxPauseMs {
		h.abandonCur()
		h.next = nil
		h.lastSeen = nowC
		return false
	}
	// (c) drop cur if past its own deadline (or drained) and promote a buffered chunk.
	h.promote(nowC)
	// (d) fast path: decrement and record the acked impression at REAL time.
	if h.cur != nil && h.cur.remaining >= n {
		h.cur.remaining -= n
		h.cur.served += n
		h.lastSeen = nowC
		h.rec(checker.SpendEvent{
			Holder: h.id, LeaseID: h.cur.leaseID, Units: n,
			AtMs: h.clk.boot(), ReclaimMs: h.cur.reclaimMs,
		})
		return true
	}
	return false
}

// promote drops cur when expired/drained, then promotes next into the empty slot (dropping it too if it is
// already past its deadline). All comparisons are in the chosen-clock domain.
func (h *holder) promote(nowC int64) {
	if h.cur != nil && (nowC >= h.cur.deadline || h.cur.remaining == 0) {
		h.cur = nil
	}
	if h.cur == nil && h.next != nil {
		h.cur = h.next
		h.next = nil
		if nowC >= h.cur.deadline {
			h.cur = nil
		}
	}
}

// abandonCur releases the current chunk on a self-fence. POSITIVE path: drop it WITHOUT a report — the
// server's forced-expiry DEBITS the un-attested tail (served-but-unreported), the safe direction. NEGATIVE
// control (b): gracefully RETURN it attesting only `reported`, so the server CREDITS the un-attested-served
// remainder back to available — modeling forced expiry crediting instead of debiting (the C2 overspend).
func (h *holder) abandonCur() {
	if h.cur == nil {
		return
	}
	if h.knobs.creditOnReclaim {
		h.be.ret(h.cur.leaseID, h.cur.reported)
	}
	h.cur = nil
}

// refill performs one logical refill (§4.2): attest the draining lease's cumulative spend, then Draw the
// next chunk under a fresh-per-attempt lease_id, run the freshness gate (unless disabled), and install it
// stamping the suspend-aware deadline. Returns false if no capacity / gave up (a later spend retries).
func (h *holder) refill() bool {
	if h.next != nil {
		return true // double buffer already full
	}
	// Attest the draining lease's cumulative spend (best-effort).
	if h.cur != nil && h.cur.served > h.cur.reported {
		h.be.report(h.cur.leaseID, h.cur.served)
		h.cur.reported = h.cur.served
	}
	for attempt := 0; attempt < 4; attempt++ {
		leaseID := h.id + "-" + itoa(h.nonce)
		g, be := h.be.grant(leaseID, h.chunkUnits)
		switch be {
		case errSettled:
			h.nonce++ // tombstoned id: mint a fresh lease and retry
			continue
		case errOther:
			return false // transient: a later spend retries
		}
		if g.granted <= 0 {
			return false // no capacity now: underspend, retry later (safe direction)
		}
		// Resync the injected clock to the leader's grantedMs (one shared time base) and then add any modeled
		// in-flight transit, so the freshness gate measures wall-grantedMs == transitDelayMs (the (a) scenario).
		if fc, ok := h.clk.(*fakeClock); ok && h.alignClock {
			fc.realign(g.grantedMs)
			if h.transitDelayMs > 0 {
				fc.advance(h.transitDelayMs)
				h.transitDelayMs = 0
			}
		}
		// Freshness gate (§16 edit #2): reject a grant whose transit exceeded self_guard, anchoring the
		// deadline on a stale receipt would be unsound. Drop + redraw with a fresh nonce.
		if !h.knobs.disableFreshness && g.ttlMs > 0 && h.clk.wall()-g.grantedMs > g.selfGuardMs {
			h.nonce++
			continue
		}
		nowC := h.chosenNow()
		ch := &leaseChunk{
			leaseID: leaseID, remaining: g.granted, amount: g.granted,
			reclaimMs: g.grantedMs + g.ttlMs + 3*maxClockSkewMs + g.maxPauseMs,
		}
		ch.deadline = maxDeadline
		if g.ttlMs > 0 {
			ch.deadline = nowC + (g.ttlMs - g.selfGuardMs)
		}
		if h.cur == nil {
			h.cur = ch
		} else {
			h.next = ch
		}
		h.maxPauseMs = g.maxPauseMs
		h.lastSeen = nowC // re-anchor so a freshly installed chunk is not false-fenced
		h.nonce++
		return true
	}
	return false
}

// returnAll gracefully settles every held chunk, attesting its TRUE final served (the honest graceful-
// shutdown path: the holder knows exactly what it delivered, so the server credits only genuinely-unspent
// budget). Used on clean holder shutdown in the positive soak.
func (h *holder) returnAll() {
	for _, ch := range []*leaseChunk{h.cur, h.next} {
		if ch != nil && ch.leaseID != "" {
			h.be.ret(ch.leaseID, ch.served)
		}
	}
	h.cur, h.next = nil, nil
}

const maxDeadline int64 = 1<<62 - 1

// itoa is a tiny base-10 formatter (avoids strconv import churn in the lease-id hot path).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// fakeClock is a deterministic, test-driven clock. boot is the real (suspend-inclusive) time; mono tracks
// boot EXCEPT while frozen (the suspend nemesis freezes mono and advances boot); wall carries the freshness-
// gate reading (= boot + skew unless overridden).
type fakeClock struct {
	bootMs   int64
	monoMs   int64
	wallMs   int64
	monoFroz bool
}

func newFakeClock(start int64) *fakeClock {
	return &fakeClock{bootMs: start, monoMs: start, wallMs: start}
}

func (c *fakeClock) boot() int64 { return c.bootMs }
func (c *fakeClock) mono() int64 { return c.monoMs }
func (c *fakeClock) wall() int64 { return c.wallMs }

// advance moves real time forward by d ms: boot and wall always advance; mono advances ONLY when not frozen
// (so a suspend window — between freeze() and thaw() — leaves mono behind, exactly like CLOCK_MONOTONIC).
func (c *fakeClock) advance(d int64) {
	c.bootMs += d
	c.wallMs += d
	if !c.monoFroz {
		c.monoMs += d
	}
}

// freeze begins a suspend (CLOCK_MONOTONIC stops counting); thaw ends it.
func (c *fakeClock) freeze() { c.monoFroz = true }
func (c *fakeClock) thaw()   { c.monoFroz = false }

// realign resyncs all three readings to t (a perfectly-synced holder at grant receipt); also clears any
// freeze so a fresh grant starts un-frozen.
func (c *fakeClock) realign(t int64) {
	c.bootMs, c.monoMs, c.wallMs, c.monoFroz = t, t, t, false
}
