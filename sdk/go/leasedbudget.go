package wavespan

import (
	"context"
	"errors"
	"sync"
)

// LeasedBudget node-side cache (design/35 Stage 2, §4). An adserver Acquires a budget once and Spends
// against an in-memory token/lease cache with ZERO Raft per spend; consensus is touched ~once per refill.
// A crashed/paused/partitioned holder can never cause STRICT overspend (worst case = underspend), because
// the holder stops EARLY on a single suspend-aware monotonic clock (CLOCK_BOOTTIME) while the grantor
// reclaims LATE on a replicated logical deadline. This is the holder surface; the Stage-1 BudgetClient is
// the controller surface.

// ErrBudgetUnavailable is returned by Spend when the cached lease has no capacity to satisfy the request
// (drained, expired, or self-fenced). It is a normal back-pressure signal — the caller serves a no-budget
// fallback; underspend is acceptable, overspend never is. A refill is triggered off-path.
var ErrBudgetUnavailable = errors.New("wavespan: budget unavailable (no cached lease capacity)")

// ErrPacingThrottled is returned by Spend when the node-side token bucket has fewer than the requested
// tokens — the spend is paced out, to be retried after tokens accrue. Distinct from ErrBudgetUnavailable:
// capacity exists, but delivery is rate-limited.
var ErrPacingThrottled = errors.New("wavespan: pacing throttled (node token bucket empty)")

// defaultChunkUnits is the per-refill Draw size when WithChunk is not given.
const defaultChunkUnits int64 = 1000

// nsPerSec is the nanoseconds-per-second divisor for the node token bucket (nowMono is in ns).
const nsPerSec int64 = 1_000_000_000

// BudgetKey identifies a budget pool: its namespace and budget id (the same (namespace, budget) pair the
// Stage-1 BudgetClient addresses).
type BudgetKey struct {
	Namespace string
	Budget    []byte
}

// LeasedBudgetClient is the node-side lease-cache surface. Obtain one via [Client.LeasedBudget].
type LeasedBudgetClient struct {
	c *Client
}

// LeasedBudget returns the node-side LeasedBudget cache client (§4). Distinct from [Client.Budget], which
// is the controller surface (Define/Grant/Report/Return/Stat).
func (c *Client) LeasedBudget() *LeasedBudgetClient { return &LeasedBudgetClient{c: c} }

// acquireOptions configures the node pacing gate and refill chunking for an Acquire. ttl/self_guard/
// max_pause are NOT set here — they come echoed from the grant result at refill (§4.2), so the node never
// hard-codes the server's timing.
type acquireOptions struct {
	rate  int64 // node token-bucket refill rate (units/sec); 0 disables node pacing
	burst int64 // node token-bucket ceiling (units); 0 defaults to chunk
	chunk int64 // per-refill Draw size (units)
}

// AcquireOption customizes an Acquire.
type AcquireOption func(*acquireOptions)

// WithRate sets the node-side token-bucket refill rate (units/sec) that shapes per-spend delivery. Zero
// (the default) disables node pacing — Spend never returns ErrPacingThrottled.
func WithRate(unitsPerSec int64) AcquireOption {
	return func(o *acquireOptions) { o.rate = unitsPerSec }
}

// WithBurst sets the node-side token-bucket ceiling (units). Zero defaults to the chunk size.
func WithBurst(units int64) AcquireOption {
	return func(o *acquireOptions) { o.burst = units }
}

// WithChunk sets the per-refill Draw size (units). Larger chunks touch consensus less often but waste more
// on a crash (bounded by 2·chunk). Zero defaults to defaultChunkUnits.
func WithChunk(units int64) AcquireOption {
	return func(o *acquireOptions) { o.chunk = units }
}

// leaseChunk is one drawn lease held in the node cache: its remaining capacity, its own monotonic stop
// deadline, the units already spent from it (cumulative, reported at refill/return), the granted amount,
// and the server lease_id it was drawn under.
type leaseChunk struct {
	remaining   int64
	deadlineMon int64 // suspend-aware monotonic deadline (ns); Spend stops when now >= deadlineMon
	spent       int64 // units spent from this chunk so far (cumulative)
	amount      int64 // units granted for this chunk
	leaseID     []byte
}

// budgetCell is the in-memory lease cache for one Acquired budget: a double-buffered cur/next pair of
// drawn chunks plus a node-side token bucket. All spend decisions read a single suspend-aware monotonic
// clock (injected as now, defaulting to nowMono) under mu, immediately before the decrement (§4.1). The
// lock is NEVER held across an RPC — refills run off-path, single-flight.
type budgetCell struct {
	mu  sync.Mutex
	now func() int64 // injectable clock (ns); nowMono in production, a fake in tests

	cur  *leaseChunk
	next *leaseChunk

	// node pacing token bucket (from Acquire opts; rate == 0 disables pacing).
	rate         int64
	burst        int64
	tokens       int64
	lastTokenMon int64 // monotonic time of the last token accrual (ns)

	chunk        int64 // per-refill Draw size (units)
	lowWatermark int64 // refill trigger threshold on cur.remaining

	// maxPauseNs is the self-fence budget (ns); 0 disables the fence. Set from the grant echo's
	// max_pause_budget_ms at refill (§4.2) — the node does not hard-code it.
	maxPauseNs  int64
	lastSeenMon int64 // monotonic time of the last served spend (0 = un-anchored)

	// triggerRefill kicks an off-path, single-flight refill. Set by Acquire; nil in pure cell unit tests.
	triggerRefill func()
}

// Budget is a handle to one Acquired budget's node-side cache. Spend is zero-coordination; Return folds the
// final spend and credits the unspent remainder back on graceful shutdown.
type Budget struct {
	lb   *LeasedBudgetClient
	key  BudgetKey
	cell *budgetCell
}

// Acquire returns a node-side cache for the (namespace, budget) pool. The pacing gate (WithRate/WithBurst)
// and refill chunk size (WithChunk) are node-local; the lease timing (ttl/self_guard/max_pause) is echoed
// from the server on each refill and installed on the cell, so the node never hard-codes the server's
// clock model. The first Spend (or the initial refill wired in a later step) fills the cache.
func (lb *LeasedBudgetClient) Acquire(ctx context.Context, key BudgetKey, opts ...AcquireOption) (*Budget, error) {
	o := acquireOptions{chunk: defaultChunkUnits}
	for _, fn := range opts {
		fn(&o)
	}
	if o.chunk <= 0 {
		o.chunk = defaultChunkUnits
	}
	if o.burst <= 0 {
		o.burst = o.chunk
	}
	cell := &budgetCell{
		now:          nowMono,
		rate:         o.rate,
		burst:        o.burst,
		tokens:       o.burst, // a token bucket starts full (mirrors the server seeding, §3.1)
		lastTokenMon: nowMono(),
		chunk:        o.chunk,
		lowWatermark: lowWatermarkFor(o.chunk),
	}
	b := &Budget{lb: lb, key: key, cell: cell}
	return b, nil
}

// lowWatermarkFor picks the refill trigger threshold: refill when cur drops below ~25% of a chunk, so the
// next chunk is in flight well before the current one drains (bounds underspend without thrashing).
func lowWatermarkFor(chunk int64) int64 {
	lw := chunk / 4
	if lw < 1 {
		lw = 1
	}
	return lw
}

// Spend consumes n units from the node cache with no RPC and no Raft on the fast path (§4.1). It returns
// nil on success, ErrPacingThrottled when the node token bucket is empty, or ErrBudgetUnavailable when the
// cached lease cannot satisfy the spend (drained / expired / self-fenced), in which case a refill is
// triggered off-path. n <= 0 is a no-op.
func (b *Budget) Spend(n int64) error { return b.cell.Spend(n) }

// Remaining reports the units currently cached across cur and next (a hint, not a guarantee — a concurrent
// Spend or self-fence may change it).
func (b *Budget) Remaining() int64 {
	c := b.cell
	c.mu.Lock()
	defer c.mu.Unlock()
	var r int64
	if c.cur != nil {
		r += c.cur.remaining
	}
	if c.next != nil {
		r += c.next.remaining
	}
	return r
}

// Spend implements the §4.1 fast path EXACTLY in order. now is re-read under the lock immediately before
// the decrement (not latched earlier). The lock is never held across the refill trigger (run after unlock).
func (c *budgetCell) Spend(n int64) error {
	if n <= 0 {
		return nil
	}
	c.mu.Lock()
	now := c.now() // re-read under the lock, immediately before the decrement (§16 #6/#B)

	// (a) self-fence — a monotonic gap past the pause budget means the lease may have been reclaimed
	// during a suspend/pause: drop cur/next and serve nothing. NO report (I-1).
	if c.maxPauseNs > 0 && c.lastSeenMon != 0 && now-c.lastSeenMon > c.maxPauseNs {
		c.cur = nil
		c.next = nil
		c.lastSeenMon = now
		trigger := c.triggerRefill
		c.mu.Unlock()
		if trigger != nil {
			trigger()
		}
		return ErrBudgetUnavailable
	}

	// (b) pacing gate — node token bucket; if fewer tokens than requested have accrued, pace out.
	if c.rate > 0 {
		c.accrueTokensLocked(now)
		if c.tokens < n {
			c.mu.Unlock()
			return ErrPacingThrottled
		}
	}

	// (c) drop cur if it is past its own monotonic deadline.
	if c.cur != nil && now >= c.cur.deadlineMon {
		c.cur = nil
	}

	// (d) fast path: decrement the cached chunk, debit a token, and trigger a refill at the low watermark.
	if c.cur != nil && c.cur.remaining >= n {
		c.cur.remaining -= n
		c.cur.spent += n
		if c.rate > 0 {
			c.tokens -= n
		}
		c.lastSeenMon = now
		needRefill := c.cur.remaining < c.lowWatermark && c.next == nil
		trigger := c.triggerRefill
		c.mu.Unlock()
		if needRefill && trigger != nil {
			trigger()
		}
		return nil
	}

	// (e) STRICT empty — nothing cached can satisfy the spend; trigger a refill and report unavailable.
	trigger := c.triggerRefill
	c.mu.Unlock()
	if trigger != nil {
		trigger()
	}
	return ErrBudgetUnavailable
}

// accrueTokensLocked tops up the node token bucket for the monotonic time elapsed since the last accrual,
// capped at burst. Called with mu held. Overflow-safe: a gap longer than a full burst-refill just tops up.
func (c *budgetCell) accrueTokensLocked(now int64) {
	if c.rate <= 0 {
		return
	}
	elapsed := now - c.lastTokenMon
	if elapsed <= 0 {
		// Monotone-forward only; never accrue on a backward/zero step (the injected clock or a coarse
		// timer can repeat a reading).
		c.lastTokenMon = now
		return
	}
	// If the gap would refill more than a full burst, just top up — avoids rate*elapsed overflow.
	fullRefillNs := (c.burst/maxI64(c.rate, 1) + 1) * nsPerSec
	if elapsed >= fullRefillNs {
		c.tokens = c.burst
	} else {
		c.tokens += c.rate * elapsed / nsPerSec
		if c.tokens > c.burst {
			c.tokens = c.burst
		}
	}
	c.lastTokenMon = now
}

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
