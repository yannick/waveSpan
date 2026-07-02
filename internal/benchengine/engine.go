// Package benchengine is the controllable, streaming benchmark run engine: it runs workloads with
// start/pause/resume/stop control and emits per-second windowed throughput + latency samples.
package benchengine

import (
	"context"
	"fmt"
	"sync"
	"time"

	benchqueries "github.com/yannick/wavespan/bench"
)

// State is the lifecycle phase of a Run.
type State int

// Run lifecycle states.
const (
	StateIdle State = iota
	StateRunning
	StatePaused
	StateStopped
	StateDone
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRunning:
		return "running"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	case StateDone:
		return "done"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// WorkloadSpec selects one workload kind and its parameters.
type WorkloadSpec struct {
	Kind   string
	Params map[string]any
}

// Config is a full benchmark run definition.
type Config struct {
	DataAddr      string
	Graph         string
	Workloads     []WorkloadSpec
	Concurrency   int
	Duration      time.Duration
	CypherQueries []benchqueries.Query

	// ShardAware (opt-in, default false) routes collection writes directly to each shard's leader,
	// eliminating the per-op forward hop. When set, the set/hash/zset/bulkremove workloads use a
	// bench.ShardAwareClient over Cores instead of the single-address path. Cores is the ordered list
	// of core data addresses (index i = replicaId i+1); DataShards is the hash directory width N.
	// Default (false) leaves the existing single-DataAddr path byte-for-byte unchanged.
	ShardAware bool
	Cores      []string
	DataShards int
}

// pauseGate blocks workers while the run is paused. Pause takes the write lock (in-flight ops
// drain, new ops block); Resume releases it. Workers wrap each op in wait().
type pauseGate struct{ mu sync.RWMutex }

func (g *pauseGate) wait() {
	g.mu.RLock()
	g.mu.RUnlock() //nolint:staticcheck // intentional: gate, not a critical section
}
func (g *pauseGate) pause()  { g.mu.Lock() }
func (g *pauseGate) resume() { g.mu.Unlock() }

// collector holds the per-workload counters and histograms. Guarded by mu.
type collector struct {
	label string
	op    func(context.Context) error

	mu       sync.Mutex
	cur      *Hist  // current window (reset each tick)
	cum      *Hist  // cumulative over whole run
	total    uint64 // total ops this window (reset each tick)
	cumTotal uint64 // cumulative successful ops
	errs     uint64 // errors this window (reset each tick)
	cumErrs  uint64 // cumulative errors
}

// Summary is the cumulative per-workload result of a run.
type Summary struct {
	PerWorkload map[string]WindowStat `json:"perWorkload"`
}

// Run is a controllable, streaming benchmark execution.
type Run struct {
	cfg         Config
	concurrency int

	mu    sync.Mutex // guards state
	state State

	collectors []*collector

	gate pauseGate

	ctx    context.Context
	cancel context.CancelFunc

	subMu sync.Mutex
	subs  map[chan Sample]struct{}

	wg        sync.WaitGroup // workers + sampler
	startTime time.Time
	endTime   time.Time // set when the run reaches a terminal state; zero while running
	doneCh    chan struct{}
	stopOnce  sync.Once
}

// New builds a Run from a Config, wiring real client ops per workload.
func New(cfg Config) (*Run, error) {
	if len(cfg.Workloads) == 0 {
		return nil, fmt.Errorf("benchengine: need at least one workload")
	}
	if cfg.Concurrency < 1 {
		return nil, fmt.Errorf("benchengine: concurrency must be >= 1")
	}
	collectors := make([]*collector, 0, len(cfg.Workloads))
	for _, spec := range cfg.Workloads {
		op, label, err := opsFor(spec, cfg)
		if err != nil {
			return nil, err
		}
		collectors = append(collectors, newCollector(label, op))
	}
	return newRun(cfg, cfg.Concurrency, collectors), nil
}

func newCollector(label string, op func(context.Context) error) *collector {
	return &collector{label: label, op: op, cur: NewHist(), cum: NewHist()}
}

func newRun(cfg Config, concurrency int, collectors []*collector) *Run {
	ctx, cancel := context.WithCancel(context.Background())
	return &Run{
		cfg:         cfg,
		concurrency: concurrency,
		state:       StateIdle,
		collectors:  collectors,
		ctx:         ctx,
		cancel:      cancel,
		subs:        make(map[chan Sample]struct{}),
		doneCh:      make(chan struct{}),
	}
}

// newRunForTest builds a Run with a single workload driven by the injected op. Same-package test
// seam — does not call opsFor or build real clients.
func newRunForTest(op func(context.Context) error, workers int) *Run {
	cfg := Config{Concurrency: workers, Workloads: []WorkloadSpec{{Kind: "fake"}}}
	return newRun(cfg, workers, []*collector{newCollector("fake", op)})
}

// State returns the current lifecycle state.
func (r *Run) State() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Subscribe registers a buffered sample channel. The returned func unsubscribes and closes it.
func (r *Run) Subscribe() (<-chan Sample, func()) {
	ch := make(chan Sample, 8)
	r.subMu.Lock()
	r.subs[ch] = struct{}{}
	r.subMu.Unlock()
	var once sync.Once
	unsub := func() {
		once.Do(func() {
			r.subMu.Lock()
			if _, ok := r.subs[ch]; ok {
				delete(r.subs, ch)
				close(ch)
			}
			r.subMu.Unlock()
		})
	}
	return ch, unsub
}

// Start launches workers and the sampler (idle -> running).
func (r *Run) Start() {
	r.mu.Lock()
	if r.state != StateIdle {
		r.mu.Unlock()
		return
	}
	r.state = StateRunning
	r.startTime = time.Now()
	r.mu.Unlock()

	for _, c := range r.collectors {
		for i := 0; i < r.concurrency; i++ {
			r.wg.Add(1)
			go r.worker(c)
		}
	}
	r.wg.Add(1)
	go r.sampler()

	if r.cfg.Duration > 0 {
		go func() {
			select {
			case <-time.After(r.cfg.Duration):
				r.finish(StateDone)
			case <-r.ctx.Done():
			}
		}()
	}
}

func (r *Run) worker(c *collector) {
	defer r.wg.Done()
	for {
		if r.ctx.Err() != nil {
			return
		}
		r.gate.wait() // blocks while paused
		if r.ctx.Err() != nil {
			return
		}
		start := time.Now()
		err := c.op(r.ctx)
		d := time.Since(start)
		// The run window ended (or was stopped) mid-request: the op fails with context.Canceled, which
		// is the harness shutting itself down, not a real error. Drain it instead of counting it.
		if err != nil && r.ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		if err != nil {
			c.errs++
			c.cumErrs++
		} else {
			c.cur.Record(d)
			c.cum.Record(d)
			c.total++
			c.cumTotal++
		}
		c.mu.Unlock()
	}
}

func (r *Run) sampler() {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := time.Now()
	for {
		select {
		case <-r.ctx.Done():
			return
		case now := <-ticker.C:
			elapsed := now.Sub(last).Seconds()
			last = now
			perWL := make(map[string]WindowStat, len(r.collectors))
			for _, c := range r.collectors {
				c.mu.Lock()
				total := c.total
				errs := c.errs
				var tput float64
				if elapsed > 0 {
					tput = float64(total) / elapsed
				}
				ws := WindowStat{
					Tput:  tput,
					P50Ms: msOf(c.cur.Percentile(0.50)),
					P95Ms: msOf(c.cur.Percentile(0.95)),
					P99Ms: msOf(c.cur.Percentile(0.99)),
					Errs:  errs,
					Total: total,
				}
				c.cur = NewHist()
				c.total = 0
				c.errs = 0
				c.mu.Unlock()
				perWL[c.label] = ws
			}
			sample := Sample{
				TimeMs:      time.Since(r.startTime).Milliseconds(),
				PerWorkload: perWL,
			}
			r.broadcast(sample)
		}
	}
}

func (r *Run) broadcast(s Sample) {
	r.subMu.Lock()
	defer r.subMu.Unlock()
	for ch := range r.subs {
		select {
		case ch <- s:
		default: // drop if full — non-blocking
		}
	}
}

// Pause blocks new ops (running -> paused). The gate's physical Lock() is taken while holding
// r.mu so the logical state and the gate's lock state can never diverge under a concurrent
// Resume/finish (which would otherwise Unlock an un-Locked gate and fatally crash the process).
func (r *Run) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != StateRunning {
		return
	}
	r.state = StatePaused
	r.gate.pause()
}

// Resume continues ops (paused -> running). The gate's Unlock() is performed under r.mu so it can
// only ever run when a prior Pause took the gate's Lock (see Pause).
func (r *Run) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != StatePaused {
		return
	}
	r.state = StateRunning
	r.gate.resume()
}

// Stop terminates the run (-> stopped) and finalizes.
func (r *Run) Stop() { r.finish(StateStopped) }

func (r *Run) finish(target State) {
	r.stopOnce.Do(func() {
		// Set the terminal state, cancel ctx, and release the pause gate (if paused) all under
		// r.mu so the gate's Unlock can never race a concurrent Pause's Lock. wg.Wait() stays
		// OUTSIDE the lock: workers only ever RLock/RUnlock the gate and never take r.mu, so
		// holding r.mu across the gate op cannot deadlock — but waiting on workers under r.mu is
		// avoided regardless.
		r.mu.Lock()
		wasPaused := r.state == StatePaused
		r.state = target
		r.endTime = time.Now()
		r.cancel()
		if wasPaused {
			r.gate.resume()
		}
		r.mu.Unlock()
		r.wg.Wait()
		close(r.doneCh)
	})
}

// Summary returns cumulative per-workload stats over the whole run (valid after Stop / completion).
func (r *Run) Summary() Summary {
	perWL := make(map[string]WindowStat, len(r.collectors))
	r.mu.Lock()
	start, end := r.startTime, r.endTime
	r.mu.Unlock()
	// Use the run's actual span: computing elapsed at FETCH time silently deflated Tput for
	// anyone reading results after the run finished (found benchmarking ovh-stag, design/37).
	if end.IsZero() {
		end = time.Now()
	}
	elapsed := end.Sub(start).Seconds()
	for _, c := range r.collectors {
		c.mu.Lock()
		var tput float64
		if elapsed > 0 {
			tput = float64(c.cumTotal) / elapsed
		}
		perWL[c.label] = WindowStat{
			Tput:  tput,
			P50Ms: msOf(c.cum.Percentile(0.50)),
			P95Ms: msOf(c.cum.Percentile(0.95)),
			P99Ms: msOf(c.cum.Percentile(0.99)),
			Errs:  c.cumErrs,
			Total: c.cumTotal,
		}
		c.mu.Unlock()
	}
	return Summary{PerWorkload: perWL}
}

func msOf(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
