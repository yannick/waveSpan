package collections

import (
	"context"
	"encoding/binary"
	"sync"
	"time"
)

// proposer is the batching/pipelining write driver (QW2, design/32 §3.1). It generalizes bulk.go's
// fan-out into a first-class component used by every single-op write: callers enqueue an encoded
// command for a shard and block on a result; a per-shard flusher coalesces commands arriving within a
// small time/size window into ONE Raft entry (an opBatch wrapper) and proposes it once, so dragonboat
// applies N ops in a single Update. Per-op results (including WRONGTYPE/NOTNUM/FROZEN sentinels and
// idempotency-dedup outcomes) are routed back to each caller from the batch's packed result.
//
// Coalescing is safe: each sub-command keeps its own NS/Coll/Idem/Items, the SM applies them in log
// order with the same in-batch overlays it already uses for a multi-entry Update, and dedup keys still
// dedup per sub-command. The proposer keys queues by shard id, so when there are N data shards (D1) it
// fans batches out per shard automatically.
type proposer struct {
	shard   asyncShard
	maxWait time.Duration // coalescing window
	maxOps  int           // max sub-commands per coalesced entry

	mu     sync.Mutex
	queues map[uint64]*shardQueue
}

// asyncShard is the engine surface the proposer drives. Manager implements it; tests can fake it.
type asyncShard interface {
	// Propose commits one already-encoded command and returns its apply result.
	Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error)
}

// proposeJob is one queued single-op write awaiting a result.
type proposeJob struct {
	cmd    []byte // a single-command encoding (appendCommand output)
	ctx    context.Context
	respCh chan proposeResp
}

type proposeResp struct {
	res ProposeResult
	err error
}

type shardQueue struct {
	shardID uint64
	jobs    chan *proposeJob
}

const (
	defaultCoalesceWindow = 200 * time.Microsecond
	defaultCoalesceMaxOps = 256
	proposeQueueDepth     = 4096 // bounded pending jobs per shard (backpressure)
)

// newProposer builds a batching proposer over the given engine. A zero window/maxOps uses defaults.
func newProposer(shard asyncShard, window time.Duration, maxOps int) *proposer {
	if window <= 0 {
		window = defaultCoalesceWindow
	}
	if maxOps <= 0 {
		maxOps = defaultCoalesceMaxOps
	}
	return &proposer{shard: shard, maxWait: window, maxOps: maxOps, queues: map[uint64]*shardQueue{}}
}

// Propose enqueues an encoded single command for shardID and blocks until it (or its coalesced batch)
// commits, returning that op's apply result.
//
// CRITICAL ROBUSTNESS (incident: a pooled encode buffer reused before dragonboat durably copied the
// proposed bytes corrupted a committed Raft entry → "short command" → all voters crash-looped). The
// caller may pass a POOLED buffer (collections.go encodeCommandPooled) and release it the instant Propose
// returns — including when this call returns EARLY on ctx cancellation, while the flusher goroutine still
// holds the bytes and has not yet proposed/copied them. So the proposer takes its OWN copy of cmd at
// enqueue: the job, the single-command dispatch, and the coalesced batch all work off proposer-owned
// bytes, fully decoupling the caller's buffer lifetime from the async propose. Correctness over the alloc.
func (p *proposer) Propose(ctx context.Context, shardID uint64, cmd []byte) (ProposeResult, error) {
	q := p.queueFor(shardID)
	owned := append([]byte(nil), cmd...) // copy off any pooled/aliased caller buffer before going async
	job := &proposeJob{cmd: owned, ctx: ctx, respCh: make(chan proposeResp, 1)}
	// ADMISSION CONTROL (load shedding): the per-shard queue is bounded (proposeQueueDepth). When it is
	// full the node is already saturated, so enqueue NON-BLOCKING — a full queue fast-rejects with ErrBusy
	// (mapped to ResourceExhausted) rather than letting callers pile up an unbounded backlog that drives
	// the node toward OOM/timeout collapse. The flood is shed; the node stays up and keeps serving.
	select {
	case q.jobs <- job:
	case <-ctx.Done():
		return ProposeResult{}, ctx.Err()
	default:
		return ProposeResult{}, ErrBusy
	}
	select {
	case r := <-job.respCh:
		return r.res, r.err
	case <-ctx.Done():
		return ProposeResult{}, ctx.Err()
	}
}

func (p *proposer) queueFor(shardID uint64) *shardQueue {
	p.mu.Lock()
	defer p.mu.Unlock()
	if q, ok := p.queues[shardID]; ok {
		return q
	}
	q := &shardQueue{shardID: shardID, jobs: make(chan *proposeJob, proposeQueueDepth)}
	p.queues[shardID] = q
	go p.flush(q)
	return q
}

// flush is the per-shard coalescing loop: it gathers the first pending job, then drains any others that
// arrive within maxWait (up to maxOps), proposes them as one entry, and replies to each caller.
func (p *proposer) flush(q *shardQueue) {
	batch := make([]*proposeJob, 0, p.maxOps)
	for first := range q.jobs {
		batch = append(batch[:0], first)
		// Opportunistically drain whatever is already queued without blocking.
		for len(batch) < p.maxOps {
			select {
			case j := <-q.jobs:
				batch = append(batch, j)
			default:
				goto window
			}
		}
		p.dispatch(q.shardID, batch)
		continue
	window:
		// Small window to let more ops accumulate (only worth it when there is real concurrency).
		if len(batch) < p.maxOps {
			timer := time.NewTimer(p.maxWait)
		fill:
			for len(batch) < p.maxOps {
				select {
				case j := <-q.jobs:
					batch = append(batch, j)
				case <-timer.C:
					break fill
				}
			}
			timer.Stop()
		}
		p.dispatch(q.shardID, batch)
	}
}

// dispatch proposes one batch of jobs as a single Raft entry and routes per-op results back. A
// single-job batch uses the plain single-command path (back-compat, no wrapper overhead).
//
// ROBUSTNESS under load: a caller that cancels (tight deadline / disconnect) returns from Propose while
// its job is still queued (the enqueue is buffered). Such an orphan job must NOT be proposed — its
// already-expired deadline would otherwise poison mergeDeadlines for the WHOLE batch (dragonboat rejects
// a past deadline with ErrInvalidDeadline), failing healthy co-batched writes. So dispatch first drops
// every job whose context is already done, replying to it with its own ctx error, and proposes only the
// live remainder. This keeps one slow/cancelled client from taking down a coalesced batch of good writes.
func (p *proposer) dispatch(shardID uint64, jobs []*proposeJob) {
	live := jobs[:0]
	for _, j := range jobs {
		if err := j.ctx.Err(); err != nil {
			j.respCh <- proposeResp{err: err} // orphaned (cancelled) before flush — reject it alone
			continue
		}
		live = append(live, j)
	}
	jobs = live
	if len(jobs) == 0 {
		return
	}
	if len(jobs) == 1 {
		j := jobs[0]
		res, err := p.shard.Propose(j.ctx, shardID, j.cmd)
		j.respCh <- proposeResp{res: res, err: err}
		return
	}
	// Use the earliest deadline among the (live) jobs so no caller waits past its own context.
	ctx, cancel := mergeDeadlines(jobs)
	defer cancel()
	cmds := make([][]byte, len(jobs))
	for i, j := range jobs {
		cmds[i] = j.cmd
	}
	enc := encodeBatchInto(make([]byte, 0, batchSize(cmds)), cmds)
	res, err := p.shard.Propose(ctx, shardID, enc)
	if err != nil {
		for _, j := range jobs {
			j.respCh <- proposeResp{err: err}
		}
		return
	}
	results, derr := decodeBatchResult(res.Data, len(jobs))
	if derr != nil {
		for _, j := range jobs {
			j.respCh <- proposeResp{err: derr}
		}
		return
	}
	for i, j := range jobs {
		j.respCh <- proposeResp{res: results[i]}
	}
}

func batchSize(cmds [][]byte) int {
	n := 5
	for _, c := range cmds {
		n += 4 + len(c)
	}
	return n
}

// mergeDeadlines returns a context whose deadline is the earliest among the jobs' contexts (or no
// deadline if none has one), and is cancelled if any job's context is cancelled.
func mergeDeadlines(jobs []*proposeJob) (context.Context, context.CancelFunc) {
	var earliest time.Time
	for _, j := range jobs {
		if dl, ok := j.ctx.Deadline(); ok {
			if earliest.IsZero() || dl.Before(earliest) {
				earliest = dl
			}
		}
	}
	if earliest.IsZero() {
		return context.WithCancel(context.Background())
	}
	return context.WithDeadline(context.Background(), earliest)
}

// --- batch result codec (per-op apply results packed into the entry's Result.Data) ---
//
// Layout: uint32(count) || (be(value) || chunk(data))*  — one (value,data) per sub-command, in order.

func encodeBatchResult(dst []byte, results []ProposeResult) []byte {
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(results)))
	dst = append(dst, cnt[:]...)
	for _, r := range results {
		dst = append(dst, u64(r.Value)...)
		dst = appendChunk(dst, r.Data)
	}
	return dst
}

func decodeBatchResult(b []byte, want int) ([]ProposeResult, error) {
	if len(b) < 4 {
		return nil, errShortCommand
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	if n != want {
		return nil, errShortCommand
	}
	rest := b[4:]
	out := make([]ProposeResult, 0, n)
	for i := 0; i < n; i++ {
		if len(rest) < 8 {
			return nil, errShortCommand
		}
		val := binary.BigEndian.Uint64(rest[:8])
		rest = rest[8:]
		data, r2, err := takeChunk(rest)
		if err != nil {
			return nil, err
		}
		rest = r2
		// Copy data out of the entry's result buffer so it outlives the proposal.
		out = append(out, ProposeResult{Value: val, Data: append([]byte(nil), data...)})
	}
	return out, nil
}
