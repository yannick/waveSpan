package local

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/placement"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// RecordReader reads the winning local record for a key, and scans a local range (the local
// recordstore).
type RecordReader interface {
	GetRecord(namespace string, key []byte) (*wavespanv1.StoredRecord, bool, error)
	ScanRange(namespace string, start, end []byte, limit int, nowMs int64) ([]recordstore.ScanRow, error)
	// ScanRecordsFrom pages the full winning records of a namespace from a cursor (for backfill).
	ScanRecordsFrom(namespace string, start []byte, limit int) ([]*wavespanv1.StoredRecord, []byte, error)
}

// RepairConfig tunes the repair engine (design/23_repair_engine.md).
type RepairConfig struct {
	WriteTimeout time.Duration
	// IsAlive reports whether a holder member is still reachable (roster liveness).
	IsAlive func(memberID string) bool
	// ChurnHigh reports whether suspect/dead churn is high, so repair should back off rather
	// than amplify instability.
	ChurnHigh func() bool
	// NowMs supplies the clock (injectable for tests).
	NowMs func() int64
}

// RepairEngine restores under-replicated keys to the target durable-holder count under spot
// churn (design/05 "Repair loop", design/23). It is a severity-ordered priority queue with a
// rate limit and churn backpressure.
type RepairEngine struct {
	self          membership.Member
	cluster       Cluster
	graph         *latencygraph.Graph
	replicator    Replicator
	holders       *HolderDirectory
	reader        RecordReader
	policy        placement.Policy
	targetHolders int
	cfg           RepairConfig
	everywhere    func(namespace string) bool

	mu      sync.Mutex
	queue   repairQueue
	pending map[string]bool

	// backfillCursor is the per-namespace resume point for the periodic under-replication sweep.
	// Touched only by the single Backfill goroutine, so it needs no lock.
	backfillCursor map[string][]byte
}

// SetEverywhere installs the predicate selecting namespaces replicated to all nodes (their repair
// target is the full alive set, and repair pushes to every missing alive member).
func (r *RepairEngine) SetEverywhere(fn func(namespace string) bool) { r.everywhere = fn }

func (r *RepairEngine) isEverywhere(ns string) bool { return r.everywhere != nil && r.everywhere(ns) }

func (r *RepairEngine) aliveMemberCount() int {
	n := 0
	for _, m := range r.cluster.Members() {
		if m.State == membership.StateAlive {
			n++
		}
	}
	return n
}

// NewRepairEngine wires a repair engine.
func NewRepairEngine(self membership.Member, cluster Cluster, graph *latencygraph.Graph, replicator Replicator, holders *HolderDirectory, reader RecordReader, policy placement.Policy, cfg RepairConfig) *RepairEngine {
	if cfg.IsAlive == nil {
		cfg.IsAlive = func(string) bool { return true }
	}
	if cfg.ChurnHigh == nil {
		cfg.ChurnHigh = func() bool { return false }
	}
	if cfg.NowMs == nil {
		cfg.NowMs = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 2 * time.Second
	}
	return &RepairEngine{
		self: self, cluster: cluster, graph: graph, replicator: replicator, holders: holders,
		reader: reader, policy: policy, targetHolders: policy.TargetNearbyReplicas + 1, cfg: cfg,
		pending: map[string]bool{}, backfillCursor: map[string][]byte{},
	}
}

// effectiveTarget caps the target durable-holder count by the alive cluster size.
func (r *RepairEngine) effectiveTarget() int {
	return capTarget(r.targetHolders, r.cluster.Members())
}

// effectiveTargetFor is the target holder count for a namespace: every alive node for an
// "everywhere" namespace, otherwise the capped nearby target.
func (r *RepairEngine) effectiveTargetFor(ns string) int {
	if r.isEverywhere(ns) {
		return r.aliveMemberCount()
	}
	return r.effectiveTarget()
}

// aliveHolderCount counts a key's holders that are still alive.
func (r *RepairEngine) aliveHolderCount(namespace string, key []byte) int {
	n := 0
	for _, h := range r.holders.Holders(namespace, key) {
		if r.cfg.IsAlive(h) {
			n++
		}
	}
	return n
}

// Enqueue schedules a key for repair if it is under-replicated and not already queued.
func (r *RepairEngine) Enqueue(item RepairItem) {
	deficit := r.effectiveTarget() - r.aliveHolderCount(item.Namespace, item.Key)
	if deficit <= 0 {
		return
	}
	id := keyID(item.Namespace, item.Key)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending[id] {
		return
	}
	item.Deficit = deficit
	item.EnqueuedAtMs = r.cfg.NowMs()
	r.pending[id] = true
	heap.Push(&r.queue, &item)
}

// OnMemberDead removes a dead member from holder sets and enqueues every key it held that is now
// under-replicated (design/05 "Repair loop" inputs: dead/suspect members).
func (r *RepairEngine) OnMemberDead(memberID string) {
	for _, id := range r.holders.keysHeldBy(memberID) {
		ns, key := splitKeyID(id)
		r.holders.RemoveHolder(ns, key, memberID)
		rec, found, err := r.reader.GetRecord(ns, key)
		if err != nil || !found {
			continue // we don't hold the record locally; another holder repairs it
		}
		r.Enqueue(RepairItem{Namespace: ns, Key: key, Record: rec})
	}
}

// QueueDepth returns the number of queued repair items.
func (r *RepairEngine) QueueDepth() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queue.Len()
}

// pop removes the highest-severity item.
func (r *RepairEngine) pop() (*RepairItem, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.queue.Len() == 0 {
		return nil, false
	}
	it := heap.Pop(&r.queue).(*RepairItem)
	delete(r.pending, keyID(it.Namespace, it.Key))
	return it, true
}

// ProcessOne repairs the most under-replicated key. It returns false when the queue is empty.
func (r *RepairEngine) ProcessOne(ctx context.Context) bool {
	it, ok := r.pop()
	if !ok {
		return false
	}
	target := r.effectiveTargetFor(it.Namespace)
	if r.aliveHolderCount(it.Namespace, it.Key) >= target {
		return true // already healed
	}
	rec := it.Record
	if rec == nil {
		got, found, err := r.reader.GetRecord(it.Namespace, it.Key)
		if err != nil || !found {
			return true // cannot repair without the record locally
		}
		rec = got
	}
	v := version.FromProto(rec.GetVersion())
	req := BuildRequest(it.Namespace, it.Key, rec, r.self.MemberID)

	cands := r.repairCandidates(it.Namespace)
	for _, c := range cands {
		if r.isHolder(it.Namespace, it.Key, c.Member.MemberID) {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, r.cfg.WriteTimeout)
		resp, rerr := r.replicator.StoreReplica(callCtx, c.Member, req)
		cancel()
		if rerr == nil && resp.GetDurable() {
			r.holders.RecordHolder(it.Namespace, it.Key, c.Member.MemberID, v)
			if r.aliveHolderCount(it.Namespace, it.Key) >= target {
				return true
			}
		}
	}
	// still short: re-enqueue for another pass (a future candidate may become available).
	if r.aliveHolderCount(it.Namespace, it.Key) < target {
		r.Enqueue(RepairItem{Namespace: it.Namespace, Key: it.Key, Record: rec})
	}
	return true
}

// repairCandidates returns the members repair may push to: every alive member for an "everywhere"
// namespace, otherwise the latency-graph-selected nearby candidates.
func (r *RepairEngine) repairCandidates(ns string) []placement.Candidate {
	if r.isEverywhere(ns) {
		var cands []placement.Candidate
		for _, m := range r.cluster.Members() {
			if m.State == membership.StateAlive && m.Member.MemberID != r.self.MemberID {
				cands = append(cands, placement.Candidate{Member: m.Member})
			}
		}
		return cands
	}
	cands, err := placement.Select(r.self, r.cluster.Members(), r.graph, r.policy)
	if err != nil {
		return nil
	}
	return cands
}

func (r *RepairEngine) isHolder(namespace string, key []byte, member string) bool {
	for _, h := range r.holders.Holders(namespace, key) {
		if h == member {
			return true
		}
	}
	return false
}

// Drain processes the queue until empty or no progress is possible (test helper).
func (r *RepairEngine) Drain(ctx context.Context) {
	// bound iterations to avoid an infinite re-enqueue loop when target is unreachable
	for i := 0; i < r.QueueDepth()*4+8; i++ {
		if !r.ProcessOne(ctx) {
			return
		}
	}
}

// Run drains the queue with `workers` concurrent workers (min 1). Each worker processes queued
// items back-to-back at full speed; when the queue is empty (or churn is high) it waits one interval
// before re-checking. So an idle engine costs one poll per interval per worker, while a backlog
// drains at the full worker concurrency instead of one item per tick.
func (r *RepairEngine) Run(ctx context.Context, interval time.Duration, workers int) {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				if ctx.Err() != nil {
					return
				}
				// Back off on churn, and idle-wait when the queue is empty; otherwise loop
				// immediately so a backlog drains as fast as the workers can push.
				if r.cfg.ChurnHigh() || !r.ProcessOne(ctx) {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
					}
				}
			}
		}()
	}
	wg.Wait()
}

// BackfillOnce scans up to `batch` of this node's records per namespace from a rolling cursor and
// enqueues any that are under-replicated. Enqueue is a no-op for keys already at target, so this is
// a cheap periodic detector that feeds the repair queue for keys that fell short of target at write
// time (e.g. a peer was briefly unreachable / the cluster was mid-converge) and were never otherwise
// re-enqueued — without it, the push queue stays empty while keys sit under-replicated forever.
func (r *RepairEngine) BackfillOnce(ctx context.Context, namespaces []string, batch int) {
	if batch <= 0 {
		batch = 1024
	}
	for _, ns := range namespaces {
		if ctx.Err() != nil {
			return
		}
		recs, next, err := r.reader.ScanRecordsFrom(ns, r.backfillCursor[ns], batch)
		if err != nil {
			continue
		}
		r.backfillCursor[ns] = next // nil => sweep restarts from the top of the namespace
		for _, rec := range recs {
			r.Enqueue(RepairItem{Namespace: ns, Key: rec.GetLogicalKey(), Record: rec})
		}
	}
}

// Backfill periodically sweeps the keyspace enqueuing under-replicated keys, until ctx is done.
func (r *RepairEngine) Backfill(ctx context.Context, namespaces []string, interval time.Duration, batch int) {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r.cfg.ChurnHigh() {
				continue // don't add repair load while the cluster is unstable
			}
			r.BackfillOnce(ctx, namespaces, batch)
		}
	}
}
