package local

import (
	"context"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/placement"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// Cluster exposes the live roster (satisfied by membership.Service).
type Cluster interface {
	Members() []membership.MemberView
}

// FanoutJob asks the background worker to fill a key up to the target durable-holder count.
type FanoutJob struct {
	Namespace string
	Key       []byte
	Record    *wavespanv1.StoredRecord
}

// Fanout is the asynchronous target-N fill worker (design/05 write state machine FANOUT_TARGET_N).
// It runs after the coordinator has already ACKed origin+1, so a failure here never fails the
// write — it records the gap for the repair engine instead.
type Fanout struct {
	self          membership.Member
	cluster       Cluster
	graph         *latencygraph.Graph
	replicator    Replicator
	holders       *HolderDirectory
	repair        *RepairEngine
	policy        placement.Policy
	targetHolders int
	writeTimeout  time.Duration
	jobs          chan FanoutJob
	everywhere    func(namespace string) bool // namespaces replicated to every node
}

// SetEverywhere installs the predicate selecting namespaces that replicate to all nodes.
func (f *Fanout) SetEverywhere(fn func(namespace string) bool) { f.everywhere = fn }

func (f *Fanout) isEverywhere(ns string) bool { return f.everywhere != nil && f.everywhere(ns) }

// NewFanout wires a fanout worker. targetHolders is origin + target nearby replicas.
func NewFanout(self membership.Member, cluster Cluster, graph *latencygraph.Graph, replicator Replicator, holders *HolderDirectory, policy placement.Policy, writeTimeout time.Duration) *Fanout {
	return &Fanout{
		self: self, cluster: cluster, graph: graph, replicator: replicator, holders: holders,
		policy: policy, targetHolders: policy.TargetNearbyReplicas + 1, writeTimeout: writeTimeout,
		jobs: make(chan FanoutJob, 4096),
	}
}

// SetRepair connects the repair engine that receives gaps fanout could not fill.
func (f *Fanout) SetRepair(r *RepairEngine) { f.repair = r }

// Enqueue schedules a background fill; it never blocks (a full queue drops to repair).
func (f *Fanout) Enqueue(job FanoutJob) {
	select {
	case f.jobs <- job:
	default:
		if f.repair != nil {
			f.repair.Enqueue(RepairItem{Namespace: job.Namespace, Key: job.Key, Record: job.Record})
		}
	}
}

// Run processes fanout jobs until ctx is done.
func (f *Fanout) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-f.jobs:
			f.Fill(ctx, job)
		}
	}
}

// capTarget bounds the target durable-holder count by the number of alive members, so a small
// cluster is not perpetually "under-replicated" (which would make repair churn forever).
func capTarget(target int, members []membership.MemberView) int {
	alive := 0
	for _, m := range members {
		if m.State == membership.StateAlive {
			alive++
		}
	}
	if target > alive {
		return alive
	}
	return target
}

// Fill replicates to additional candidates until the target durable-holder count is reached.
// Any shortfall is handed to the repair engine.
func (f *Fanout) Fill(ctx context.Context, job FanoutJob) {
	if f.isEverywhere(job.Namespace) {
		f.fillEverywhere(ctx, job)
		return
	}
	target := capTarget(f.targetHolders, f.cluster.Members())
	if f.holders.Count(job.Namespace, job.Key) >= target {
		return
	}
	v := version.FromProto(job.Record.GetVersion())
	req := BuildRequest(job.Namespace, job.Key, job.Record, f.self.MemberID)

	cands, err := placement.Select(f.self, f.cluster.Members(), f.graph, f.policy)
	if err == nil {
		for _, c := range cands {
			if f.holds(job.Namespace, job.Key, c.Member.MemberID) {
				continue
			}
			callCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
			resp, rerr := f.replicator.StoreReplica(callCtx, c.Member, req)
			cancel()
			if rerr == nil && resp.GetDurable() {
				f.holders.RecordHolder(job.Namespace, job.Key, c.Member.MemberID, v)
				if f.holders.Count(job.Namespace, job.Key) >= target {
					return
				}
			}
		}
	}
	// could not reach target: record the gap for repair (design/05 failure paths).
	if f.holders.Count(job.Namespace, job.Key) < target && f.repair != nil {
		f.repair.Enqueue(RepairItem{Namespace: job.Namespace, Key: job.Key, Record: job.Record})
	}
}

// fillEverywhere replicates the record to EVERY alive member (the "replicate everywhere" policy).
// Any member it cannot reach is handed to repair, which re-pushes until all alive nodes hold it.
func (f *Fanout) fillEverywhere(ctx context.Context, job FanoutJob) {
	v := version.FromProto(job.Record.GetVersion())
	req := BuildRequest(job.Namespace, job.Key, job.Record, f.self.MemberID)
	missed := false
	for _, m := range f.cluster.Members() {
		if m.State != membership.StateAlive || m.Member.MemberID == f.self.MemberID {
			continue
		}
		if f.holds(job.Namespace, job.Key, m.Member.MemberID) {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
		resp, rerr := f.replicator.StoreReplica(callCtx, m.Member, req)
		cancel()
		if rerr == nil && resp.GetDurable() {
			f.holders.RecordHolder(job.Namespace, job.Key, m.Member.MemberID, v)
		} else {
			missed = true
		}
	}
	if missed && f.repair != nil {
		f.repair.Enqueue(RepairItem{Namespace: job.Namespace, Key: job.Key, Record: job.Record})
	}
}

func (f *Fanout) holds(namespace string, key []byte, member string) bool {
	for _, h := range f.holders.Holders(namespace, key) {
		if h == member {
			return true
		}
	}
	return false
}
