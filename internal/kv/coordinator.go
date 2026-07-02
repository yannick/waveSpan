// Package kv implements the public KV API: the origin+1 write coordinator, the local read path,
// and the Connect service (design/03_kv_store.md, design/05 write algorithm).
package kv

import (
	"context"
	"errors"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/placement"
	"github.com/yannick/wavespan/internal/recordstore"
	local "github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// ErrInsufficientNearbyReplicas is returned when origin+1 cannot be satisfied: no nearby durable
// replica acknowledged the write (design/05; ADR 0002). The origin copy is durable, but the write
// is not acknowledged.
var ErrInsufficientNearbyReplicas = errors.New("kv: insufficient nearby durable replicas for origin+1")

// ErrDiskPressure is returned when a write is shed because the storage volume is low on free space
// (design/36). The KV tier writes records into the same wavesdb store that backs the consensus tier, so
// it can grow the volume too; shedding here keeps a KV write burst from filling the disk. Transient —
// retryable once compaction frees space — and mapped to gRPC ResourceExhausted. Reads are never gated.
var ErrDiskPressure = errors.New("kv: disk pressure (write shed, retry)")

// DiskGate reports whether the storage volume is under disk pressure (internal/health.Monitor satisfies
// it). When set on a Coordinator, writes are shed while it reports pressure; reads are unaffected.
type DiskGate interface {
	UnderPressure() bool
}

// Cluster exposes the live roster to the coordinator (satisfied by membership.Service).
type Cluster interface {
	Members() []membership.MemberView
}

// Coordinator is the write coordinator: any pod can accept a write, persist it locally, and
// replicate to nearby durable peers, acknowledging only after origin + minAck replicas.
type Coordinator struct {
	store        *recordstore.Store
	self         membership.Member
	cluster      Cluster
	graph        *latencygraph.Graph
	replicator   local.Replicator
	policy       placement.Policy
	idem         *local.Idempotency
	holders      *local.HolderDirectory
	fanout       *local.Fanout
	onStored     func(namespace string, key []byte)
	globalTap    func(namespace string, key []byte, rec *wavespanv1.StoredRecord)
	writeTimeout time.Duration
	diskGate     DiskGate // disk-pressure admission (design/36); nil = no gating
	onShed       func()   // optional: called once per write shed for disk pressure (metrics counter)
}

// WithDiskGate installs a disk-pressure admission gate: while gate.UnderPressure() is true, KV writes
// (Put / PutTo / Delete) are shed with ErrDiskPressure before touching the store, and onShed (if non-nil)
// is called once per shed. Reads are never gated. Returns the Coordinator for chaining.
func (c *Coordinator) WithDiskGate(gate DiskGate, onShed func()) *Coordinator {
	c.diskGate = gate
	c.onShed = onShed
	return c
}

// NewCoordinator wires a coordinator. holders and fanout are optional (nil disables holder
// tracking / background target-N fill; the M4 node wires both).
func NewCoordinator(store *recordstore.Store, self membership.Member, cluster Cluster, graph *latencygraph.Graph, replicator local.Replicator, policy placement.Policy, idem *local.Idempotency, holders *local.HolderDirectory, fanout *local.Fanout, writeTimeout time.Duration) *Coordinator {
	if idem == nil {
		idem = local.NewIdempotency(0)
	}
	if writeTimeout <= 0 {
		writeTimeout = 2 * time.Second
	}
	return &Coordinator{store: store, self: self, cluster: cluster, graph: graph, replicator: replicator, policy: policy, idem: idem, holders: holders, fanout: fanout, writeTimeout: writeTimeout}
}

func (c *Coordinator) recordHolder(namespace string, key []byte, member string, v version.Version) {
	if c.holders != nil {
		c.holders.RecordHolder(namespace, key, member, v)
	}
}

// SetOnStored installs a callback invoked after the origin durable write, so the node advertises
// itself as a holder.
func (c *Coordinator) SetOnStored(fn func(namespace string, key []byte)) { c.onStored = fn }

// SetGlobalTap installs a callback invoked after the origin durable write (puts AND tombstones),
// so the node appends the mutation to each peer cluster's outbound replication log (M7). Only the
// origin coordinator taps — replica receivers do not — to avoid N× cross-cluster duplication.
func (c *Coordinator) SetGlobalTap(fn func(namespace string, key []byte, rec *wavespanv1.StoredRecord)) {
	c.globalTap = fn
}

// PutOutcome is the result of a coordinated write.
type PutOutcome struct {
	Version             version.Version
	AckedNearbyReplicas int
	GeoSpillover        bool
}

// Put coordinates an origin+1 write (design/05 "Write algorithm"). It assigns a version, writes
// the origin copy durably, replicates to nearby candidates, and returns success only once at
// least minAck nearby durable replicas acknowledged.
func (c *Coordinator) Put(ctx context.Context, namespace string, key, value []byte, ttlMs *int64, idemKey string) (PutOutcome, error) {
	return c.write(ctx, namespace, key, value, false, ttlMs, idemKey, nil)
}

// PutTo coordinates a write to an explicit candidate set instead of latency-graph placement. It is
// the affinity-placement entry point (design/29 Phase 3): vector writes target the bucket's HRW ring
// so a bucket concentrates on a deterministic node-set. Origin+1 semantics are unchanged — the origin
// is still locally durable and acks after minAck of the given candidates.
func (c *Coordinator) PutTo(ctx context.Context, namespace string, key, value []byte, candidates []placement.Candidate, idemKey string) (PutOutcome, error) {
	return c.write(ctx, namespace, key, value, false, nil, idemKey, candidates)
}

// Delete coordinates a tombstone write (design/03 "Delete path": Delete = Put(tombstone)).
func (c *Coordinator) Delete(ctx context.Context, namespace string, key []byte, idemKey string) (PutOutcome, error) {
	return c.write(ctx, namespace, key, nil, true, nil, idemKey, nil)
}

func (c *Coordinator) write(ctx context.Context, namespace string, key, value []byte, tombstone bool, ttlMs *int64, idemKey string, candidates []placement.Candidate) (PutOutcome, error) {
	if idemKey != "" {
		if v, ok := c.idem.Check(idemKey); ok {
			// An idempotent replay of an already-applied write adds no new bytes, so serve it even under
			// pressure (checked before the gate).
			return PutOutcome{Version: v, AckedNearbyReplicas: c.policy.MinAckNearbyReplicas}, nil
		}
	}
	// DISK-PRESSURE ADMISSION (design/36): shed a NEW write before it touches the store when the volume is
	// low on free space, so a KV write burst cannot fill the same wavesdb volume the consensus tier depends
	// on. Transient ResourceExhausted; reads are never gated.
	if c.diskGate != nil && c.diskGate.UnderPressure() {
		if c.onShed != nil {
			c.onShed()
		}
		return PutOutcome{}, ErrDiskPressure
	}

	v := c.store.NextVersion()
	rec := c.store.BuildRecord(namespace, key, value, v, tombstone, ttlMs)
	kind := wavespanv1.MutationKind_MUTATION_KIND_PUT
	if tombstone {
		kind = wavespanv1.MutationKind_MUTATION_KIND_DELETE
	}
	if _, err := c.store.Apply(rec, kind); err != nil { // origin local durable
		return PutOutcome{}, err
	}
	c.recordHolder(namespace, key, c.self.MemberID, v) // origin is a durable holder
	if c.onStored != nil && !tombstone {
		c.onStored(namespace, key)
	}
	if c.globalTap != nil {
		c.globalTap(namespace, key, rec) // ship to peer clusters (puts and tombstones)
	}

	cands := candidates
	if cands == nil {
		// Default path: choose nearby durable candidates by the latency graph.
		selected, err := placement.Select(c.self, c.cluster.Members(), c.graph, c.policy)
		if err != nil {
			// minAck=0 explicitly opts into local-only writes (single-node dev / degraded mode):
			// the origin copy is durable and the write is acknowledged with zero nearby replicas.
			if c.policy.MinAckNearbyReplicas <= 0 {
				if idemKey != "" {
					c.idem.Record(idemKey, v)
				}
				return PutOutcome{Version: v, AckedNearbyReplicas: 0}, nil
			}
			// no candidate can host a nearby durable replica -> origin+1 cannot be satisfied
			return PutOutcome{}, ErrInsufficientNearbyReplicas
		}
		cands = selected
	} else if len(cands) == 0 {
		// Affinity placement resolved to no off-origin candidate (e.g. the origin is the only ring
		// member, or a single-node cluster): the origin copy is durable; ack local-only.
		if idemKey != "" {
			c.idem.Record(idemKey, v)
		}
		return PutOutcome{Version: v, AckedNearbyReplicas: 0}, nil
	}

	acked, spill := c.replicateMinAck(ctx, cands, namespace, key, rec, v)
	if acked < c.policy.MinAckNearbyReplicas {
		return PutOutcome{}, ErrInsufficientNearbyReplicas
	}
	if idemKey != "" {
		c.idem.Record(idemKey, v)
	}
	// background fill to the target nearby replica count (M4).
	if c.fanout != nil {
		c.fanout.Enqueue(local.FanoutJob{Namespace: namespace, Key: key, Record: rec})
	}
	return PutOutcome{Version: v, AckedNearbyReplicas: acked, GeoSpillover: spill}, nil
}

// replicateMinAck replicates to candidates until minAck durable acknowledgements are collected.
// The background fanout worker fills the remaining target-N holders (M4).
//
// The first minAck candidates are tried CONCURRENTLY (design/37 P1.4): the old serial loop made a
// write's ack latency the SUM of candidate round-trips whenever minAck > 1, and stacked a full
// writeTimeout (2s) ahead of the fallback candidate when the preferred one was down. Placement
// order is preserved — candidate i+minAck is only tried after an earlier attempt fails.
func (c *Coordinator) replicateMinAck(ctx context.Context, cands []placement.Candidate, namespace string, key []byte, rec *wavespanv1.StoredRecord, v version.Version) (acked int, spill bool) {
	req := local.BuildRequest(namespace, key, rec, c.self.MemberID)
	need := c.policy.MinAckNearbyReplicas
	if need <= 0 {
		return 0, false
	}
	type result struct {
		ok    bool
		spill bool
	}
	// Buffered to len(cands): a goroutine finishing after an early return must never block.
	resCh := make(chan result, len(cands))
	next, inflight := 0, 0
	launch := func() {
		cand := cands[next]
		next++
		inflight++
		go func() {
			callCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
			resp, err := c.replicator.StoreReplica(callCtx, cand.Member, req)
			cancel()
			ok := err == nil && resp.GetDurable()
			if ok {
				// Recorded in the RPC goroutine so a straggler landing after we already reached
				// minAck still registers as a real holder (it durably has the record).
				c.recordHolder(namespace, key, cand.Member.MemberID, v)
			}
			resCh <- result{ok: ok, spill: ok && cand.GeoSpillover}
		}()
	}
	for inflight < need && next < len(cands) {
		launch()
	}
	for inflight > 0 {
		r := <-resCh
		inflight--
		if r.ok {
			acked++
			if r.spill {
				spill = true
			}
			if acked >= need {
				return acked, spill
			}
		} else if next < len(cands) {
			launch()
		}
	}
	return acked, spill
}
