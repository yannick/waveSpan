// Package kv implements the public KV API: the origin+1 write coordinator, the local read path,
// and the Connect service (design/03_kv_store.md, design/05 write algorithm).
package kv

import (
	"context"
	"errors"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/placement"
	"github.com/cwire/wavespan/internal/recordstore"
	local "github.com/cwire/wavespan/internal/replication/local"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// ErrInsufficientNearbyReplicas is returned when origin+1 cannot be satisfied: no nearby durable
// replica acknowledged the write (design/05; ADR 0002). The origin copy is durable, but the write
// is not acknowledged.
var ErrInsufficientNearbyReplicas = errors.New("kv: insufficient nearby durable replicas for origin+1")

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
	writeTimeout time.Duration
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
	return c.write(ctx, namespace, key, value, false, ttlMs, idemKey)
}

// Delete coordinates a tombstone write (design/03 "Delete path": Delete = Put(tombstone)).
func (c *Coordinator) Delete(ctx context.Context, namespace string, key []byte, idemKey string) (PutOutcome, error) {
	return c.write(ctx, namespace, key, nil, true, nil, idemKey)
}

func (c *Coordinator) write(ctx context.Context, namespace string, key, value []byte, tombstone bool, ttlMs *int64, idemKey string) (PutOutcome, error) {
	if idemKey != "" {
		if v, ok := c.idem.Check(idemKey); ok {
			return PutOutcome{Version: v, AckedNearbyReplicas: c.policy.MinAckNearbyReplicas}, nil
		}
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

	cands, err := placement.Select(c.self, c.cluster.Members(), c.graph, c.policy)
	if err != nil {
		// no candidate can host a nearby durable replica -> origin+1 cannot be satisfied
		return PutOutcome{}, ErrInsufficientNearbyReplicas
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

// replicateMinAck replicates to candidates in order until minAck durable acknowledgements are
// collected. The background fanout worker fills the remaining target-N holders (M4).
func (c *Coordinator) replicateMinAck(ctx context.Context, cands []placement.Candidate, namespace string, key []byte, rec *wavespanv1.StoredRecord, v version.Version) (acked int, spill bool) {
	req := local.BuildRequest(namespace, key, rec, c.self.MemberID)
	for _, cand := range cands {
		callCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
		resp, err := c.replicator.StoreReplica(callCtx, cand.Member, req)
		cancel()
		if err == nil && resp.GetDurable() {
			acked++
			c.recordHolder(namespace, key, cand.Member.MemberID, v)
			if cand.GeoSpillover {
				spill = true
			}
			if acked >= c.policy.MinAckNearbyReplicas {
				return acked, spill
			}
		}
	}
	return acked, spill
}
