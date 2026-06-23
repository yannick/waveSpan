package collections

import (
	"context"
	"errors"
	"sync"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

// ErrNotHosted is returned by a read when this node does not host the shard (dragonboat's
// ErrShardNotFound); it is the demand-fill trigger.
var ErrNotHosted = dragonboat.ErrShardNotFound

// LearnerAdmitter asks a current member of a shard to admit this node as a non-voting learner. In
// production it is an RPC to a peer (its CollectionService/admin); in tests it can call the peer's
// Manager.AddLearner directly. The seam keeps the demand-fill orchestration transport-agnostic.
type LearnerAdmitter interface {
	AdmitLearner(ctx context.Context, shardID, learnerReplicaID uint64, learnerTarget string) error
}

// DemandFiller turns a not-hosted read into a learner join (design/30 §9): it asks a member to admit
// this node, starts the local non-voting replica, and waits until it can serve. Joins are deduplicated
// per shard and idempotent (a shard already hosted is a no-op).
type DemandFiller struct {
	mgr           *Manager
	selfReplicaID uint64
	selfTarget    string
	admitter      LearnerAdmitter

	mu      sync.Mutex
	filling map[uint64]chan struct{} // shardID -> done channel for an in-flight fill
}

// NewDemandFiller builds a filler that joins shards as selfReplicaID reachable at selfTarget, asking
// admitter to admit it to each shard.
func NewDemandFiller(mgr *Manager, selfReplicaID uint64, selfTarget string, admitter LearnerAdmitter) *DemandFiller {
	return &DemandFiller{mgr: mgr, selfReplicaID: selfReplicaID, selfTarget: selfTarget, admitter: admitter, filling: map[uint64]chan struct{}{}}
}

// hosted reports whether this node already hosts the shard (a probe read succeeds).
func (d *DemandFiller) hosted(ctx context.Context, shardID uint64) bool {
	_, err := d.mgr.Read(ctx, shardID, cardQuery{}, false)
	return !errors.Is(err, ErrNotHosted)
}

// Fill ensures this node hosts shardID by joining it as a learner. Concurrent calls for the same shard
// collapse onto one join.
func (d *DemandFiller) Fill(ctx context.Context, shardID uint64) error {
	if d.hosted(ctx, shardID) {
		return nil
	}
	d.mu.Lock()
	if ch, ok := d.filling[shardID]; ok {
		d.mu.Unlock()
		select { // someone else is filling — wait for them
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	ch := make(chan struct{})
	d.filling[shardID] = ch
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.filling, shardID)
		d.mu.Unlock()
		close(ch)
	}()

	if d.hosted(ctx, shardID) { // re-check under the in-flight guard
		return nil
	}
	if err := d.admitter.AdmitLearner(ctx, shardID, d.selfReplicaID, d.selfTarget); err != nil {
		return err
	}
	if err := d.mgr.StartLearner(shardID, d.selfReplicaID); err != nil {
		return err
	}
	return d.waitHosted(ctx, shardID)
}

// waitHosted blocks until the local learner replica is up enough to answer reads (it then catches up
// asynchronously; reads are bounded-stale until then).
func (d *DemandFiller) waitHosted(ctx context.Context, shardID uint64) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		if d.hosted(ctx, shardID) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("wavespan: demand-fill learner did not come up in time")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// TransferLeadership asks the shard to move leadership to targetReplicaID — used when gracefully

// Learner demand-fill (design/30 §9, M-C): a node asked for a collection whose owning shard it does
// not host can join that shard as a non-voting learner, stream its state, then serve bounded-stale
// reads locally — a dynamically-filling read cache. Voters stay on the stable core; learners live
// anywhere and are evicted when cold (unless pinned).
//
// The cross-node trigger — detect ErrShardNotFound on a read, ask a current member to admit this node,
// start the learner locally, then retry locally — needs the node RPC/transport layer (a later
// milestone). These are the engine primitives that trigger drives: AddLearner / RemoveLearner run on a
// node that already hosts the shard (routed to the leader); StartLearner / StopLocalReplica run on the
// joining/leaving node itself.

// AddLearner asks the shard (via its leader) to admit replicaID at addr as a non-voting learner.
// Call on a node that already hosts the shard.
func (m *Manager) AddLearner(ctx context.Context, shardID, replicaID uint64, addr string) error {
	return m.nh.SyncRequestAddNonVoting(ctx, shardID, replicaID, addr, 0)
}

// StartLearner starts a local non-voting replica that joins an existing shard and streams its state.
// Call on the joining node after AddLearner has committed.
func (m *Manager) StartLearner(shardID, replicaID uint64) error {
	factory := func(sid, _ uint64) sm.IOnDiskStateMachine { return newShardSM(m.store, sid) }
	return m.startReplica(shardID, replicaID, nil, true /*join*/, factory, true, true /*nonVoting*/)
}

// RemoveLearner asks the shard (via its leader) to drop replicaID from membership (eviction). Call on
// a node that hosts the shard.
func (m *Manager) RemoveLearner(ctx context.Context, shardID, replicaID uint64) error {
	return m.nh.SyncRequestDeleteReplica(ctx, shardID, replicaID, 0)
}

// TransferLeadership asks the shard to move leadership to targetReplicaID — used when gracefully
// draining a node that currently leads a shard, so a peer leads before this node's replica is removed.
// The transfer completes asynchronously; this returns dragonboat's immediate acceptance error.
func (m *Manager) TransferLeadership(shardID, targetReplicaID uint64) error {
	return m.nh.RequestLeaderTransfer(shardID, targetReplicaID)
}

// StopLocalReplica stops and deregisters a locally hosted replica (after eviction, or on drain).
func (m *Manager) StopLocalReplica(shardID, replicaID uint64) error {
	err := m.nh.StopReplica(shardID, replicaID)
	m.mu.Lock()
	delete(m.shards, shardID)
	m.mu.Unlock()
	return err
}
