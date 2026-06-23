package collections

import (
	"context"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

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
