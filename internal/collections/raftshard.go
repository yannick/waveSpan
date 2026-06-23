package collections

import "context"

// RaftShard is the boundary between the collections data plane and the Raft engine (design/30 §12.4).
// dragonboat is the only implementation today (Manager); the interface keeps etcd/raft a swappable
// fallback and lets tests substitute a fake.
type RaftShard interface {
	// StartShard starts (or restarts) a shard whose on-disk state machine applies into CFReplData.
	StartShard(shardID, replicaID uint64, initialMembers map[uint64]string, join bool) error
	// Propose commits an encoded command through the shard leader and returns the Update result value.
	Propose(ctx context.Context, shardID uint64, cmd []byte) (uint64, error)
	// Read answers a query: linearizable routes a read-index through the leader; otherwise a
	// bounded-stale local read (design/30 §5.4).
	Read(ctx context.Context, shardID uint64, query interface{}, linearizable bool) (interface{}, error)
	// Stop releases the engine (not the shared store).
	Stop()
}
