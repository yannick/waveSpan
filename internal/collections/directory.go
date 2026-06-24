package collections

import "context"

// Directory maps a (namespace, collection) to the Raft shard that owns it (design/30 §7). M-A uses a
// single range — every collection lives in one shard. The meta Raft group + multi-range directory
// (with split/merge) is a later milestone; this interface is the seam for it.
type Directory interface {
	ShardFor(ns, coll []byte) uint64
	// Shards returns every data shard the directory routes to, for cross-shard enumeration (design/30 §13.7).
	Shards() []uint64
	// Refresh reloads the routing table from the meta shard (self-heal a stale/missed local directory).
	Refresh(ctx context.Context) error
}

type singleShard struct{ id uint64 }

// SingleShardDirectory routes every collection to one shard id (M-A single-range mode).
func SingleShardDirectory(id uint64) Directory { return singleShard{id: id} }

func (s singleShard) ShardFor(_, _ []byte) uint64        { return s.id }
func (s singleShard) Shards() []uint64                   { return []uint64{s.id} }
func (s singleShard) Refresh(_ context.Context) error    { return nil }
