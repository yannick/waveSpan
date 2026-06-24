package collections

import (
	"context"
	"hash/fnv"
)

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

func (s singleShard) ShardFor(_, _ []byte) uint64     { return s.id }
func (s singleShard) Shards() []uint64                { return []uint64{s.id} }
func (s singleShard) Refresh(_ context.Context) error { return nil }

// HashDirectory statically pre-splits the collection keyspace into N hash-routed data shards (D1,
// design/32 §3.4): a collection routes to firstDataShard + (hash(routeKey) mod N). The mapping is
// deterministic and depends only on N, so every node agrees on routing without consulting the meta
// shard — no range metadata, no hotspots, even spread. Cross-shard enumeration (ListCollections /
// BulkRemove) iterates Shards(). Trades range-scan locality, which collections do not use (they are
// addressed by exact (ns,coll)). The N shards must be bootstrapped with ids [firstDataShard,
// firstDataShard+N) and the same voter set.
type HashDirectory struct {
	n      uint64
	shards []uint64
}

var _ Directory = (*HashDirectory)(nil)

// NewHashDirectory builds a hash directory over n data shards (n>=1) starting at firstDataShard.
func NewHashDirectory(n uint64) *HashDirectory {
	if n < 1 {
		n = 1
	}
	shards := make([]uint64, n)
	for i := uint64(0); i < n; i++ {
		shards[i] = firstDataShard + i
	}
	return &HashDirectory{n: n, shards: shards}
}

// FirstDataShard is the lowest data shard id (data shard ids run [FirstDataShard, FirstDataShard+N)).
// Exported so a shard-aware client can interpret ShardForKey's result without package internals.
const FirstDataShard = firstDataShard

// ShardForKey is the canonical hash-routing function shared by server and client: a collection
// routes to FirstDataShard + (fnv64a(routeKey(ns,coll)) mod dataShards). Exporting it as a pure
// function lets a shard-aware client compute the owning shard identically to HashDirectory, so the
// two can never diverge. dataShards < 1 is clamped to 1.
func ShardForKey(ns, coll []byte, dataShards uint64) uint64 {
	if dataShards < 1 {
		dataShards = 1
	}
	hsh := fnv.New64a()
	_, _ = hsh.Write(routeKey(ns, coll))
	return firstDataShard + (hsh.Sum64() % dataShards)
}

// ShardFor routes a collection to one of the N data shards by hashing its route key.
func (h *HashDirectory) ShardFor(ns, coll []byte) uint64 {
	return ShardForKey(ns, coll, h.n)
}

// Shards returns every data shard id (for cross-shard enumeration, §13.7).
func (h *HashDirectory) Shards() []uint64 { return append([]uint64(nil), h.shards...) }

// Refresh is a no-op: hash routing is purely local and needs no meta-shard reload.
func (h *HashDirectory) Refresh(_ context.Context) error { return nil }

// DataShardCount returns N.
func (h *HashDirectory) DataShardCount() uint64 { return h.n }
