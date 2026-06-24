package collections

import (
	"context"
	"sync"
)

// bulkRemoveConcurrency bounds how many per-collection removal proposals a single BulkRemove pipelines
// at once. Each is an independent Raft proposal, so firing them concurrently lets the shard leader
// batch them instead of paying one round-trip per collection serially.
const bulkRemoveConcurrency = 64

// BulkRemoveEntry is the per-collection result of a BulkRemove fan-out.
type BulkRemoveEntry struct {
	Collection []byte
	Removed    uint64
	Err        error
}

// ListCollections returns the collection names in a namespace, gathered across every data shard
// (best-effort: a shard this node can't read is skipped). design/30 §13.7.
func (c *Collections) ListCollections(ctx context.Context, ns []byte, linearizable bool) ([][]byte, error) {
	seen := map[string]bool{}
	var out [][]byte
	for _, shard := range c.dir.Shards() {
		v, err := c.shard.Read(ctx, shard, collectionsQuery{NS: ns}, linearizable)
		if err != nil {
			continue // best-effort across shards
		}
		for _, coll := range v.([][]byte) {
			if !seen[string(coll)] {
				seen[string(coll)] = true
				out = append(out, coll)
			}
		}
	}
	return out, nil
}

// BulkRemove removes the given members from each target collection (design/30 §13.7). When colls is
// empty, every collection in the namespace is targeted. The removal is type-agnostic — each
// collection's actual type (set/hash/zset) is honored — and best-effort: each collection's change is
// atomic on its shard, but the fan-out is eventually-consistent across shards and one collection's
// failure does not abort the others. Returns a per-collection result.
func (c *Collections) BulkRemove(ctx context.Context, ns []byte, colls, members [][]byte) ([]BulkRemoveEntry, error) {
	targets := colls
	if len(targets) == 0 {
		listed, err := c.ListCollections(ctx, ns, false)
		if err != nil {
			return nil, err
		}
		targets = listed
	}
	items := itemsFromKeys(members)
	// Pipeline the per-collection proposals: results are written by index (no shared-slice mutation),
	// and a semaphore bounds in-flight proposals to bulkRemoveConcurrency.
	out := make([]BulkRemoveEntry, len(targets))
	sem := make(chan struct{}, bulkRemoveConcurrency)
	var wg sync.WaitGroup
	for i, coll := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, coll []byte) {
			defer wg.Done()
			defer func() { <-sem }()
			removed, err := c.proposeCount(ctx, command{Op: opRemove, NS: ns, Coll: coll, Items: items})
			out[i] = BulkRemoveEntry{Collection: coll, Removed: removed, Err: err}
		}(i, coll)
	}
	wg.Wait()
	return out, nil
}
