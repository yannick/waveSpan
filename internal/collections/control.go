package collections

import (
	"bytes"
	"context"
	"errors"
	"time"
)

const ingestBatch = 256 // rawKV pairs per opIngest proposal during a split migration

// Reserved shard ids for the consensus tier. The meta shard holds the range directory; data shards
// are assigned from firstDataShard upward by the placement driver.
const (
	MetaShardID    uint64 = 1
	firstDataShard uint64 = 2
)

// Control bootstraps and owns the consensus tier on one node: the meta shard (range directory), the
// data shard(s), the cached RangeDirectory, and the typed Collections API. This is the M-B control-
// plane foundation — a single data range; range split/merge, learner demand-fill, and a multi-node
// placement driver are later milestones.
type Control struct {
	mgr  *Manager
	dir  *RangeDirectory
	cols *Collections
}

// Bootstrap starts the meta shard and the initial data shard on this node, ensures the directory has
// a full initial range, and returns a ready Control. metaMembers and dataMembers map ReplicaID ->
// RaftAddress for each group (identical sets when single-node).
func Bootstrap(ctx context.Context, mgr *Manager, replicaID uint64, metaMembers, dataMembers map[uint64]string) (*Control, error) {
	if err := mgr.StartMetaShard(MetaShardID, replicaID, metaMembers, false); err != nil {
		return nil, err
	}
	dir := NewRangeDirectory(mgr, MetaShardID)

	// Minimal placement driver: ensure the initial full range [-inf,+inf) -> firstDataShard.
	if err := ensureInitialRange(ctx, mgr, dir); err != nil {
		return nil, err
	}
	if err := mgr.StartShard(firstDataShard, replicaID, dataMembers, false); err != nil {
		return nil, err
	}
	if err := refreshWithRetry(ctx, dir); err != nil {
		return nil, err
	}
	return &Control{mgr: mgr, dir: dir, cols: New(mgr, dir)}, nil
}

// Collections returns the typed datatype API routed through the range directory.
func (c *Control) Collections() *Collections { return c.cols }

// Split divides the range covering splitKey into [oldStart, splitKey) on the existing shard and
// [splitKey, oldEnd) on a new shard, migrating the subrange's data (design/30 §6, ADR 0008): because
// dragonboat shards are independent, this starts a new shard, copies the subrange in, cuts the
// directory over, and purges the subrange from the old shard. Returns the new shard id.
//
// v1 assumes the splitting subrange is quiescent during the migration (no concurrent writes to it);
// a freeze/cutover is a follow-up (design/30 §6.2).
func (c *Control) Split(ctx context.Context, splitKey []byte, replicaID uint64, newMembers map[uint64]string) (uint64, error) {
	if len(splitKey) == 0 {
		return 0, errors.New("collections: empty split key")
	}
	if err := c.dir.Refresh(ctx); err != nil {
		return 0, err
	}
	old, ok := c.dir.rangeContaining(splitKey)
	if !ok {
		return 0, errors.New("collections: no range contains the split key")
	}
	if bytes.Equal(old.Start, splitKey) {
		return 0, errors.New("collections: split key equals the range start (no-op)")
	}
	newShard := c.dir.maxShardID() + 1

	// 1. start the new (empty) shard and wait for its leader.
	if err := c.mgr.StartShard(newShard, replicaID, newMembers, false); err != nil {
		return 0, err
	}
	if err := waitLeader(ctx, c.mgr, newShard); err != nil {
		return 0, err
	}

	// 2. read the subrange [splitKey, old.End) from the old shard.
	v, err := c.mgr.Read(ctx, old.ShardID, migrateScanQuery{StartRoute: splitKey, EndRoute: old.End}, true)
	if err != nil {
		return 0, err
	}
	kvs, _ := v.([]rawKV)

	// 3. ingest the subrange into the new shard (batched).
	for off := 0; off < len(kvs); off += ingestBatch {
		end := off + ingestBatch
		if end > len(kvs) {
			end = len(kvs)
		}
		if _, err := c.mgr.Propose(ctx, newShard, encodeIngest(kvs[off:end])); err != nil {
			return 0, err
		}
	}

	// 4. cut the directory over: shrink the old range, add the new range.
	if _, err := c.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opMetaPut, Start: old.Start, End: splitKey, ShardID: old.ShardID})); err != nil {
		return 0, err
	}
	if _, err := c.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opMetaPut, Start: splitKey, End: old.End, ShardID: newShard})); err != nil {
		return 0, err
	}

	// 5. purge the migrated subrange from the old shard, then refresh the directory.
	if _, err := c.mgr.Propose(ctx, old.ShardID, encodePurge(splitKey, old.End)); err != nil {
		return 0, err
	}
	return newShard, c.dir.Refresh(ctx)
}

func waitLeader(ctx context.Context, mgr *Manager, shardID uint64) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		if mgr.hasLeader(shardID) {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Directory returns the cached range directory (for refresh / inspection).
func (c *Control) Directory() *RangeDirectory { return c.dir }

// ensureInitialRange waits for the meta shard's leader, then upserts the initial range if absent.
func ensureInitialRange(ctx context.Context, mgr *Manager, dir *RangeDirectory) error {
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := dir.Refresh(ctx); err == nil {
			dir.mu.RLock()
			has := len(dir.ranges) > 0
			dir.mu.RUnlock()
			if has {
				return nil
			}
			cmd := encodeMetaCommand(metaCommand{Op: opMetaPut, Start: nil, End: nil, ShardID: firstDataShard})
			pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, perr := mgr.Propose(pctx, MetaShardID, cmd)
			cancel()
			if perr == nil {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func refreshWithRetry(ctx context.Context, dir *RangeDirectory) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := dir.Refresh(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		time.Sleep(100 * time.Millisecond)
	}
}
