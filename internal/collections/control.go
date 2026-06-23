package collections

import (
	"context"
	"time"
)

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
