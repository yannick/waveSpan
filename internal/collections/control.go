package collections

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
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

// Placement describes a node's role in the consensus tier (design/30 §4, §7). Voter-eligible nodes are
// the stable core — e.g. annotated in Kubernetes — that hold voting replicas of the meta and data
// shards; non-eligible (spot/edge) nodes hold no replicas until they demand-fill a collection as a
// learner. Voters is the stable-core voter set (replicaID -> NodeHostID or address) the meta and data
// shards bootstrap with.
type Placement struct {
	SelfReplicaID uint64
	VoterEligible bool
	Voters        map[uint64]string
}

// BootstrapWithPlacement brings up the tier per p: a voter-eligible node bootstraps the meta + initial
// data shard with the stable-core voter set; a non-eligible node holds no replicas and returns
// (nil, nil) — it serves collections by demand-filling them as a learner on read (design/30 §9).
func BootstrapWithPlacement(ctx context.Context, mgr *Manager, p Placement) (*Control, error) {
	if !p.VoterEligible {
		return nil, nil
	}
	return Bootstrap(ctx, mgr, p.SelfReplicaID, p.Voters, p.Voters)
}

// SpotReplicaID derives a stable, high, per-node replica id from a node identity (e.g. its member id),
// kept well above the small stable-core voter ids so a spot node never collides with the core and
// rejoins as the same replica across restarts.
func SpotReplicaID(nodeID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	return (uint64(1) << 40) | (h.Sum64() & ((uint64(1) << 40) - 1))
}

// JoinAsSpot brings up a non-voting spot/edge node that holds no shards initially: it demand-fills the
// meta shard to obtain the range directory, then serves collections by demand-filling their data
// shards on read (design/30 §9). selfReplicaID must be stable + unique (see SpotReplicaID); admitter
// asks a stable-core peer to admit this node.
func JoinAsSpot(ctx context.Context, mgr *Manager, selfReplicaID uint64, selfTarget string, admitter LearnerAdmitter) (*Control, error) {
	dir := NewRangeDirectory(mgr, MetaShardID)
	if err := EnsureSpotMembership(ctx, mgr, selfReplicaID, selfTarget, admitter, dir); err != nil {
		return nil, err
	}
	cols := New(mgr, dir).WithDemandFill(NewDemandFiller(mgr, selfReplicaID, selfTarget, admitter))
	return &Control{mgr: mgr, dir: dir, cols: cols}, nil
}

// EnsureSpotMembership joins the meta shard as a learner and blocks until dir is populated, so a spot
// node obtains the range directory. The admit step is retried until a stable-core peer is reachable —
// run this in the background after mounting the service so node startup is not blocked on the core.
func EnsureSpotMembership(ctx context.Context, mgr *Manager, selfReplicaID uint64, selfTarget string, admitter LearnerAdmitter, dir *RangeDirectory) error {
	// 1. ask a peer to admit us to the meta shard, retrying until one accepts.
	for {
		if err := admitter.AdmitLearner(ctx, MetaShardID, selfReplicaID, selfTarget); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	// 2. start the local meta learner.
	if err := mgr.StartMetaLearner(MetaShardID, selfReplicaID); err != nil {
		return err
	}
	// 3. wait until the meta learner serves the directory locally.
	for {
		if err := dir.Refresh(ctx); err == nil {
			dir.mu.RLock()
			has := len(dir.ranges) > 0
			dir.mu.RUnlock()
			if has {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
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
	if err := c.ingestInto(ctx, newShard, kvs); err != nil {
		return 0, err
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

// Merge absorbs the range starting at boundary into its left neighbour (whose end == boundary),
// migrating the right range's data into the left shard and retiring the right range from the directory
// (design/30 §6.2). Mirror of Split; same quiescent-subrange assumption for v1. The emptied shard is
// left running but unreferenced — clean replica teardown is a follow-up.
func (c *Control) Merge(ctx context.Context, boundary []byte) error {
	if len(boundary) == 0 {
		return errors.New("collections: empty merge boundary")
	}
	if err := c.dir.Refresh(ctx); err != nil {
		return err
	}
	var left, right rangeEntry
	var lok, rok bool
	for _, r := range c.dir.all() {
		if len(r.End) > 0 && bytes.Equal(r.End, boundary) {
			left, lok = r, true
		}
		if bytes.Equal(r.Start, boundary) {
			right, rok = r, true
		}
	}
	if !lok || !rok {
		return errors.New("collections: no adjacent ranges at the merge boundary")
	}
	if left.ShardID == right.ShardID {
		return errors.New("collections: ranges already share a shard")
	}

	// 1. read the right range's data and 2. ingest it into the left shard.
	v, err := c.mgr.Read(ctx, right.ShardID, migrateScanQuery{StartRoute: right.Start, EndRoute: right.End}, true)
	if err != nil {
		return err
	}
	kvs, _ := v.([]rawKV)
	if err := c.ingestInto(ctx, left.ShardID, kvs); err != nil {
		return err
	}

	// 3. extend the left range over the right, then drop the right range from the directory.
	if _, err := c.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opMetaPut, Start: left.Start, End: right.End, ShardID: left.ShardID})); err != nil {
		return err
	}
	if _, err := c.mgr.Propose(ctx, MetaShardID, encodeMetaCommand(metaCommand{Op: opMetaDelete, Start: right.Start})); err != nil {
		return err
	}

	// 4. purge the now-unreferenced right shard's data, then refresh.
	if _, err := c.mgr.Propose(ctx, right.ShardID, encodePurge(right.Start, right.End)); err != nil {
		return err
	}
	return c.dir.Refresh(ctx)
}

// ingestInto writes rawKV pairs into a shard in opIngest batches.
func (c *Control) ingestInto(ctx context.Context, shard uint64, kvs []rawKV) error {
	for off := 0; off < len(kvs); off += ingestBatch {
		end := off + ingestBatch
		if end > len(kvs) {
			end = len(kvs)
		}
		if _, err := c.mgr.Propose(ctx, shard, encodeIngest(kvs[off:end])); err != nil {
			return err
		}
	}
	return nil
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
