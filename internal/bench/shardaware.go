package bench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// ShardAwareClient routes each collection write directly to the data shard's current leader,
// eliminating the per-op forward hop (client -> any node -> shard leader). It is OPT-IN: the default
// bench path (CollectionsClient against a single address) is unchanged.
//
// Routing is split in two:
//   - which shard owns (ns,coll): collections.ShardForKey — the SAME pure function the server's
//     HashDirectory uses, so the client can never diverge from server placement.
//   - which core leads that shard: a routing table built from CollectionService.TierInfo, mapping
//     each shard's leaderReplicaId to a core index (replicaId R -> cores[R-1], voters numbered 1..K
//     in WAVESPAN_COLLECTIONS_VOTERS order).
//
// The table is refreshed on a short interval and lazily whenever a write errors (a leadership change
// surfaces as a not-leader/forward error). If the leader for a shard is unknown, the op falls back
// to a deterministic core so progress is still made (the server forwards as it does today).
//
// Concurrency: the benchmark drives one client from many goroutines; the routing table is guarded by
// an RWMutex (read on the hot path, write only on refresh).
type ShardAwareClient struct {
	cores      []string                             // index i -> replicaId i+1 (data addr)
	clients    []wavespanv1.CollectionServiceClient // one per core, same index
	dataShards uint64                               // N

	mu          sync.RWMutex
	shardLeader map[uint64]int // shard id -> core index (leader); absent = unknown
	lastRefresh time.Time

	refreshMin time.Duration // min gap between forced (lazy) refreshes
}

// shardAwareRefreshInterval is how often the routing table is proactively rebuilt, and the floor on
// lazy (error-triggered) refreshes so a burst of errors cannot hammer TierInfo.
const shardAwareRefreshInterval = time.Second

// NewShardAwareClient builds a shard-aware client over the ordered core data addresses (index i =
// replicaId i+1) for a hash directory of dataShards shards. It dials one gRPC client per core and
// primes the routing table once (best-effort: an unreachable cluster yields an empty table that the
// first op's lazy refresh / fallback handles). dataShards < 1 is clamped to 1.
func NewShardAwareClient(cores []string, dataShards int) (*ShardAwareClient, error) {
	if len(cores) == 0 {
		return nil, fmt.Errorf("bench: shard-aware client needs at least one core address")
	}
	if dataShards < 1 {
		dataShards = 1
	}
	clients := make([]wavespanv1.CollectionServiceClient, len(cores))
	for i, addr := range cores {
		conn, err := rpcopts.GRPCConn(addr)
		if err != nil {
			return nil, fmt.Errorf("bench: dial core %d (%s): %w", i, addr, err)
		}
		clients[i] = wavespanv1.NewCollectionServiceClient(conn)
	}
	c := &ShardAwareClient{
		cores:       append([]string(nil), cores...),
		clients:     clients,
		dataShards:  uint64(dataShards),
		shardLeader: make(map[uint64]int),
		refreshMin:  shardAwareRefreshInterval,
	}
	// Prime the table once, best-effort; the cluster may not be up yet.
	c.refresh(context.Background())
	return c, nil
}

// DataShards returns N (the hash directory width the client routes against).
func (c *ShardAwareClient) DataShards() uint64 { return c.dataShards }

// clientForKey returns the gRPC client for the leader of (ns,coll)'s shard, refreshing the routing
// table first if it is stale (older than the refresh interval). If the leader is unknown it falls
// back to a deterministic core (shard id modulo core count) so the op still makes progress.
func (c *ShardAwareClient) clientForKey(ctx context.Context, ns string, coll []byte) wavespanv1.CollectionServiceClient {
	shard := collections.ShardForKey([]byte(ns), coll, c.dataShards)

	c.mu.RLock()
	idx, known := c.shardLeader[shard]
	stale := time.Since(c.lastRefresh) >= c.refreshMin
	c.mu.RUnlock()

	if !known && stale {
		c.refresh(ctx)
		c.mu.RLock()
		idx, known = c.shardLeader[shard]
		c.mu.RUnlock()
	}
	if !known || idx < 0 || idx >= len(c.clients) {
		idx = int(shard % uint64(len(c.clients))) // deterministic fallback
	}
	return c.clients[idx]
}

// onError is called after a write fails; a not-leader/leadership change surfaces here, so it
// triggers a (rate-limited) routing-table refresh for the next op.
func (c *ShardAwareClient) onError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err // run shutting down; do not bother refreshing
	}
	c.mu.RLock()
	stale := time.Since(c.lastRefresh) >= c.refreshMin
	c.mu.RUnlock()
	if stale {
		c.refresh(ctx)
	}
	return err
}

// refresh rebuilds the routing table from TierInfo on the first reachable core. TierInfo reports
// per-shard leaderReplicaId for the shards a node hosts; a single voter (which hosts every data
// shard in the static pre-split layout) reports leaders for all of them. replicaId R maps to core
// index R-1. On total failure the existing table is left intact (we only bump lastRefresh to
// rate-limit retries).
func (c *ShardAwareClient) refresh(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var table map[uint64]int
	for _, cl := range c.clients {
		resp, err := cl.TierInfo(rctx, &wavespanv1.TierInfoRequest{})
		if err != nil || resp == nil {
			continue
		}
		t := make(map[uint64]int)
		for _, sh := range resp.GetShards() {
			if !sh.GetIsData() || !sh.GetHasLeader() {
				continue
			}
			leader := sh.GetLeaderReplicaId()
			if leader < 1 {
				continue
			}
			idx := int(leader - 1)
			if idx < 0 || idx >= len(c.clients) {
				continue // leader replicaId without a known core address; skip
			}
			t[sh.GetShardId()] = idx
		}
		if len(t) > 0 {
			table = t
			break
		}
	}

	c.mu.Lock()
	c.lastRefresh = time.Now()
	if table != nil {
		c.shardLeader = table
	}
	c.mu.Unlock()
}

// --- Op* surface: mirrors collections.go so workloads use either client interchangeably. ---

// SAdd adds members to a set, routed to the shard leader.
func (c *ShardAwareClient) SAdd(ctx context.Context, ns string, coll []byte, members ...[]byte) error {
	return c.onError(ctx, OpSAdd(ctx, c.clientForKey(ctx, ns, coll), ns, coll, members...))
}

// SRem removes members from a set, routed to the shard leader.
func (c *ShardAwareClient) SRem(ctx context.Context, ns string, coll []byte, members ...[]byte) error {
	return c.onError(ctx, OpSRem(ctx, c.clientForKey(ctx, ns, coll), ns, coll, members...))
}

// SIsMember tests set membership, routed to the shard leader.
func (c *ShardAwareClient) SIsMember(ctx context.Context, ns string, coll, member []byte) error {
	return c.onError(ctx, OpSIsMember(ctx, c.clientForKey(ctx, ns, coll), ns, coll, member))
}

// SCard returns set cardinality, routed to the shard leader.
func (c *ShardAwareClient) SCard(ctx context.Context, ns string, coll []byte) error {
	return c.onError(ctx, OpSCard(ctx, c.clientForKey(ctx, ns, coll), ns, coll))
}

// HSet sets one hash field, routed to the shard leader.
func (c *ShardAwareClient) HSet(ctx context.Context, ns string, coll, field, value []byte) error {
	return c.onError(ctx, OpHSet(ctx, c.clientForKey(ctx, ns, coll), ns, coll, field, value))
}

// HGet reads one hash field, routed to the shard leader.
func (c *ShardAwareClient) HGet(ctx context.Context, ns string, coll, field []byte) error {
	return c.onError(ctx, OpHGet(ctx, c.clientForKey(ctx, ns, coll), ns, coll, field))
}

// HIncrBy atomically increments a counter field, routed to the shard leader.
func (c *ShardAwareClient) HIncrBy(ctx context.Context, ns string, coll, field []byte, delta int64) error {
	return c.onError(ctx, OpHIncrBy(ctx, c.clientForKey(ctx, ns, coll), ns, coll, field, delta))
}

// ZAdd adds a scored member, routed to the shard leader.
func (c *ShardAwareClient) ZAdd(ctx context.Context, ns string, coll, member []byte, score float64) error {
	return c.onError(ctx, OpZAdd(ctx, c.clientForKey(ctx, ns, coll), ns, coll, member, score))
}

// ZScore reads a member's score, routed to the shard leader.
func (c *ShardAwareClient) ZScore(ctx context.Context, ns string, coll, member []byte) error {
	return c.onError(ctx, OpZScore(ctx, c.clientForKey(ctx, ns, coll), ns, coll, member))
}

// BulkRemove removes members from the given collections. The collections may span multiple shards,
// so a single leader cannot serve them all; this routes to the leader of the FIRST collection (or a
// fallback core) and relies on server-side cross-shard fan-out for the rest. Returns the number of
// collections touched and the total removed (mirrors OpBulkRemove).
func (c *ShardAwareClient) BulkRemove(ctx context.Context, ns string, colls, members [][]byte) (count int, removed uint64, err error) {
	var cl wavespanv1.CollectionServiceClient
	if len(colls) > 0 {
		cl = c.clientForKey(ctx, ns, colls[0])
	} else {
		cl = c.clients[0]
	}
	count, removed, err = OpBulkRemove(ctx, cl, ns, colls, members)
	return count, removed, c.onError(ctx, err)
}
