package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestThreeNodeQuorum brings up a real 3-voter shard (dragonboat's built-in transport over localhost),
// commits through the leader, confirms the write replicates to the followers' local state (bounded-
// stale reads), and that the shard keeps serving writes after one follower is stopped (2/3 quorum).
func TestThreeNodeQuorum(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	const n = 3
	members := map[uint64]string{}
	mgrs := map[uint64]*Manager{}
	cols := map[uint64]*Collections{}
	for i := uint64(1); i <= n; i++ {
		addr := freeAddr(t)
		members[i] = addr
		store := storage.NewMemStore()
		t.Cleanup(func() { _ = store.Close() })
		mgrs[i] = newMgr(t, t.TempDir(), addr, store)
	}
	for i := uint64(1); i <= n; i++ {
		if err := mgrs[i].StartShard(1, i, members, false); err != nil {
			t.Fatalf("StartShard %d: %v", i, err)
		}
		cols[i] = New(mgrs[i], SingleShardDirectory(1))
	}
	defer func() {
		for _, m := range mgrs {
			m.Stop()
		}
	}()

	ns, coll := []byte("flags"), []byte("enabled")

	// Commit through whichever node is leader (the client's leader-routing, design/30 §13.13).
	if n := clusterSAdd(t, cols, ns, coll, []byte("a"), []byte("b")); n != 2 {
		t.Fatalf("SAdd via leader = %d want 2", n)
	}

	// The write must reach every follower's local applied state (bounded-stale read on each node).
	for id, c := range cols {
		if got := awaitMember(t, c, ns, coll, []byte("a")); !got {
			t.Fatalf("node %d never observed member a", id)
		}
	}

	// Stop one follower (a node that is not currently the leader); the shard keeps a 2/3 quorum.
	leader := leaderID(t, mgrs[1], 1)
	var victim uint64
	for i := uint64(1); i <= n; i++ {
		if i != leader {
			victim = i
			break
		}
	}
	mgrs[victim].Stop()
	delete(mgrs, victim)
	delete(cols, victim)

	if n := clusterSAdd(t, cols, ns, coll, []byte("c")); n != 1 {
		t.Fatalf("SAdd after follower down = %d want 1", n)
	}
	// a surviving node sees the new member
	for _, c := range cols {
		if got := awaitMember(t, c, ns, coll, []byte("c")); !got {
			t.Fatal("surviving node never observed member c after follower loss")
		}
	}
}

func clusterSAdd(t *testing.T, nodes map[uint64]*Collections, ns, coll []byte, members ...[]byte) uint64 {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, c := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			got, err := c.SAdd(ctx, ns, coll, members...)
			cancel()
			if err == nil {
				return got
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no node accepted the proposal (no stable leader?)")
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func awaitMember(t *testing.T, c *Collections, ns, coll, member []byte) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		got, err := c.SIsMember(ctx, ns, coll, member, false) // bounded-stale local read
		cancel()
		if err == nil && got {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func leaderID(t *testing.T, m *Manager, shardID uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if id, _, ok, err := m.nh.GetLeaderID(shardID); err == nil && ok {
			return id
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader elected")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
