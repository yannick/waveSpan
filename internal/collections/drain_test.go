package collections

import (
	"context"
	"errors"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"

	"github.com/yannick/wavespan/internal/storage"
)

// TestGracefulDrainLeader drains the node that currently leads a 3-voter shard: it transfers
// leadership to a peer, a remaining member removes the drained replica, and the node stops it. The
// shard keeps quorum (2 voters) and still accepts writes; the drained node no longer hosts the shard
// (design/30 §7 graceful drain).
func TestGracefulDrainLeader(t *testing.T) {
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
	if got := clusterSAdd(t, cols, ns, coll, []byte("a"), []byte("b")); got != 2 {
		t.Fatalf("initial SAdd = %d want 2", got)
	}

	leader := leaderID(t, mgrs[1], 1)
	var target uint64
	for i := uint64(1); i <= n; i++ {
		if i != leader {
			target = i
			break
		}
	}

	// 1. transfer leadership off the drained node.
	if err := mgrs[leader].TransferLeadership(1, target); err != nil {
		t.Fatalf("TransferLeadership: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		id, _, ok, err := mgrs[target].nh.GetLeaderID(1)
		if err == nil && ok && id != leader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leadership never moved off the drained node")
		}
		time.Sleep(150 * time.Millisecond)
	}

	// 2. a remaining member removes the drained replica; 3. the drained node stops it locally.
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := mgrs[target].RemoveLearner(rctx, 1, leader); err != nil {
		rcancel()
		t.Fatalf("remove drained replica: %v", err)
	}
	rcancel()
	_ = mgrs[leader].StopLocalReplica(1, leader)

	// the drained node no longer hosts the shard.
	notHosted := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := mgrs[leader].Read(ctx, 1, cardQuery{NS: ns, Coll: coll}, false)
		cancel()
		if errors.Is(err, dragonboat.ErrShardNotFound) {
			break
		}
		if time.Now().After(notHosted) {
			t.Fatalf("drained node still hosts the shard (err=%v)", err)
		}
		time.Sleep(150 * time.Millisecond)
	}

	// the surviving 2-voter group still accepts writes.
	remaining := map[uint64]*Collections{}
	for i := uint64(1); i <= n; i++ {
		if i != leader {
			remaining[i] = cols[i]
		}
	}
	if got := clusterSAdd(t, remaining, ns, coll, []byte("c")); got != 1 {
		t.Fatalf("post-drain SAdd = %d want 1 (quorum lost?)", got)
	}
}
