package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestMultiNodeBootstrap bootstraps the full control plane (meta shard + initial data shard) across a
// 3-voter stable core: every voter calls Bootstrap with the same voter set. The meta group and data
// shard each form a 3-voter quorum, the directory routes collections, and a write replicates
// (design/30 §7, §18). The voter set is the placement decision — these are the stable-core nodes; spot
// nodes join later as learners via demand-fill.
func TestMultiNodeBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node raft test")
	}
	const n = 3
	voters := map[uint64]string{}
	mgrs := map[uint64]*Manager{}
	for i := uint64(1); i <= n; i++ {
		addr := freeAddr(t)
		voters[i] = addr
		store := storage.NewMemStore()
		t.Cleanup(func() { _ = store.Close() })
		mgrs[i] = newMgr(t, t.TempDir(), addr, store)
	}
	defer func() {
		for _, m := range mgrs {
			m.Stop()
		}
	}()

	// Every stable-core voter bootstraps with the same voter set (meta + data are 3-voter groups).
	ctrls := map[uint64]*Control{}
	done := make(chan struct{}, n)
	errs := make(chan error, n)
	for i := uint64(1); i <= n; i++ {
		i := i
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			ctrl, err := Bootstrap(ctx, mgrs[i], i, voters, voters)
			if err != nil {
				errs <- err
				return
			}
			ctrls[i] = ctrl
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case err := <-errs:
			t.Fatalf("Bootstrap: %v", err)
		case <-time.After(45 * time.Second):
			t.Fatal("Bootstrap timed out")
		}
	}

	ns, coll := []byte("flags"), []byte("enabled")
	cols := map[uint64]*Collections{}
	for i := uint64(1); i <= n; i++ {
		cols[i] = ctrls[i].Collections()
	}
	// All voters route the collection to the same data shard.
	if sid := ctrls[1].Directory().ShardFor(ns, coll); sid != firstDataShard {
		t.Fatalf("ShardFor = %d want %d", sid, firstDataShard)
	}
	// A write commits at 3-voter quorum and replicates.
	if got := clusterSAdd(t, cols, ns, coll, []byte("x"), []byte("y")); got != 2 {
		t.Fatalf("cluster SAdd = %d want 2", got)
	}
	for i := uint64(1); i <= n; i++ {
		if !awaitMember(t, cols[i], ns, coll, []byte("x")) {
			t.Fatalf("member not replicated to node %d", i)
		}
	}
}
