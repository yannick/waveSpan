package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestRangeMergeAbsorbs splits the initial range in two, then merges it back, verifying the migrated
// collection returns to the left shard with intact data and the directory collapses to one range
// (design/30 §6.2 merge).
func TestRangeMergeAbsorbs(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	mgr := newMgr(t, t.TempDir(), addr, mem)
	defer mgr.Stop()
	members := map[uint64]string{1: addr}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	ctrl, err := Bootstrap(ctx, mgr, 1, members, members)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c := ctrl.Collections()
	waitReady(t, c)

	ns := []byte("app")
	low, high := []byte("aaa"), []byte("zzz")
	if _, err := c.SAdd(ctx, ns, low, []byte("l1"), []byte("l2")); err != nil {
		t.Fatalf("SAdd low: %v", err)
	}
	if _, err := c.SAdd(ctx, ns, high, []byte("h1"), []byte("h2"), []byte("h3")); err != nil {
		t.Fatalf("SAdd high: %v", err)
	}

	boundary := routeKey(ns, []byte("mmm"))
	newShard, err := ctrl.Split(ctx, boundary, 1, members)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if ctrl.Directory().ShardFor(ns, high) != newShard {
		t.Fatal("high should be on the new shard after split")
	}

	// Merge the boundary back: the new shard's range is absorbed into the original.
	if err := ctrl.Merge(ctx, boundary); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Both collections route to the original shard again, one collapsed range.
	if got := ctrl.Directory().ShardFor(ns, low); got != firstDataShard {
		t.Fatalf("low routes to %d, want %d", got, firstDataShard)
	}
	if got := ctrl.Directory().ShardFor(ns, high); got != firstDataShard {
		t.Fatalf("high routes to %d, want %d (merged back)", got, firstDataShard)
	}
	if ranges := ctrl.Directory().all(); len(ranges) != 1 {
		t.Fatalf("directory has %d ranges after merge, want 1", len(ranges))
	}

	// Data intact, now both served from the original shard.
	if n, _ := c.SCard(ctx, ns, high, true); n != 3 {
		t.Fatalf("high SCard after merge = %d want 3", n)
	}
	if got := mustMembers(t, c, ns, high); !equalMembers(got, [][]byte{[]byte("h1"), []byte("h2"), []byte("h3")}) {
		t.Fatalf("high members after merge = %q want [h1 h2 h3]", got)
	}
	if got := mustMembers(t, c, ns, low); !equalMembers(got, [][]byte{[]byte("l1"), []byte("l2")}) {
		t.Fatalf("low members after merge = %q want [l1 l2]", got)
	}
}
