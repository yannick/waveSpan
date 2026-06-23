package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestRangeSplitMigrates splits the initial range so one collection migrates to a new shard while the
// other stays, then verifies the directory routes each to the right shard and both keep their data
// (design/30 §6 migrate-on-split).
func TestRangeSplitMigrates(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	mgr := newMgr(t, t.TempDir(), addr, mem)
	defer mgr.Stop()
	members := map[uint64]string{1: addr}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	ctrl, err := Bootstrap(ctx, mgr, 1, members, members)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	c := ctrl.Collections()
	waitReady(t, c)

	ns := []byte("app")
	// Same-length names so route-key order follows content: "aaa" < "mmm" < "zzz".
	low, high := []byte("aaa"), []byte("zzz")
	if _, err := c.SAdd(ctx, ns, low, []byte("l1"), []byte("l2")); err != nil {
		t.Fatalf("SAdd low: %v", err)
	}
	if _, err := c.SAdd(ctx, ns, high, []byte("h1"), []byte("h2"), []byte("h3")); err != nil {
		t.Fatalf("SAdd high: %v", err)
	}
	if ctrl.Directory().ShardFor(ns, low) != firstDataShard || ctrl.Directory().ShardFor(ns, high) != firstDataShard {
		t.Fatal("both collections should start on the initial data shard")
	}

	// Split at route key "app"/"mmm": "zzz" (>= split) migrates to a new shard; "aaa" stays.
	newShard, err := ctrl.Split(ctx, routeKey(ns, []byte("mmm")), 1, members)
	if err != nil {
		t.Fatalf("Split: %v", err)
	}
	if newShard == firstDataShard {
		t.Fatal("split should allocate a new shard id")
	}

	// Routing after the split.
	if got := ctrl.Directory().ShardFor(ns, low); got != firstDataShard {
		t.Fatalf("low routes to %d, want %d (old)", got, firstDataShard)
	}
	if got := ctrl.Directory().ShardFor(ns, high); got != newShard {
		t.Fatalf("high routes to %d, want %d (new)", got, newShard)
	}

	// Data intact on both sides (high now served from the new shard, low from the old).
	if n, _ := c.SCard(ctx, ns, low, true); n != 2 {
		t.Fatalf("low SCard = %d want 2", n)
	}
	if n, _ := c.SCard(ctx, ns, high, true); n != 3 {
		t.Fatalf("high SCard = %d want 3 (migrated)", n)
	}
	if ok, _ := c.SIsMember(ctx, ns, high, []byte("h2"), true); !ok {
		t.Fatal("migrated member h2 not found on the new shard")
	}
	if got := mustMembers(t, c, ns, high); !equalMembers(got, [][]byte{[]byte("h1"), []byte("h2"), []byte("h3")}) {
		t.Fatalf("high members after split = %q want [h1 h2 h3]", got)
	}
	if got := mustMembers(t, c, ns, low); !equalMembers(got, [][]byte{[]byte("l1"), []byte("l2")}) {
		t.Fatalf("low members after split = %q want [l1 l2]", got)
	}
}
