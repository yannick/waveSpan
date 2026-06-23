package collections

import (
	"context"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/storage"
)

// TestControlBootstrapAndRoute brings up the control plane (meta shard + range directory + initial
// data shard via the minimal placement driver) and confirms a collection routes through the
// meta-backed directory to the data shard and that ops work end to end (design/30 §7).
func TestControlBootstrapAndRoute(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	mgr := newMgr(t, t.TempDir(), addr, mem)
	defer mgr.Stop()
	members := map[uint64]string{1: addr}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctrl, err := Bootstrap(ctx, mgr, 1, members, members)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	ns, coll := []byte("flags"), []byte("enabled")
	// The directory (loaded from the meta shard) routes the collection to the initial data shard.
	if sid := ctrl.Directory().ShardFor(ns, coll); sid != firstDataShard {
		t.Fatalf("ShardFor = %d want %d (directory not loaded from meta?)", sid, firstDataShard)
	}

	c := ctrl.Collections()
	waitReady(t, c) // data shard leader

	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	if n, err := c.SAdd(rctx, ns, coll, []byte("x"), []byte("y")); err != nil || n != 2 {
		t.Fatalf("SAdd via directory = %d,%v want 2", n, err)
	}
	if ok, _ := c.SIsMember(rctx, ns, coll, []byte("x"), true); !ok {
		t.Fatal("member x not found after routed SAdd")
	}
	if n, _ := c.SCard(rctx, ns, coll, true); n != 2 {
		t.Fatalf("SCard = %d want 2", n)
	}
}
