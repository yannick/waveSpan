package collections

import (
	"context"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/storage"
)

// TestSetTTLExpiry checks that a member added with a TTL is present immediately and is removed by the
// leader sweeper after it expires (log-driven expiry, design/30 §10) — and that a non-TTL member in
// the same set survives.
func TestSetTTLExpiry(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(1, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer m.Stop()
	c := New(m, SingleShardDirectory(1))
	waitReady(t, c)
	ns, coll := []byte("sessions"), []byte("active")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.SAdd(ctx, ns, coll, []byte("keep")); err != nil {
		t.Fatalf("SAdd keep: %v", err)
	}
	if _, err := c.SAddTTL(ctx, ns, coll, 600, []byte("temp")); err != nil {
		t.Fatalf("SAddTTL: %v", err)
	}
	// present immediately
	if ok, _ := c.SIsMember(ctx, ns, coll, []byte("temp"), true); !ok {
		t.Fatal("temp should be present immediately after SAddTTL")
	}
	if n, _ := c.SCard(ctx, ns, coll, true); n != 2 {
		t.Fatalf("SCard = %d want 2", n)
	}

	// after expiry + a sweep tick, temp is gone but keep remains
	deadline := time.Now().Add(8 * time.Second)
	for {
		rctx, rcancel := context.WithTimeout(context.Background(), 1*time.Second)
		present, err := c.SIsMember(rctx, ns, coll, []byte("temp"), true)
		rcancel()
		if err == nil && !present {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("temp never expired via the sweeper")
		}
		time.Sleep(150 * time.Millisecond)
	}
	if ok, _ := c.SIsMember(ctx, ns, coll, []byte("keep"), true); !ok {
		t.Fatal("keep (no TTL) must survive")
	}
	if n, _ := c.SCard(ctx, ns, coll, true); n != 1 {
		t.Fatalf("SCard after expiry = %d want 1", n)
	}
}
