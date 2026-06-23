package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestIdempotentWrite verifies that a write carrying an idempotency key is applied exactly once: a
// retry with the same key returns the original result without re-applying, while a different key (or
// no key) is a normal write (design/30 §13.12).
func TestIdempotentWrite(t *testing.T) {
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
	ns, coll := []byte("app"), []byte("s")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyed := command{Op: opSAdd, NS: ns, Coll: coll, Idem: []byte("req-1"),
		Items: itemsFromKeys([][]byte{[]byte("x"), []byte("y")})}

	// First apply adds both members.
	if n, err := c.proposeCmd(ctx, keyed); err != nil || n != 2 {
		t.Fatalf("first keyed write = %d,%v want 2", n, err)
	}
	// Retry with the SAME key returns the cached count (2), not 0 — proof it was not re-applied.
	if n, err := c.proposeCmd(ctx, keyed); err != nil || n != 2 {
		t.Fatalf("idempotent retry = %d,%v want cached 2 (re-applied would be 0)", n, err)
	}
	// State is exactly the two members (no double-apply), counter exact.
	cc, err := c.CardCheck(ctx, ns, coll, true)
	if err != nil || cc.Stored != 2 || cc.Counted != 2 {
		t.Fatalf("after idempotent retry CardCheck = %+v,%v want {2,2}", cc, err)
	}
	// A DIFFERENT key adding the same members is a normal write: they already exist, so 0 added.
	other := command{Op: opSAdd, NS: ns, Coll: coll, Idem: []byte("req-2"),
		Items: itemsFromKeys([][]byte{[]byte("x"), []byte("y")})}
	if n, err := c.proposeCmd(ctx, other); err != nil || n != 0 {
		t.Fatalf("different-key write = %d,%v want 0 (members already present)", n, err)
	}
}
