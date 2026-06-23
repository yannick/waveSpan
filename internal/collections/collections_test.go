package collections

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/cwire/wavespan/internal/storage"
)

// TestSetOpsAndRestart exercises the Set datatype end to end over a single dragonboat shard
// (design/30 §13.4): add/dedup, exact cardinality, membership, enumeration, removal — then a NodeHost
// restart to confirm the on-disk state machine persists (resumes from the applied index).
func TestSetOpsAndRestart(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	dir := t.TempDir()
	addr := freeAddr(t)
	members := map[uint64]string{1: addr}
	ns, coll := []byte("flags"), []byte("enabled")

	m := newMgr(t, dir, addr, mem)
	if err := m.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	c := New(m, SingleShardDirectory(1))
	waitReady(t, c)

	ctx := func() context.Context {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)
		return c
	}

	if n, err := c.SAdd(ctx(), ns, coll, []byte("a"), []byte("b"), []byte("c")); err != nil || n != 3 {
		t.Fatalf("SAdd a,b,c = %d,%v want 3", n, err)
	}
	if n, err := c.SAdd(ctx(), ns, coll, []byte("a"), []byte("b")); err != nil || n != 0 {
		t.Fatalf("SAdd dup = %d,%v want 0 (dedup)", n, err)
	}
	if n, err := c.SCard(ctx(), ns, coll, true); err != nil || n != 3 {
		t.Fatalf("SCard = %d,%v want 3", n, err)
	}
	for _, tc := range []struct {
		member string
		want   bool
	}{{"a", true}, {"z", false}} {
		for _, lin := range []bool{false, true} {
			got, err := c.SIsMember(ctx(), ns, coll, []byte(tc.member), lin)
			if err != nil || got != tc.want {
				t.Fatalf("SIsMember %q (lin=%v) = %v,%v want %v", tc.member, lin, got, err, tc.want)
			}
		}
	}
	if got := mustMembers(t, c, ns, coll); !equalMembers(got, [][]byte{[]byte("a"), []byte("b"), []byte("c")}) {
		t.Fatalf("SMembers = %q want [a b c]", got)
	}

	if n, err := c.SRem(ctx(), ns, coll, []byte("b")); err != nil || n != 1 {
		t.Fatalf("SRem b = %d,%v want 1", n, err)
	}
	if n, err := c.SRem(ctx(), ns, coll, []byte("b")); err != nil || n != 0 {
		t.Fatalf("SRem b again = %d,%v want 0", n, err)
	}
	if n, _ := c.SCard(ctx(), ns, coll, true); n != 2 {
		t.Fatalf("SCard after rem = %d want 2", n)
	}
	if got := mustMembers(t, c, ns, coll); !equalMembers(got, [][]byte{[]byte("a"), []byte("c")}) {
		t.Fatalf("SMembers after rem = %q want [a c]", got)
	}
	m.Stop()

	// restart: same store + dir + addr; the SM resumes from the applied index, data persists.
	m2 := newMgr(t, dir, addr, mem)
	if err := m2.StartShard(1, 1, members, false); err != nil {
		t.Fatalf("restart StartShard: %v", err)
	}
	defer m2.Stop()
	c2 := New(m2, SingleShardDirectory(1))
	waitReady(t, c2)

	if n, _ := c2.SCard(ctx(), ns, coll, true); n != 2 {
		t.Fatalf("post-restart SCard = %d want 2", n)
	}
	if got := mustMembers(t, c2, ns, coll); !equalMembers(got, [][]byte{[]byte("a"), []byte("c")}) {
		t.Fatalf("post-restart SMembers = %q want [a c]", got)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func newMgr(t *testing.T, dir, addr string, store storage.LocalStore) *Manager {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m, err := NewManager(dir, addr, store)
		if err == nil {
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewManager never succeeded: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitReady probes with a zero-member SAdd (a committed no-op) until the shard has a leader.
func waitReady(t *testing.T, c *Collections) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := c.SAdd(ctx, []byte("__probe__"), []byte("__probe__"))
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard never became ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func mustMembers(t *testing.T, c *Collections, ns, coll []byte) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got, err := c.SMembers(ctx, ns, coll, 0, true)
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	return got
}

func equalMembers(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
