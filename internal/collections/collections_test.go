package collections

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
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

// TestHashZSetWrongType exercises the Hash and Sorted-set datatypes and the WRONGTYPE guard over one
// shard (design/30 §13.5-13.6).
func TestHashZSetWrongType(t *testing.T) {
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
	ns := []byte("app")
	ctx := func() context.Context {
		cc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)
		return cc
	}

	// --- Hash ---
	h := []byte("h1")
	if n, err := c.HSet(ctx(), ns, h, FieldValue{[]byte("f1"), []byte("v1")}, FieldValue{[]byte("f2"), []byte("v2")}); err != nil || n != 2 {
		t.Fatalf("HSet new = %d,%v want 2", n, err)
	}
	if n, err := c.HSet(ctx(), ns, h, FieldValue{[]byte("f1"), []byte("v1b")}); err != nil || n != 0 {
		t.Fatalf("HSet update = %d,%v want 0", n, err)
	}
	if n, _ := c.HLen(ctx(), ns, h, true); n != 2 {
		t.Fatalf("HLen = %d want 2", n)
	}
	if v, ok, _ := c.HGet(ctx(), ns, h, []byte("f1"), true); !ok || string(v) != "v1b" {
		t.Fatalf("HGet f1 = %q,%v want v1b,true", v, ok)
	}
	if _, ok, _ := c.HGet(ctx(), ns, h, []byte("f3"), false); ok {
		t.Fatal("HGet f3 should be absent")
	}
	if all, _ := c.HGetAll(ctx(), ns, h, 0, true); len(all) != 2 {
		t.Fatalf("HGetAll = %v want 2 fields", all)
	}
	if n, err := c.HDel(ctx(), ns, h, []byte("f1")); err != nil || n != 1 {
		t.Fatalf("HDel = %d,%v want 1", n, err)
	}
	if n, _ := c.HLen(ctx(), ns, h, true); n != 1 {
		t.Fatalf("HLen after del = %d want 1", n)
	}

	// --- Sorted set ---
	z := []byte("z1")
	if n, err := c.ZAdd(ctx(), ns, z, ScoredMember{[]byte("a"), 1}, ScoredMember{[]byte("b"), 2}, ScoredMember{[]byte("c"), 3}); err != nil || n != 3 {
		t.Fatalf("ZAdd new = %d,%v want 3", n, err)
	}
	if n, err := c.ZAdd(ctx(), ns, z, ScoredMember{[]byte("a"), 5}); err != nil || n != 0 {
		t.Fatalf("ZAdd update = %d,%v want 0", n, err)
	}
	if sc, ok, _ := c.ZScore(ctx(), ns, z, []byte("a"), true); !ok || sc != 5 {
		t.Fatalf("ZScore a = %v,%v want 5", sc, ok)
	}
	if n, _ := c.ZCard(ctx(), ns, z, true); n != 3 {
		t.Fatalf("ZCard = %d want 3", n)
	}
	// after a:5, ascending score order is b(2), c(3), a(5)
	if got := zMembers(t, c, ns, z); !equalMembers(got, [][]byte{[]byte("b"), []byte("c"), []byte("a")}) {
		t.Fatalf("ZRange = %q want [b c a]", got)
	}
	if n, err := c.ZRem(ctx(), ns, z, []byte("b")); err != nil || n != 1 {
		t.Fatalf("ZRem = %d,%v want 1", n, err)
	}
	if got := zMembers(t, c, ns, z); !equalMembers(got, [][]byte{[]byte("c"), []byte("a")}) {
		t.Fatalf("ZRange after rem = %q want [c a]", got)
	}

	// --- WRONGTYPE ---
	if _, err := c.SAdd(ctx(), ns, h, []byte("x")); err != ErrWrongType {
		t.Fatalf("SAdd on a hash = %v want ErrWrongType", err)
	}
	s := []byte("s1")
	if _, err := c.SAdd(ctx(), ns, s, []byte("m")); err != nil {
		t.Fatalf("SAdd set: %v", err)
	}
	if _, err := c.HSet(ctx(), ns, s, FieldValue{[]byte("f"), []byte("v")}); err != ErrWrongType {
		t.Fatalf("HSet on a set = %v want ErrWrongType", err)
	}
}

func zMembers(t *testing.T, c *Collections, ns, coll []byte) [][]byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, err := c.ZRange(ctx, ns, coll, 0, true)
	if err != nil {
		t.Fatalf("ZRange: %v", err)
	}
	out := make([][]byte, len(rows))
	for i, r := range rows {
		out[i] = r.Member
	}
	return out
}

func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func newMgr(t testing.TB, dir, addr string, store storage.LocalStore) *Manager {
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
func waitReady(t testing.TB, c *Collections) {
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
