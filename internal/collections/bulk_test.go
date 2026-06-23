package collections

import (
	"context"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// TestBulkRemoveAcrossCollections covers enumeration + the type-agnostic bulk removal: a member is
// removed from a set, a hash, and a sorted set in one call, leaving the rest intact (design/30 §13.7).
func TestBulkRemoveAcrossCollections(t *testing.T) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// user-42 appears in a set, a hash, and a zset (alongside others).
	if _, err := c.SAdd(ctx, ns, []byte("s1"), []byte("user-42"), []byte("user-7")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.HSet(ctx, ns, []byte("h1"),
		FieldValue{Field: []byte("user-42"), Value: []byte("x")},
		FieldValue{Field: []byte("admin"), Value: []byte("y")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ZAdd(ctx, ns, []byte("z1"),
		ScoredMember{Member: []byte("user-42"), Score: 1},
		ScoredMember{Member: []byte("vip"), Score: 2}); err != nil {
		t.Fatal(err)
	}

	// Enumeration finds all three collections.
	listed, err := c.ListCollections(ctx, ns, true)
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	got := map[string]bool{}
	for _, coll := range listed {
		got[string(coll)] = true
	}
	if len(listed) != 3 || !got["s1"] || !got["h1"] || !got["z1"] {
		t.Fatalf("ListCollections = %v want {s1,h1,z1}", got)
	}

	// Bulk-remove user-42 from EVERY collection in the namespace.
	res, err := c.BulkRemove(ctx, ns, nil, [][]byte{[]byte("user-42")})
	if err != nil {
		t.Fatalf("BulkRemove: %v", err)
	}
	var total uint64
	for _, e := range res {
		if e.Err != nil {
			t.Fatalf("BulkRemove %q: %v", e.Collection, e.Err)
		}
		total += e.Removed
	}
	if len(res) != 3 || total != 3 {
		t.Fatalf("BulkRemove removed %d across %d colls, want 3 across 3", total, len(res))
	}

	// user-42 is gone everywhere; the rest survive; cardinalities are exact.
	if ok, _ := c.SIsMember(ctx, ns, []byte("s1"), []byte("user-42"), true); ok {
		t.Fatal("user-42 still in s1")
	}
	if _, found, _ := c.HGet(ctx, ns, []byte("h1"), []byte("user-42"), true); found {
		t.Fatal("user-42 still in h1")
	}
	if _, found, _ := c.ZScore(ctx, ns, []byte("z1"), []byte("user-42"), true); found {
		t.Fatal("user-42 still in z1")
	}
	if ok, _ := c.SIsMember(ctx, ns, []byte("s1"), []byte("user-7"), true); !ok {
		t.Fatal("user-7 wrongly removed from s1")
	}
	for _, coll := range []string{"s1", "h1", "z1"} {
		cc, err := c.CardCheck(ctx, ns, []byte(coll), true)
		if err != nil || cc.Stored != 1 || cc.Counted != 1 {
			t.Fatalf("%s CardCheck = %+v,%v want {1,1}", coll, cc, err)
		}
	}

	// A named-list bulk-remove touches only the listed collection.
	res2, err := c.BulkRemove(ctx, ns, [][]byte{[]byte("s1")}, [][]byte{[]byte("user-7")})
	if err != nil || len(res2) != 1 || res2[0].Removed != 1 {
		t.Fatalf("named BulkRemove = %+v,%v want 1 removed from s1", res2, err)
	}
	if cc, _ := c.CardCheck(ctx, ns, []byte("h1"), true); cc.Stored != 1 {
		t.Fatalf("h1 changed by a named-list remove targeting s1")
	}
}
