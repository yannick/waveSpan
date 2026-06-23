package vector

import "testing"

func TestBucketDirRouting(t *testing.T) {
	d := NewBucketDir("node1")

	// node1 holds buckets {1,2} for "docs"; peers advertise their own.
	d.AddOwn("docs", 1, 1)
	d.AddOwn("docs", 1, 2)
	d.ApplyPeer("node2", HeldBucket{Collection: "docs", QVer: 1, Buckets: []uint32{2, 3}, GeneratedAtUnixMs: 10})
	d.ApplyPeer("node3", HeldBucket{Collection: "docs", QVer: 1, Buckets: []uint32{9}, GeneratedAtUnixMs: 10})

	// probing bucket 2 should reach node1 (self) + node2, not node3.
	members, ok := d.Holders("docs", 1, []uint32{2})
	if !ok {
		t.Fatal("expected routing info")
	}
	got := map[string]bool{}
	for _, m := range members {
		got[m] = true
	}
	if !got["node1"] || !got["node2"] || got["node3"] {
		t.Fatalf("bucket 2 holders = %v, want node1+node2", members)
	}

	// probing bucket 3 should reach only node2.
	members, _ = d.Holders("docs", 1, []uint32{3})
	if len(members) != 1 || members[0] != "node2" {
		t.Fatalf("bucket 3 holders = %v, want [node2]", members)
	}

	// a different quantizer version must not match (buckets are version-scoped).
	if m, _ := d.Holders("docs", 2, []uint32{2}); len(m) != 0 {
		t.Fatalf("qver mismatch should yield no holders, got %v", m)
	}

	// newest-generation-wins: a newer advert replaces node2's set.
	d.ApplyPeer("node2", HeldBucket{Collection: "docs", QVer: 1, Buckets: []uint32{42}, GeneratedAtUnixMs: 20})
	if m, _ := d.Holders("docs", 1, []uint32{3}); len(m) != 0 {
		t.Fatalf("after newer advert node2 no longer holds bucket 3, got %v", m)
	}

	// an older advert is ignored.
	d.ApplyPeer("node2", HeldBucket{Collection: "docs", QVer: 1, Buckets: []uint32{3}, GeneratedAtUnixMs: 5})
	if m, _ := d.Holders("docs", 1, []uint32{42}); len(m) != 1 || m[0] != "node2" {
		t.Fatalf("older advert should be ignored, node2 keeps {42}, got %v", m)
	}

	// SetOwn de-advertises: drop bucket 1 from node1's own set.
	d.SetOwn("docs", 1, []uint32{2}, 30)
	if m, _ := d.Holders("docs", 1, []uint32{1}); len(m) != 0 {
		t.Fatalf("after SetOwn dropping bucket 1, no holder, got %v", m)
	}

	// unknown collection → no routing info (caller scatters to all).
	if _, ok := d.Holders("missing", 1, []uint32{1}); ok {
		t.Fatal("unknown collection should report no routing info")
	}
}

func TestQuantSetDeterministic(t *testing.T) {
	metas := []*IndexMeta{{Name: "docs", Collection: "docs", Metric: Cosine, Dimensions: 8}}
	a := NewQuantSet(metas, 256)
	b := NewQuantSet(metas, 256)
	qa, _ := a.For("docs")
	qb, _ := b.For("docs")
	v := []float32{0.3, -0.1, 0.5, 0.2, 0, 0, 0, 0.7}
	if qa.Bucket(v) != qb.Bucket(v) {
		t.Fatal("two QuantSets must derive the same bucket (all nodes agree)")
	}
}
