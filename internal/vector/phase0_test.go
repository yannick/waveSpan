package vector

import (
	"testing"

	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/vector/ann"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// TestRebuildFromStoreRepopulates is the regression test for the Phase-0 boot-rebuild fix: a fresh
// IndexSet is empty, and RebuildFromStore must repopulate the ANN index from the authoritative raw
// vectors so a node restart doesn't lose searchability.
func TestRebuildFromStoreRepopulates(t *testing.T) {
	ws, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Close() }()
	vs := NewStore(ws)

	put := func(id string, v ...float32) {
		if err := vs.Put(&wavespanv1.VectorRecord{Collection: "c", VectorId: id, Values: v, Dimensions: uint32(len(v))}); err != nil {
			t.Fatal(err)
		}
	}
	put("a", 1, 0, 0)
	put("b", 0, 1, 0)
	put("c", 0, 0, 1)

	meta := &IndexMeta{Name: "c", Collection: "c", Metric: Cosine, Dimensions: 3}

	// A freshly-built index (the boot state) is empty — this is the bug if left unrebuilt.
	is := NewIndexSet([]*IndexMeta{meta}, ann.DefaultParams())
	if li, _ := is.Live("c"); li.Len() != 0 {
		t.Fatalf("new index should be empty, got %d", li.Len())
	}

	// Rebuild from the store (what boot must do).
	if err := is.RebuildFromStore(vs); err != nil {
		t.Fatal(err)
	}
	li, _ := is.Live("c")
	if li.Len() != 3 {
		t.Fatalf("after rebuild Len=%d, want 3", li.Len())
	}
	hits := li.Search([]float32{1, 0, 0}, 1, 32)
	if len(hits) == 0 || hits[0].ID != "a" {
		t.Fatalf("nearest to [1,0,0] should be a, got %+v", hits)
	}

	// A tombstoned vector must not be resurrected by a rebuild.
	if err := vs.Delete("c", "a", nil); err != nil {
		t.Fatal(err)
	}
	if err := is.RebuildFromStore(vs); err != nil {
		t.Fatal(err)
	}
	li, _ = is.Live("c")
	if li.Len() != 2 {
		t.Fatalf("after delete+rebuild Len=%d, want 2", li.Len())
	}
}

// TestCollectionDims validates the dimension resolver used to reject mismatched Put requests.
func TestCollectionDims(t *testing.T) {
	is := NewIndexSet([]*IndexMeta{{Name: "c", Collection: "c", Metric: Cosine, Dimensions: 8}}, ann.DefaultParams())
	if d, ok := is.CollectionDims("c"); !ok || d != 8 {
		t.Fatalf("CollectionDims(c) = %d,%v want 8,true", d, ok)
	}
	if _, ok := is.CollectionDims("missing"); ok {
		t.Fatal("CollectionDims(missing) should be false")
	}
}

// TestVecHashStable confirms the embedding-derived identity is deterministic and discriminating.
func TestVecHashStable(t *testing.T) {
	a := VecHash([]float32{1, 2, 3})
	if a != VecHash([]float32{1, 2, 3}) {
		t.Fatal("VecHash not deterministic")
	}
	if a == VecHash([]float32{1, 2, 4}) {
		t.Fatal("VecHash should differ for different vectors")
	}
}
