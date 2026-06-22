package vector

import (
	"testing"

	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/vector/ann"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func params() ann.Params { return ann.Params{M: 8, EfConstruction: 64, EfSearchDefault: 32, Seed: 1} }

func candIDs(cs []ann.Candidate) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.ID
	}
	return out
}

func hasCand(cs []ann.Candidate, id string) bool {
	for _, c := range cs {
		if c.ID == id {
			return true
		}
	}
	return false
}

func TestDeltaImmediateVisibility(t *testing.T) {
	li := NewLiveIndex(Cosine, params())
	li.Insert("fresh", []float32{1, 0})
	// main segment is empty; the vector must be found via the delta immediately, before any merge
	if !hasCand(li.Search([]float32{1, 0}, 5, 0), "fresh") {
		t.Fatal("freshly inserted vector not visible through the delta")
	}
}

func TestDeltaTombstoneHidesMainSegment(t *testing.T) {
	li := NewLiveIndex(Cosine, params())
	li.Insert("a", []float32{1, 0})
	li.Insert("b", []float32{0, 1})
	li.Merge() // a, b now in the main segment
	if !hasCand(li.Search([]float32{1, 0}, 5, 0), "a") {
		t.Fatal("a should be in the main segment")
	}
	li.Delete("a") // delta tombstone over a main-segment vector
	if hasCand(li.Search([]float32{1, 0}, 5, 0), "a") {
		t.Fatal("tombstoned vector should be hidden")
	}
}

func TestMergeDrainsDeltaIntoSegment(t *testing.T) {
	li := NewLiveIndex(Cosine, params())
	li.Insert("a", []float32{1, 0})
	li.Insert("b", []float32{0.9, 0.1})
	before := candIDs(li.Search([]float32{1, 0}, 2, 0))
	li.Merge()
	if li.delta.Len() != 0 {
		t.Fatal("delta should be empty after merge")
	}
	after := candIDs(li.Search([]float32{1, 0}, 2, 0))
	if len(before) != 2 || len(after) != 2 || before[0] != after[0] {
		t.Fatalf("results changed across merge: %v vs %v", before, after)
	}
	if li.Main().vecs["a"] == nil {
		t.Fatal("merged vector should live in the main segment")
	}
}

func TestOldSegmentGCAfterNoReaders(t *testing.T) {
	li := NewLiveIndex(Cosine, params())
	li.Insert("a", []float32{1, 0})
	li.Merge()
	old := li.Main()
	old.Acquire() // an in-flight reader holds the old segment
	li.Insert("b", []float32{0, 1})
	retired := li.Merge()
	if retired != old {
		t.Fatal("merge should retire the previous main segment")
	}
	if old.GCed() {
		t.Fatal("retired segment must not be GC'd while a reader holds it")
	}
	old.Release()
	if !old.GCed() {
		t.Fatal("retired segment should be GC'd once readers drain")
	}
}

func TestRebuildFromRawRecords(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	store := NewStore(mem)
	for _, tc := range []struct {
		id string
		v  []float32
	}{{"a", []float32{1, 0}}, {"b", []float32{0, 1}}, {"c", []float32{0.9, 0.1}}} {
		if err := store.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: tc.id, Values: tc.v, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = store.Delete("docs", "b", &wavespanv1.Version{HlcPhysicalMs: 9}) // tombstoned -> excluded from rebuild

	li, err := RebuildLiveIndex(store, "docs", Cosine, params())
	if err != nil {
		t.Fatal(err)
	}
	got := li.Search([]float32{1, 0}, 5, 0)
	if hasCand(got, "b") {
		t.Fatal("tombstoned vector must not be in the rebuilt index")
	}
	if !hasCand(got, "a") || !hasCand(got, "c") {
		t.Fatalf("rebuilt index missing live vectors: %v", candIDs(got))
	}
}

func TestExactRerank(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	store := NewStore(mem)
	put := func(id string, v ...float32) {
		_ = store.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: v, Version: &wavespanv1.Version{HlcPhysicalMs: 1}})
	}
	put("a", 1, 0)
	put("b", 0.8, 0.2)
	put("c", 0.95, 0.05)
	// ANN candidates in an arbitrary (approximate) order
	cands := []ann.Candidate{{ID: "b"}, {ID: "a"}, {ID: "c"}}
	got := candIDs(Rerank(store, "docs", []float32{1, 0}, cands, 3, Cosine))
	if got[0] != "a" || got[1] != "c" || got[2] != "b" {
		t.Fatalf("rerank should order by exact distance a,c,b: %v", got)
	}
}
