package vector

import (
	"testing"

	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func searchStore(t *testing.T, vecs map[string][]float32) *Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	store := NewStore(mem)
	for id, v := range vecs {
		if err := store.Put(&wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: v, GraphNodeId: "node-" + id, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func TestLocalSearchExact(t *testing.T) {
	store := searchStore(t, map[string][]float32{
		"a": {1, 0}, "b": {0, 1}, "c": {0.9, 0.1},
	})
	meta := &IndexMeta{Name: "docs", Collection: "docs", Metric: Cosine}
	hits := LocalSearch(store, meta, nil, []float32{1, 0}, 2, 0, true, false)
	if len(hits) != 2 {
		t.Fatalf("expected top-2, got %d", len(hits))
	}
	if hits[0].VectorID != "a" || hits[1].VectorID != "c" {
		t.Fatalf("expected [a, c] closest to (1,0), got [%s, %s]", hits[0].VectorID, hits[1].VectorID)
	}
	if hits[0].GraphNodeID != "node-a" {
		t.Fatalf("graph node id should be carried, got %q", hits[0].GraphNodeID)
	}
}

func TestLocalSearchApproxExactScored(t *testing.T) {
	store := searchStore(t, map[string][]float32{"a": {1, 0}, "b": {0, 1}, "c": {0.9, 0.1}})
	live, err := RebuildLiveIndex(store, "docs", Cosine, params())
	if err != nil {
		t.Fatal(err)
	}
	meta := &IndexMeta{Name: "docs", Collection: "docs", Metric: Cosine}
	hits := LocalSearch(store, meta, live, []float32{1, 0}, 2, 16, false, true)
	if len(hits) == 0 || hits[0].VectorID != "a" {
		t.Fatalf("approx search should rank a first, got %+v", hits)
	}
	// approx path must exact-score from the stored vector (score in [0,1] for cosine, a == 1.0)
	if hits[0].Score < 0.999 {
		t.Fatalf("nearest hit should be exact-scored ~1.0, got %v", hits[0].Score)
	}
}

func TestMergeTopKDedupsReplicatedIDs(t *testing.T) {
	// the same vector "a" appears in two holders' fragments (replicated); it must consume ONE slot.
	frag1 := []Hit{{Collection: "docs", VectorID: "a", Distance: 0.0}, {Collection: "docs", VectorID: "b", Distance: 0.5}}
	frag2 := []Hit{{Collection: "docs", VectorID: "a", Distance: 0.0}, {Collection: "docs", VectorID: "c", Distance: 0.2}}
	merged := MergeTopK([][]Hit{frag1, frag2}, 3)
	if len(merged) != 3 {
		t.Fatalf("expected 3 distinct ids, got %d: %+v", len(merged), merged)
	}
	seen := map[string]int{}
	for _, h := range merged {
		seen[h.VectorID]++
	}
	if seen["a"] != 1 {
		t.Fatalf("replicated id 'a' should appear once, got %d", seen["a"])
	}
	// global order by distance: a(0.0) < c(0.2) < b(0.5)
	if merged[0].VectorID != "a" || merged[1].VectorID != "c" || merged[2].VectorID != "b" {
		t.Fatalf("merge order wrong: %s,%s,%s", merged[0].VectorID, merged[1].VectorID, merged[2].VectorID)
	}
}
