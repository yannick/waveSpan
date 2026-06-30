package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// TestPartialBackupRoundTrip extracts one tenant's slice (ns1 + g1 + c1) across
// all four datatypes and proves it restores correctly while the other tenant's
// data (ns2/g2/c2) is absent. Crucially it asserts INDEX CONSISTENCY on the
// partial restore: the graph label index (CFGraphIndex) and the vector data
// (CFVectorRaw) — both exported verbatim-but-filtered — answer queries for the
// selected entities and return nothing for the excluded ones. That is the proof
// prefix-aware filtering kept index entries attributed to the right entity.
func TestPartialBackupRoundTrip(t *testing.T) {
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// KV in two namespaces.
	mustPut(t, src, storage.CFKVData, kvKey("ns1", "k1"), []byte("v-ns1"))
	mustPut(t, src, storage.CFKVData, kvKey("ns2", "k2"), []byte("v-ns2"))
	// Collections rows in two namespaces.
	mustPut(t, src, storage.CFReplData, replDataKey("ns1", "coll", "row", 4), []byte("c-ns1"))
	mustPut(t, src, storage.CFReplData, replDataKey("ns2", "coll", "row", 4), []byte("c-ns2"))

	// Graph: two graphs, each with a labeled node (writes CFGraphData + CFGraphIndex).
	gs := graph.NewStore(src)
	mkNode := func(g, id string) *wavespanv1.NodeRecord {
		return &wavespanv1.NodeRecord{GraphId: g, NodeId: id, Labels: []string{"User"}, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	}
	if err := gs.CreateNode(mkNode("g1", "n1")); err != nil {
		t.Fatal(err)
	}
	if err := gs.CreateNode(mkNode("g2", "n2")); err != nil {
		t.Fatal(err)
	}

	// Vector: two collections, each with a vector (writes CFVectorRaw + CFVectorIndex).
	vs := vector.NewStore(src)
	mkVec := func(coll, id string, vals []float32) *wavespanv1.VectorRecord {
		return &wavespanv1.VectorRecord{Collection: coll, VectorId: id, Values: vals, GraphNodeId: "node-" + id, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	}
	if err := vs.Put(mkVec("c1", "v1", []float32{1, 0})); err != nil {
		t.Fatal(err)
	}
	if err := vs.Put(mkVec("c2", "v2", []float32{0, 1})); err != nil {
		t.Fatal(err)
	}

	// Partial export: only ns1 + g1 + c1.
	store, _ := objstore.NewFS(t.TempDir())
	sel := Selector{Namespaces: Set("ns1"), Graphs: Set("g1"), VectorCollections: Set("c1")}
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000, sel); err != nil {
		t.Fatal(err)
	}

	// Restore into a fresh dst (same shape; no re-shard).
	dst, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{}); err != nil {
		t.Fatal(err)
	}

	// (a) Selected data present; the other tenant's data absent.
	if v, ok, _ := dst.Get(storage.CFKVData, kvKey("ns1", "k1")); !ok || string(v) != "v-ns1" {
		t.Fatalf("ns1 KV not restored: ok=%v v=%q", ok, v)
	}
	if _, ok, _ := dst.Get(storage.CFKVData, kvKey("ns2", "k2")); ok {
		t.Fatal("ns2 KV must be absent from a partial ns1 backup")
	}
	if v, ok, _ := dst.Get(storage.CFReplData, replDataKey("ns1", "coll", "row", 4)); !ok || string(v) != "c-ns1" {
		t.Fatalf("ns1 collections row not restored: ok=%v v=%q", ok, v)
	}
	if _, ok, _ := dst.Get(storage.CFReplData, replDataKey("ns2", "coll", "row", 4)); ok {
		t.Fatal("ns2 collections row must be absent")
	}

	// (b) Index consistency on the partial restore.
	// Graph: the g1 label index (CFGraphIndex) resolves the seeded node; g2 is absent.
	rgs := graph.NewStore(dst)
	if ids, err := rgs.ScanLabel("g1", "User"); err != nil || len(ids) != 1 || ids[0] != "n1" {
		t.Fatalf("g1 label index inconsistent after partial restore: ids=%v err=%v", ids, err)
	}
	if ids, _ := rgs.ScanLabel("g2", "User"); len(ids) != 0 {
		t.Fatalf("g2 must be absent from a partial g1 backup, got %v", ids)
	}
	// Vector: a c1 search returns the seeded vector; c2 returns nothing.
	rvs := vector.NewStore(dst)
	c1 := vector.LocalSearch(rvs, &vector.IndexMeta{Name: "c1", Collection: "c1", Metric: vector.Cosine}, nil, []float32{1, 0}, 5, 0, true, false)
	if len(c1) != 1 || c1[0].VectorID != "v1" {
		t.Fatalf("c1 vector search inconsistent after partial restore: %+v", c1)
	}
	c2 := vector.LocalSearch(rvs, &vector.IndexMeta{Name: "c2", Collection: "c2", Metric: vector.Cosine}, nil, []float32{0, 1}, 5, 0, true, false)
	if len(c2) != 0 {
		t.Fatalf("c2 must be absent from a partial c1 backup, got %+v", c2)
	}
}
