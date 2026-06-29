package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

func mustPut(t *testing.T, s storage.LocalStore, cf storage.ColumnFamily, k, v []byte) {
	t.Helper()
	if err := s.Put(cf, k, v); err != nil {
		t.Fatal(err)
	}
}

func TestExportLogicalWritesObjectsAndManifest(t *testing.T) {
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// Seed authoritative + derived + transient data.
	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))
	mustPut(t, src, storage.CFKVData, []byte("k2"), []byte("v2"))
	mustPut(t, src, storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll"), []byte("set"))
	mustPut(t, src, storage.CFGraphIndex, []byte("idx"), []byte("derived")) // 2a: copied verbatim
	mustPut(t, src, storage.CFCacheMeta, []byte("c"), []byte("transient"))  // transient: must NOT be exported

	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	man, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000, Selector{})
	if err != nil {
		t.Fatal(err)
	}

	if man.CFEntryCount("kv_data") != 2 {
		t.Fatalf("want 2 kv_data entries, got %d", man.CFEntryCount("kv_data"))
	}
	if man.CFEntryCount("repl_data") != 1 {
		t.Fatalf("want 1 repl_data entry, got %d", man.CFEntryCount("repl_data"))
	}
	if man.CFEntryCount("graph_index") != 1 {
		t.Fatal("2a copies graph_index verbatim — want 1")
	}
	if man.CFEntryCount("cache_meta") != 0 {
		t.Fatal("transient cache_meta must not be exported")
	}
	if ok, _ := store.Exists("bk/node.manifest.json"); !ok {
		t.Fatal("manifest object missing")
	}
}

func TestExportLogicalAppliesSelector(t *testing.T) {
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })

	mustPut(t, src, storage.CFSys, []byte("/sys/config"), []byte("x"))
	mustPut(t, src, storage.CFKVData, kvKey("ns1", "k"), []byte("a"))
	mustPut(t, src, storage.CFKVData, kvKey("ns2", "k"), []byte("b"))
	mustPut(t, src, storage.CFReplData, replDataKey("ns1", "c", "row", 4), []byte("a"))
	mustPut(t, src, storage.CFReplData, replDataKey("ns2", "c", "row", 4), []byte("b"))
	mustPut(t, src, storage.CFGraphData, graph.NodeKey("g1", "n"), []byte("a"))
	mustPut(t, src, storage.CFGraphData, graph.NodeKey("g2", "n"), []byte("b"))
	mustPut(t, src, storage.CFVectorRaw, vrKey("c1", "v"), []byte("a"))
	mustPut(t, src, storage.CFVectorRaw, vrKey("c2", "v"), []byte("b"))

	// Partial export: only ns1 + g1 + c1 (plus always-on CFSys).
	store, _ := objstore.NewFS(t.TempDir())
	sel := Selector{Namespaces: Set("ns1"), Graphs: Set("g1"), VectorCollections: Set("c1")}
	man, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000, sel)
	if err != nil {
		t.Fatal(err)
	}
	for _, cf := range []string{"kv_data", "repl_data", "graph_data", "vector_raw"} {
		if got := man.CFEntryCount(cf); got != 1 {
			t.Fatalf("partial export %s: got %d entries, want 1 (only the selected entity)", cf, got)
		}
	}
	if man.CFEntryCount("sys") < 1 {
		t.Fatal("CFSys must always be exported regardless of selector")
	}

	// Empty selector exports everything (regression: same as a full export).
	store2, _ := objstore.NewFS(t.TempDir())
	full, err := ExportLogical(src, store2, "bk", DefaultRegistry(), 1719000000000, Selector{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cf := range []string{"kv_data", "repl_data", "graph_data", "vector_raw"} {
		if got := full.CFEntryCount(cf); got != 2 {
			t.Fatalf("full export %s: got %d entries, want 2", cf, got)
		}
	}
}
