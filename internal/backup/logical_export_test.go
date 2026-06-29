package backup

import (
	"testing"

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

	man, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000)
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
