package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

func TestLogicalRoundTripSameShape(t *testing.T) {
	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })
	srcUUID, _ := storage.EnsureStorageUUID(src)

	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))
	mustPut(t, src, storage.CFKVMeta, []byte("k1"), []byte("ptr"))
	mustPut(t, src, storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll"), []byte("set"))

	store, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000); err != nil {
		t.Fatal(err)
	}

	// Fresh destination with its OWN identity.
	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	dstUUID, _ := storage.EnsureStorageUUID(dst)
	if dstUUID == srcUUID {
		t.Fatal("test precondition: dst should have a different UUID")
	}

	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{RestoreWallClockMs: 1719000100000}); err != nil {
		t.Fatal(err)
	}

	// Data restored.
	if v, ok, _ := dst.Get(storage.CFKVData, []byte("k1")); !ok || string(v) != "v1" {
		t.Fatalf("kv_data k1 not restored: ok=%v v=%q", ok, v)
	}
	if v, ok, _ := dst.Get(storage.CFReplData, []byte("\x00\x00\x00\x00\x00\x00\x00\x02coll")); !ok || string(v) != "set" {
		t.Fatalf("repl_data coll not restored: ok=%v v=%q", ok, v)
	}
	// Identity preserved (NOT overwritten by source).
	nowUUID, _, _ := dst.Get(storage.CFSys, []byte("/sys/storage_uuid"))
	if string(nowUUID) != dstUUID {
		t.Fatalf("dst identity was overwritten: want %q got %q", dstUUID, nowUUID)
	}
}

// TestLogicalRoundTripAllDatatypes proves every datatype round-trips verbatim in
// 2a (same-shape): graph data + index and vector raw + index are copied as-is, no
// rebuild needed.
func TestLogicalRoundTripAllDatatypes(t *testing.T) {
	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })

	seed := []struct {
		cf   storage.ColumnFamily
		k, v []byte
	}{
		{storage.CFGraphData, []byte("node:1"), []byte("alice")},
		{storage.CFGraphIndex, []byte("label:Person"), []byte("1")},
		{storage.CFVectorRaw, []byte("vec:1"), []byte("\x01\x02\x03\x04")},
		{storage.CFVectorIndex, []byte("seg:1"), []byte("ann-meta")},
	}
	for _, s := range seed {
		mustPut(t, src, s.cf, s.k, s.v)
	}

	store, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000); err != nil {
		t.Fatal(err)
	}

	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{}); err != nil {
		t.Fatal(err)
	}

	for _, s := range seed {
		v, ok, err := dst.Get(s.cf, s.k)
		if err != nil {
			t.Fatalf("get %v/%q: %v", s.cf, s.k, err)
		}
		if !ok || string(v) != string(s.v) {
			t.Fatalf("%v/%q not restored verbatim: ok=%v got=%q want=%q", s.cf, s.k, ok, v, s.v)
		}
	}
}
