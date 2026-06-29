package backup

import (
	"bufio"
	"bytes"
	"strings"
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

// TestRestoreLogicalRejectsUnsupportedVersion checks the format-version guard at
// both bounds: a zero/missing version (corruption) and a future version.
func TestRestoreLogicalRejectsUnsupportedVersion(t *testing.T) {
	for _, ver := range []int{0, manifestFormatVersion + 1} {
		store, _ := objstore.NewFS(t.TempDir())
		man := NodeManifest{FormatVersion: ver}
		var buf bytes.Buffer
		if err := man.Encode(&buf); err != nil {
			t.Fatal(err)
		}
		if err := store.Put("bk/node.manifest.json", bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			t.Fatal(err)
		}
		dst, _ := storage.OpenWavesdb(t.TempDir())
		err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{})
		_ = dst.Close()
		if err == nil {
			t.Fatalf("format version %d: expected an unsupported-version error, got nil", ver)
		}
	}
}

// TestRestoreLogicalDetectsTruncatedObject proves the manifest entry-count
// integrity check catches a truncated/corrupt CF object — readBytes returns a
// clean io.EOF after a complete pair, so a dropped tail would otherwise restore
// silently. Here the kv_data object is rewritten with one pair while the manifest
// still claims two.
func TestRestoreLogicalDetectsTruncatedObject(t *testing.T) {
	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })
	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))
	mustPut(t, src, storage.CFKVData, []byte("k2"), []byte("v2"))

	store, _ := objstore.NewFS(t.TempDir())
	man, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000)
	if err != nil {
		t.Fatal(err)
	}
	if man.CFEntryCount("kv_data") != 2 {
		t.Fatalf("precondition: want 2 kv_data entries, got %d", man.CFEntryCount("kv_data"))
	}

	// Overwrite the kv_data object with only ONE complete pair (cleanly truncated
	// tail) while the manifest still records two.
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	writeBytes(bw, []byte("k1"))
	writeBytes(bw, []byte("v1"))
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("bk/cf/kv_data", bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
		t.Fatal(err)
	}

	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	err = RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{})
	if err == nil {
		t.Fatal("expected a count-mismatch error on a truncated CF object, got nil")
	}
	if !strings.Contains(err.Error(), "count") {
		t.Fatalf("error should mention count mismatch, got: %v", err)
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
