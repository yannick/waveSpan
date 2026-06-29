package backup

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// CFReplData wire-format constants, mirrored from package collections (the
// sub-prefix bytes and the uint32be-length-prefixed chunk encoding) so these
// backup tests can hand-build collection keys without collections exporting them.
const (
	subDataByte byte = 0x01 // collections.subData (statemachine.go)
	subMetaByte byte = 0x00 // collections.subMeta (base_sm.go)
)

// chunkBE encodes uint32be(len(b)) || b, matching collections.appendChunk.
func chunkBE(b []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	return append(l[:], b...)
}

// replDataSuffix builds a subData CFReplData suffix: subData || chunk(ns) || chunk(coll) || tail.
func replDataSuffix(ns, coll, tail string) []byte {
	s := []byte{subDataByte}
	s = append(s, chunkBE([]byte(ns))...)
	s = append(s, chunkBE([]byte(coll))...)
	return append(s, []byte(tail)...)
}

// replDataKey prefixes a suffix with be8(ShardForKey(ns,coll,n)).
func replDataKey(ns, coll, tail string, n uint64) []byte {
	suffix := replDataSuffix(ns, coll, tail)
	return append(be8(collections.ShardForKey([]byte(ns), []byte(coll), n)), suffix...)
}

func TestReshardKey(t *testing.T) {
	const n = 8

	// A data-shard subData row re-routes to its shard under n (prefix rewritten, suffix kept).
	dataKey := replDataKey("ns1", "c1", "row", 4)
	gotKey, keep, err := reshardKey(dataKey, n)
	wantKey := append(be8(collections.ShardForKey([]byte("ns1"), []byte("c1"), n)), dataKey[8:]...)
	if err != nil || !keep || !bytes.Equal(gotKey, wantKey) {
		t.Fatalf("data-shard reroute: keep=%v err=%v key=%x want=%x", keep, err, gotKey, wantKey)
	}

	// A meta-shard subData row with an EMPTY routing body must DROP, not error.
	metaEmpty := append(be8(collections.MetaShardID), subDataByte)
	if _, keep, err := reshardKey(metaEmpty, n); err != nil || keep {
		t.Fatalf("meta-shard empty subData must drop (no error): keep=%v err=%v", keep, err)
	}

	// A subMeta row on a data shard drops (shard-local bookkeeping).
	sm := append(be8(collections.FirstDataShard), subMetaByte)
	sm = append(sm, []byte("applied")...)
	if _, keep, err := reshardKey(sm, n); err != nil || keep {
		t.Fatalf("subMeta must drop: keep=%v err=%v", keep, err)
	}

	// An unknown sub-prefix on a DATA shard is a loud error.
	unknown := append(be8(collections.FirstDataShard), 0x7f, 'x')
	if _, _, err := reshardKey(unknown, n); err == nil {
		t.Fatal("unknown sub-prefix on a data shard must error")
	}
}

func TestRestoreLogicalReshardsCFReplData(t *testing.T) {
	const srcN, dstN = 4, 8
	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })

	pairs := []struct{ ns, coll string }{{"ns1", "c1"}, {"ns2", "c2"}, {"ns3", "c3"}}
	for _, p := range pairs {
		mustPut(t, src, storage.CFReplData, replDataKey(p.ns, p.coll, "row", srcN), []byte("val:"+p.ns+p.coll))
	}
	// A shard-local subMeta row (be8(shard) || subMeta || "applied"): must drop on re-shard.
	metaKey := append(be8(7), subMetaByte)
	metaKey = append(metaKey, []byte("applied")...)
	mustPut(t, src, storage.CFReplData, metaKey, []byte("99"))

	store, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000); err != nil {
		t.Fatal(err)
	}

	// Re-shard restore to N=8.
	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{CollectionsDataShards: dstN}); err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		newKey := replDataKey(p.ns, p.coll, "row", dstN)
		if v, ok, _ := dst.Get(storage.CFReplData, newKey); !ok || string(v) != "val:"+p.ns+p.coll {
			t.Fatalf("%s/%s not at N=8 shard: ok=%v v=%q", p.ns, p.coll, ok, v)
		}
		oldShard := collections.ShardForKey([]byte(p.ns), []byte(p.coll), srcN)
		newShard := collections.ShardForKey([]byte(p.ns), []byte(p.coll), dstN)
		if oldShard != newShard {
			oldKey := replDataKey(p.ns, p.coll, "row", srcN)
			if _, ok, _ := dst.Get(storage.CFReplData, oldKey); ok {
				t.Fatalf("%s/%s should not remain under old N=4 shard %d", p.ns, p.coll, oldShard)
			}
		}
	}
	// subMeta dropped.
	if _, ok, _ := dst.Get(storage.CFReplData, metaKey); ok {
		t.Fatal("subMeta row must be dropped on re-shard")
	}

	// Zero-N restore is byte-for-byte verbatim (Phase 2a regression guard).
	dst2, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst2.Close() })
	if err := RestoreLogical(dst2, store, "bk", DefaultRegistry(), RestoreInfo{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range pairs {
		origKey := replDataKey(p.ns, p.coll, "row", srcN)
		if _, ok, _ := dst2.Get(storage.CFReplData, origKey); !ok {
			t.Fatalf("verbatim restore lost %s/%s", p.ns, p.coll)
		}
	}
	if _, ok, _ := dst2.Get(storage.CFReplData, metaKey); !ok {
		t.Fatal("verbatim restore must keep the subMeta row")
	}
}

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
