package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// seedLogicalBackup seeds a source store (KV + collections at N=srcN + graph + vector + raft bookkeeping),
// exports it logically to objStore as backupID with one node sub-manifest + a cluster.manifest, and
// returns the object store. It mirrors a single-node 3a logical full backup.
func seedLogicalBackup(t *testing.T, backupID string, srcN uint64) *objstore.FS {
	t.Helper()
	store, _ := seedLogicalBackupAt(t, t.TempDir(), backupID, srcN)
	return store
}

// seedLogicalBackupAt seeds a logical backup into an FS object store rooted at objDir, returning the store
// and objDir (so callers can build an fs:// restore URL).
func seedLogicalBackupAt(t *testing.T, objDir, backupID string, srcN uint64) (*objstore.FS, string) {
	t.Helper()
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })

	mustPut(t, src, storage.CFSys, []byte(storageIdentityKey), []byte("SOURCE-UUID"))
	mustPut(t, src, storage.CFKVData, kvKey("app", "k1"), []byte("kvval"))
	mustPut(t, src, storage.CFReplData, replDataKey("ns1", "c1", "doc", srcN), []byte("collval"))
	mustPut(t, src, storage.CFGraphData, graph.NodeKey("g1", "n1"), []byte("graphval"))
	mustPut(t, src, storage.CFVectorRaw, vrKey("vc1", "v1"), []byte("vecval"))
	// Raft bookkeeping a real cluster holds (must NOT survive restore — §5.0).
	metaApplied := append(be8(2), subMetaByte)
	metaApplied = append(metaApplied, []byte("applied")...)
	mustPut(t, src, storage.CFReplData, metaApplied, []byte("42"))
	dedup := append(be8(2), 0x03) // subDedup
	dedup = append(dedup, []byte("idem-token")...)
	mustPut(t, src, storage.CFReplData, dedup, []byte("seen"))

	objStore, err := objstore.NewFS(objDir)
	if err != nil {
		t.Fatal(err)
	}
	keyPrefix := backupID + "/nodes/m1"
	if _, err := ExportLogical(src, objStore, keyPrefix, DefaultRegistry(), 1719000000000, Selector{}); err != nil {
		t.Fatalf("ExportLogical: %v", err)
	}
	cm := &ClusterManifest{
		FormatVersion: clusterManifestFormatVersion,
		BackupID:      backupID,
		Planes:        []string{"logical"},
		PerNode:       []PerNodeRef{{MemberID: "m1", Ref: keyPrefix + "/node.manifest.json"}},
		Status:        "COMPLETE",
	}
	if err := WriteClusterManifest(objStore, cm); err != nil {
		t.Fatal(err)
	}
	return objStore, objDir
}

// TestRestoreBootstrapLogicalReshard restores an N=4 logical backup into a fresh N=8 store: KV/graph/
// vector restored, collections data re-routed to N=8, raft bookkeeping absent, identity preserved.
func TestRestoreBootstrapLogicalReshard(t *testing.T) {
	objStore := seedLogicalBackup(t, "bk-clone", 4)

	dst, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	mustPut(t, dst, storage.CFSys, []byte(storageIdentityKey), []byte("DEST-UUID")) // the clone's own identity

	if err := RestoreBootstrapLogical(dst, objStore, "bk-clone", RestoreInfo{CollectionsDataShards: 8, Clone: true}); err != nil {
		t.Fatalf("RestoreBootstrapLogical: %v", err)
	}

	// KV / graph / vector restored verbatim.
	if v, ok, _ := dst.Get(storage.CFKVData, kvKey("app", "k1")); !ok || string(v) != "kvval" {
		t.Fatalf("kv not restored: ok=%v v=%q", ok, v)
	}
	if _, ok, _ := dst.Get(storage.CFGraphData, graph.NodeKey("g1", "n1")); !ok {
		t.Fatal("graph not restored")
	}
	if _, ok, _ := dst.Get(storage.CFVectorRaw, vrKey("vc1", "v1")); !ok {
		t.Fatal("vector not restored")
	}
	// Collections data row re-routed to its N=8 shard.
	if _, ok, _ := dst.Get(storage.CFReplData, replDataKey("ns1", "c1", "doc", 8)); !ok {
		t.Fatal("collections data row not present under the N=8 shard prefix")
	}
	// Raft bookkeeping reset (§5.0).
	metaApplied := append(be8(2), subMetaByte)
	metaApplied = append(metaApplied, []byte("applied")...)
	if _, ok, _ := dst.Get(storage.CFReplData, metaApplied); ok {
		t.Fatal("subMeta applied-index row must be absent after restore")
	}
	// Node identity preserved (not overwritten by the source's).
	if v, _, _ := dst.Get(storage.CFSys, []byte(storageIdentityKey)); string(v) != "DEST-UUID" {
		t.Fatalf("node identity = %q, want DEST-UUID (must not inherit the source's)", v)
	}
}

// TestRestoreBootstrapLogicalSameShapeStripsBookkeeping proves §5.0 holds even for a same-shape restore
// (CollectionsDataShards == 0, verbatim CFReplData): StripRaftBookkeeping still removes subMeta/dedup.
func TestRestoreBootstrapLogicalSameShapeStripsBookkeeping(t *testing.T) {
	objStore := seedLogicalBackup(t, "bk-dr", 4)

	dst, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	if err := RestoreBootstrapLogical(dst, objStore, "bk-dr", RestoreInfo{}); err != nil { // same shape (verbatim)
		t.Fatalf("RestoreBootstrapLogical: %v", err)
	}
	// Data row verbatim under the original N=4 shard.
	if _, ok, _ := dst.Get(storage.CFReplData, replDataKey("ns1", "c1", "doc", 4)); !ok {
		t.Fatal("same-shape: data row should be present under its original shard")
	}
	// Bookkeeping stripped despite no re-shard.
	metaApplied := append(be8(2), subMetaByte)
	metaApplied = append(metaApplied, []byte("applied")...)
	if _, ok, _ := dst.Get(storage.CFReplData, metaApplied); ok {
		t.Fatal("same-shape restore must still strip the subMeta applied-index row (§5.0)")
	}
	dedup := append(be8(2), 0x03)
	dedup = append(dedup, []byte("idem-token")...)
	if _, ok, _ := dst.Get(storage.CFReplData, dedup); ok {
		t.Fatal("same-shape restore must still strip the subDedup row (§5.0)")
	}
}
