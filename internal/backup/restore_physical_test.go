package backup

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// TestRestoreBootstrapPhysicalChain seeds a source node, takes a full physical backup B0 + an incremental
// B1, then restores the B1 chain (base+delta) into a fresh data dir for the matched node. The restored dir
// opens with all KV/graph/vector/collections data, and collections raft bookkeeping is reset (§5.0).
func TestRestoreBootstrapPhysicalChain(t *testing.T) {
	ctx := context.Background()
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })

	mustPut(t, src, storage.CFSys, []byte(storageIdentityKey), []byte("SRC-UUID"))
	mustPut(t, src, storage.CFKVData, kvKey("app", "k1"), []byte("kvval"))
	mustPut(t, src, storage.CFGraphData, graph.NodeKey("g1", "n1"), []byte("graphval"))
	mustPut(t, src, storage.CFVectorRaw, vrKey("vc1", "v1"), []byte("vecval"))
	mustPut(t, src, storage.CFReplData, replDataKey("ns1", "c1", "doc", 4), []byte("collval"))
	// raft bookkeeping that must be reset after restore
	metaApplied := append(be8(2), subMetaByte)
	metaApplied = append(metaApplied, []byte("applied")...)
	mustPut(t, src, storage.CFReplData, metaApplied, []byte("99"))

	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agent := NewAgent(nil)
	const ft = int64(1719000000000)

	// B0 full physical + its cluster.manifest.
	res0, err := agent.Export(ctx, src, objStore, "B0", "m1", Selector{}, []Plane{PlanePhysical}, ft, nil)
	if err != nil {
		t.Fatalf("export B0: %v", err)
	}
	writePhysicalClusterManifest(t, objStore, "B0", "")

	// Mutate + incremental B1 (parent B0) + its cluster.manifest.
	mustPut(t, src, storage.CFKVData, kvKey("app", "k2"), []byte("kvval2"))
	if _, err := agent.Export(ctx, src, objStore, "B1", "m1", Selector{}, []Plane{PlanePhysical}, ft, res0.Checkpoint); err != nil {
		t.Fatalf("export B1: %v", err)
	}
	writePhysicalClusterManifest(t, objStore, "B1", "B0")

	// Restore the B1 chain into a fresh data dir.
	dataDir := filepath.Join(t.TempDir(), "restored")
	if err := RestoreBootstrapPhysical(ctx, objStore, "B1", "m1", dataDir); err != nil {
		t.Fatalf("RestoreBootstrapPhysical: %v", err)
	}

	// The restored dir opens with all data (base + increment).
	dst, err := storage.OpenWavesdb(dataDir)
	if err != nil {
		t.Fatalf("open restored dir: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	for _, c := range []struct {
		cf  storage.ColumnFamily
		key []byte
		val string
	}{
		{storage.CFKVData, kvKey("app", "k1"), "kvval"},
		{storage.CFKVData, kvKey("app", "k2"), "kvval2"}, // from the increment
		{storage.CFGraphData, graph.NodeKey("g1", "n1"), "graphval"},
		{storage.CFVectorRaw, vrKey("vc1", "v1"), "vecval"},
		{storage.CFReplData, replDataKey("ns1", "c1", "doc", 4), "collval"},
	} {
		v, ok, err := dst.Get(c.cf, c.key)
		if err != nil || !ok || string(v) != c.val {
			t.Fatalf("restored %s: ok=%v v=%q err=%v, want %q", c.cf.Name(), ok, v, err, c.val)
		}
	}
	// Collections raft bookkeeping reset.
	if _, ok, _ := dst.Get(storage.CFReplData, metaApplied); ok {
		t.Fatal("collections subMeta applied-index must be reset after physical restore (§5.0)")
	}
}

// writePhysicalClusterManifest writes a minimal physical cluster.manifest (one node m1) for chain tests.
func writePhysicalClusterManifest(t *testing.T, objStore ObjectStore, backupID, parent string) {
	t.Helper()
	cm := &ClusterManifest{
		FormatVersion: clusterManifestFormatVersion,
		BackupID:      backupID,
		Parent:        parent,
		Planes:        []string{"physical"},
		SourceTopology: []TopologyEntry{{MemberID: "m1", StorageUUID: "SRC-UUID"}},
		PerNode:       []PerNodeRef{{MemberID: "m1", PhysicalManifest: PhysicalManifestKey(backupID, "m1")}},
		Status:        "COMPLETE",
	}
	if err := WriteClusterManifest(objStore, cm); err != nil {
		t.Fatal(err)
	}
}
