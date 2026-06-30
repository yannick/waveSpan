package backup

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// TestAgentExport seeds a node's KV + collections data, runs the agent export to an FS object store, and
// asserts the per-CF objects and the per-node sub-manifest are written under <backupID>/nodes/<member>/
// with the expected counts.
func TestAgentExport(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })

	mustPut(t, mem, storage.CFKVData, kvKey("ns1", "k1"), []byte("v1"))
	mustPut(t, mem, storage.CFKVData, kvKey("ns1", "k2"), []byte("v2"))
	mustPut(t, mem, storage.CFReplData, replDataKey("ns1", "c", "row", 4), []byte("set"))

	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	a := NewAgent(nil)

	const frontierT = int64(1719720000000)
	pr, err := a.Prepare(ctx, mem, "bk-1", frontierT, []string{"ns1"})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if pr.GlobalSeq != uint64(frontierT) {
		t.Fatalf("Prepare GlobalSeq = %d, want %d", pr.GlobalSeq, frontierT)
	}

	res, err := a.Export(ctx, mem, objStore, "bk-1", "m1", Selector{}, []Plane{PlaneLogical}, frontierT, nil)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Two authoritative CFs got objects (kv_data + repl_data).
	if res.Objects != 2 {
		t.Fatalf("Export Objects = %d, want 2", res.Objects)
	}
	if res.SubManifestKey != "bk-1/nodes/m1/node.manifest.json" {
		t.Fatalf("SubManifestKey = %q", res.SubManifestKey)
	}
	if ok, _ := objStore.Exists("bk-1/nodes/m1/node.manifest.json"); !ok {
		t.Fatal("per-node sub-manifest object missing")
	}
	if ok, _ := objStore.Exists("bk-1/nodes/m1/cf/kv_data"); !ok {
		t.Fatal("kv_data CF object missing")
	}

	// The decoded manifest carries the per-CF counts.
	if res.Manifest.CFEntryCount("kv_data") != 2 {
		t.Fatalf("manifest kv_data count = %d, want 2", res.Manifest.CFEntryCount("kv_data"))
	}
	if res.Manifest.CFEntryCount("repl_data") != 1 {
		t.Fatalf("manifest repl_data count = %d, want 1", res.Manifest.CFEntryCount("repl_data"))
	}

	// Re-export is idempotent (same keys, no error) — the resumability guarantee.
	if _, err := a.Export(ctx, mem, objStore, "bk-1", "m1", Selector{}, []Plane{PlaneLogical}, frontierT, nil); err != nil {
		t.Fatalf("re-export: %v", err)
	}
}
