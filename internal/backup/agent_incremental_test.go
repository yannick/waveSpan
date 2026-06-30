package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// countKlogs returns how many SSTable klog objects were uploaded under prefix (one per exported table).
func countKlogs(t *testing.T, store *objstore.FS, prefix string) int {
	t.Helper()
	keys, err := store.List(prefix)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	n := 0
	for _, k := range keys {
		if strings.HasSuffix(k, ".klog") {
			n++
		}
	}
	return n
}

// TestAgentPhysicalFullAndIncremental drives the physical plane against a real wavesdb store: a full
// export records a CheckpointManifest (>=1 table) + GlobalSeq in a per-node physical sub-manifest and
// uploads every table; a follow-up incremental (prior checkpoint as parent) records the FULL cumulative
// table set but uploads ONLY the new SSTable(s), with a higher GlobalSeq.
func TestAgentPhysicalFullAndIncremental(t *testing.T) {
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	mustPut(t, src, storage.CFSys, []byte(storageIdentityKey), []byte("uuid-m1"))
	mustPut(t, src, storage.CFKVData, []byte("k1"), []byte("v1"))

	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a := NewAgent(nil)
	const frontierT = int64(1719720000000)

	// Full physical backup B0.
	res0, err := a.Export(ctx, src, objStore, "B0", "m1", Selector{}, []Plane{PlanePhysical}, frontierT, nil)
	if err != nil {
		t.Fatalf("full physical Export: %v", err)
	}
	if res0.Checkpoint == nil || res0.PhysicalGlobalSeq == 0 || len(res0.Checkpoint.Tables) == 0 {
		t.Fatalf("full export checkpoint = %+v, want non-nil with tables + GlobalSeq", res0.Checkpoint)
	}
	if res0.StorageUUID != "uuid-m1" {
		t.Fatalf("full export StorageUUID = %q, want uuid-m1", res0.StorageUUID)
	}
	// Physical sub-manifest persisted, listing the full table set.
	pm0, err := ReadPhysicalManifest(objStore, res0.PhysicalManifestKey)
	if err != nil {
		t.Fatalf("ReadPhysicalManifest B0: %v", err)
	}
	if len(pm0.Tables) != len(res0.Checkpoint.Tables) || pm0.GlobalSeq != res0.PhysicalGlobalSeq || pm0.ParentGlobalSeq != 0 {
		t.Fatalf("B0 physical manifest = %+v, want full set, parent 0", pm0)
	}
	n0 := len(res0.Checkpoint.Tables)
	uploaded0 := countKlogs(t, objStore, "B0/nodes/m1/physical")
	if uploaded0 != n0 {
		t.Fatalf("full export uploaded %d klogs, want all %d", uploaded0, n0)
	}

	// Mutate + incremental backup B1 against B0's checkpoint.
	mustPut(t, src, storage.CFKVData, []byte("k2"), []byte("v2"))
	res1, err := a.Export(ctx, src, objStore, "B1", "m1", Selector{}, []Plane{PlanePhysical}, frontierT, res0.Checkpoint)
	if err != nil {
		t.Fatalf("incremental physical Export: %v", err)
	}
	n1 := len(res1.Checkpoint.Tables)
	if n1 <= n0 {
		t.Fatalf("incremental table set = %d, want > full %d", n1, n0)
	}
	if res1.PhysicalGlobalSeq <= res0.PhysicalGlobalSeq {
		t.Fatalf("incremental GlobalSeq %d, want > full %d", res1.PhysicalGlobalSeq, res0.PhysicalGlobalSeq)
	}
	// Only the NEW SSTable(s) were uploaded into B1's prefix (parent ids skipped).
	uploaded1 := countKlogs(t, objStore, "B1/nodes/m1/physical")
	if uploaded1 != n1-n0 {
		t.Fatalf("incremental uploaded %d klogs, want only the %d new (full set %d, parent %d)", uploaded1, n1-n0, n1, n0)
	}
	pm1, err := ReadPhysicalManifest(objStore, res1.PhysicalManifestKey)
	if err != nil {
		t.Fatalf("ReadPhysicalManifest B1: %v", err)
	}
	if pm1.ParentGlobalSeq != res0.PhysicalGlobalSeq || len(pm1.Tables) != n1 {
		t.Fatalf("B1 physical manifest = %+v, want parent %d + full set %d", pm1, res0.PhysicalGlobalSeq, n1)
	}
}

// TestAgentPhysicalRequiresWavesdbStore proves a physical export against a non-wavesdb store (MemStore)
// fails clearly rather than silently producing nothing.
func TestAgentPhysicalRequiresWavesdbStore(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	objStore, _ := objstore.NewFS(t.TempDir())
	a := NewAgent(nil)
	_, err := a.Export(context.Background(), mem, objStore, "B0", "m1", Selector{}, []Plane{PlanePhysical}, 0, nil)
	if err == nil || !strings.Contains(err.Error(), "wavesdb-backed store") {
		t.Fatalf("physical export on MemStore err = %v, want a clear wavesdb-store error", err)
	}
}
