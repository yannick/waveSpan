package backup

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// TestRunBootstrapRestoreEndToEnd drives the startup entry point: parse a fs:// restore URL, run the
// bootstrap into a fresh data dir, and assert the store is populated + the one-shot marker is written and
// honoured on a second run.
func TestRunBootstrapRestoreEndToEnd(t *testing.T) {
	objDir := filepath.Join(t.TempDir(), "backups")
	seedLogicalBackupAt(t, objDir, "bk-boot", 4)

	rc, ok, err := ParseRestoreConfig(envMap(map[string]string{
		"WAVESPAN_RESTORE_FROM":   "fs://" + objDir + "/bk-boot",
		"WAVESPAN_RESTORE_INTENT": "clone",
		"WAVESPAN_RESTORE_SHARDS": "8",
	}))
	if err != nil || !ok {
		t.Fatalf("ParseRestoreConfig: ok %v err %v", ok, err)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
	if err := RunBootstrapRestore(dataDir, "m1", rc, nil); err != nil {
		t.Fatalf("RunBootstrapRestore: %v", err)
	}

	// The data dir opens with the restored data + the one-shot marker.
	dst, err := storage.OpenWavesdb(dataDir)
	if err != nil {
		t.Fatalf("open restored dir: %v", err)
	}
	if v, ok, _ := dst.Get(storage.CFKVData, kvKey("app", "k1")); !ok || string(v) != "kvval" {
		t.Fatalf("kv not restored: ok=%v v=%q", ok, v)
	}
	if _, ok, _ := dst.Get(storage.CFReplData, replDataKey("ns1", "c1", "doc", 8)); !ok {
		t.Fatal("collections data not re-routed to N=8")
	}
	marker, ok, _ := dst.Get(storage.CFSys, []byte(restoreMarkerKey))
	if !ok || string(marker) != "bk-boot" {
		t.Fatalf("one-shot marker = %q ok %v, want bk-boot", marker, ok)
	}
	_ = dst.Close()

	// Second run is a no-op (marker present): the source is gone, so if it tried to restore it would error.
	rcGone, _, _ := ParseRestoreConfig(envMap(map[string]string{
		"WAVESPAN_RESTORE_FROM": "fs://" + filepath.Join(t.TempDir(), "absent") + "/bk-boot",
	}))
	if err := RunBootstrapRestore(dataDir, "m1", rcGone, nil); err != nil {
		t.Fatalf("second RunBootstrapRestore should be a skipped no-op, got: %v", err)
	}
}

// TestRunBootstrapRestorePhysicalDR drives the physical same-shape DR path through the startup entry.
func TestRunBootstrapRestorePhysicalDR(t *testing.T) {
	objDir := filepath.Join(t.TempDir(), "backups")
	objStore, err := objstore.NewFS(objDir)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a source store + a full physical backup B0 with a physical cluster.manifest (member m1).
	src, err := storage.OpenWavesdb(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	mustPut(t, src, storage.CFKVData, kvKey("app", "dr"), []byte("drval"))
	if _, err := NewAgent(nil).Export(context.Background(), src, objStore, "B0", "m1", Selector{}, []Plane{PlanePhysical}, 1, nil); err != nil {
		t.Fatalf("export: %v", err)
	}
	writePhysicalClusterManifest(t, objStore, "B0", "")

	rc, _, err := ParseRestoreConfig(envMap(map[string]string{
		"WAVESPAN_RESTORE_FROM":   "fs://" + objDir + "/B0",
		"WAVESPAN_RESTORE_INTENT": "dr",
	}))
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := RunBootstrapRestore(dataDir, "m1", rc, nil); err != nil {
		t.Fatalf("RunBootstrapRestore(physical): %v", err)
	}
	dst, err := storage.OpenWavesdb(dataDir)
	if err != nil {
		t.Fatalf("open restored dir: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if v, ok, _ := dst.Get(storage.CFKVData, kvKey("app", "dr")); !ok || string(v) != "drval" {
		t.Fatalf("physical DR did not restore data: ok=%v v=%q", ok, v)
	}
	if _, ok, _ := dst.Get(storage.CFSys, []byte(restoreMarkerKey)); !ok {
		t.Fatal("physical restore did not write the one-shot marker")
	}
}
