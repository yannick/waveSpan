package backup

import (
	"bytes"
	"context"
	"testing"

	"wavesdb/objstore"
)

// seedBackup writes an intent + a couple of objects under <id>/ to simulate a committed backup.
func seedBackup(ctx context.Context, t *testing.T, store MetaStore, objStore ObjectStore, id, parent string) {
	t.Helper()
	if err := PutIntent(ctx, store, &Intent{BackupID: id, Status: StatusComplete, Parent: parent, Planes: []Plane{PlanePhysical}}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{id + "/cluster.manifest.json", id + "/nodes/m1/physical/cf_kv_data/1.klog"} {
		if err := objStore.Put(k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}
}

func objExists(t *testing.T, objStore ObjectStore, key string) bool {
	t.Helper()
	ok, err := objStore.Exists(key)
	if err != nil {
		t.Fatalf("Exists(%q): %v", key, err)
	}
	return ok
}

// TestDeleteBackupChainAware covers the chain-aware delete: deleting a base with a live child is refused;
// deleting the leaf removes only its objects; force cascades; unknown id reports deleted=false.
func TestDeleteBackupChainAware(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedBackup(ctx, t, store, objStore, "B0", "")
	seedBackup(ctx, t, store, objStore, "B1", "B0") // incremental child of B0

	// Deleting the base with a live child is refused (no force).
	if deleted, err := DeleteBackup(ctx, store, objStore, "B0", false); err == nil || deleted {
		t.Fatalf("DeleteBackup(B0,false) = %v err %v, want refusal", deleted, err)
	}
	if _, found, _ := GetIntent(ctx, store, "B0"); !found {
		t.Fatalf("B0 intent removed despite refusal")
	}
	if !objExists(t, objStore, "B0/nodes/m1/physical/cf_kv_data/1.klog") {
		t.Fatalf("B0 objects removed despite refusal")
	}

	// Deleting the leaf removes ONLY its intent + objects; the base's objects survive (chain integrity).
	if deleted, err := DeleteBackup(ctx, store, objStore, "B1", false); err != nil || !deleted {
		t.Fatalf("DeleteBackup(B1,false) = %v err %v, want true nil", deleted, err)
	}
	if _, found, _ := GetIntent(ctx, store, "B1"); found {
		t.Fatalf("B1 intent still present after delete")
	}
	if objExists(t, objStore, "B1/nodes/m1/physical/cf_kv_data/1.klog") {
		t.Fatalf("B1 objects not deleted")
	}
	if !objExists(t, objStore, "B0/nodes/m1/physical/cf_kv_data/1.klog") {
		t.Fatalf("B0 objects wrongly deleted by B1 removal")
	}

	// Now B0 has no live child → deletable.
	if deleted, err := DeleteBackup(ctx, store, objStore, "B0", false); err != nil || !deleted {
		t.Fatalf("DeleteBackup(B0,false) after child gone = %v err %v, want true nil", deleted, err)
	}
	if objExists(t, objStore, "B0/cluster.manifest.json") {
		t.Fatalf("B0 objects not deleted")
	}

	// Unknown id → deleted=false, no error.
	if deleted, err := DeleteBackup(ctx, store, objStore, "ghost", false); err != nil || deleted {
		t.Fatalf("DeleteBackup(ghost) = %v err %v, want false nil", deleted, err)
	}
}

// TestDeleteBackupForceCascade proves force deletes a base and all its dependent children.
func TestDeleteBackupForceCascade(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, _ := objstore.NewFS(t.TempDir())
	seedBackup(ctx, t, store, objStore, "B0", "")
	seedBackup(ctx, t, store, objStore, "B1", "B0")
	seedBackup(ctx, t, store, objStore, "B2", "B1")

	if deleted, err := DeleteBackup(ctx, store, objStore, "B0", true); err != nil || !deleted {
		t.Fatalf("force DeleteBackup(B0) = %v err %v, want true nil", deleted, err)
	}
	for _, id := range []string{"B0", "B1", "B2"} {
		if _, found, _ := GetIntent(ctx, store, id); found {
			t.Fatalf("%s intent survived force cascade", id)
		}
		if objExists(t, objStore, id+"/cluster.manifest.json") {
			t.Fatalf("%s objects survived force cascade", id)
		}
	}
}
