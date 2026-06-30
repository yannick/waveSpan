package backup

import (
	"bytes"
	"context"
	"testing"

	"wavesdb/objstore"
)

// TestReconcileOrphans seeds a live backup's objects plus orphan objects (a backup id with no live
// intent — failed-export debris) and asserts only the orphans are deleted.
func TestReconcileOrphans(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	put := func(k string) {
		if err := objStore.Put(k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}

	// A live backup with an intent.
	if err := PutIntent(ctx, store, &BackupIntent{BackupID: "live-bk", Status: StatusComplete}); err != nil {
		t.Fatal(err)
	}
	liveKeys := []string{"live-bk/cluster.manifest.json", "live-bk/nodes/m1/cf/kv_data"}
	for _, k := range liveKeys {
		put(k)
	}
	// Orphan objects: a backup id with no intent (failed/abandoned export).
	orphanKeys := []string{"orphan-bk/cluster.manifest.json", "orphan-bk/nodes/m1/physical/cf_kv/9.klog"}
	for _, k := range orphanKeys {
		put(k)
	}

	deleted, err := ReconcileOrphans(ctx, store, objStore, "")
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}
	if len(deleted) != len(orphanKeys) {
		t.Fatalf("deleted %d objects %v, want %d orphans", len(deleted), deleted, len(orphanKeys))
	}

	// Orphans gone, live objects intact.
	for _, k := range orphanKeys {
		if ok, _ := objStore.Exists(k); ok {
			t.Fatalf("orphan %q not deleted", k)
		}
	}
	for _, k := range liveKeys {
		if ok, _ := objStore.Exists(k); !ok {
			t.Fatalf("live object %q wrongly deleted", k)
		}
	}

	// Idempotent: a second pass finds nothing to delete.
	if deleted2, err := ReconcileOrphans(ctx, store, objStore, ""); err != nil || len(deleted2) != 0 {
		t.Fatalf("second ReconcileOrphans = %v err %v, want none", deleted2, err)
	}
}
