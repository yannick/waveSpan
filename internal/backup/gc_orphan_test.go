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

// hidingMetaStore omits one id from ListBlobs (so it's absent from the live snapshot) while GetBlob
// still returns it — simulating a backup that Begins AFTER the live snapshot is taken but before objects
// are reconciled (the TOCTOU window).
type hidingMetaStore struct {
	MetaStore
	hidden string
}

func (h *hidingMetaStore) ListBlobs(ctx context.Context) (map[string][]byte, error) {
	m, err := h.MetaStore.ListBlobs(ctx)
	if err != nil {
		return nil, err
	}
	delete(m, h.hidden)
	return m, nil
}

// TestReconcileOrphansTOCTOU proves an in-flight backup (intent created after the live snapshot, so
// absent from it) is NOT collected: the fresh GetIntent re-check sees it and its objects survive, while
// a genuinely intent-less backup id is still deleted.
func TestReconcileOrphansTOCTOU(t *testing.T) {
	ctx := context.Background()
	base := newFakeMetaStore()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	put := func(k string) {
		if err := objStore.Put(k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}

	// "racing-bk" has an intent (it exists fresh) but is hidden from the live snapshot.
	if err := PutIntent(ctx, base, &BackupIntent{BackupID: "racing-bk", Status: StatusRunning}); err != nil {
		t.Fatal(err)
	}
	put("racing-bk/nodes/m1/cf/kv_data")
	// "dead-bk" has no intent at all — a true orphan.
	put("dead-bk/cluster.manifest.json")

	store := &hidingMetaStore{MetaStore: base, hidden: "racing-bk"}
	deleted, err := ReconcileOrphans(ctx, store, objStore, "")
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}

	if ok, _ := objStore.Exists("racing-bk/nodes/m1/cf/kv_data"); !ok {
		t.Fatalf("in-flight backup's objects were collected (TOCTOU): %v deleted", deleted)
	}
	if ok, _ := objStore.Exists("dead-bk/cluster.manifest.json"); ok {
		t.Fatalf("true orphan dead-bk not deleted")
	}
}
