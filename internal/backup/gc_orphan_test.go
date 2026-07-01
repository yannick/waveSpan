package backup

import (
	"bytes"
	"context"
	"testing"
	"time"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// recOpts builds ReconcileOptions for tests with the age grace OFF (objects of any age reapable) — used
// by tests that assert orphan deletion without faking time.
func recOpts(objStore ObjectStore) ReconcileOptions {
	return ReconcileOptions{StoreFor: toStore(objStore), DefaultStore: objStore}
}

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
	if err := PutIntent(ctx, store, &Intent{BackupID: "live-bk", Status: StatusComplete}); err != nil {
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

	deleted, err := ReconcileOrphans(ctx, store, recOpts(objStore))
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
	if deleted2, err := ReconcileOrphans(ctx, store, recOpts(objStore)); err != nil || len(deleted2) != 0 {
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

// ListBlobsStale hides the id too — ReconcileOrphans's enumeration scan now uses the stale list, so the
// TOCTOU window (id absent from the scan snapshot, present via the fresh GetBlob re-check) is simulated here.
func (h *hidingMetaStore) ListBlobsStale(ctx context.Context) (map[string][]byte, error) {
	return h.ListBlobs(ctx)
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

	// A visible live intent keeps the catalog NON-empty (so the empty-catalog fail-safe isn't what's
	// under test here — the TOCTOU re-check is).
	if err := PutIntent(ctx, base, &Intent{BackupID: "keep-bk", Status: StatusComplete}); err != nil {
		t.Fatal(err)
	}
	put("keep-bk/cluster.manifest.json")
	// "racing-bk" has an intent (it exists fresh) but is hidden from the live snapshot.
	if err := PutIntent(ctx, base, &Intent{BackupID: "racing-bk", Status: StatusRunning}); err != nil {
		t.Fatal(err)
	}
	put("racing-bk/nodes/m1/cf/kv_data")
	// "dead-bk" has no intent at all — a true orphan.
	put("dead-bk/cluster.manifest.json")

	store := &hidingMetaStore{MetaStore: base, hidden: "racing-bk"}
	deleted, err := ReconcileOrphans(ctx, store, recOpts(objStore))
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}

	if ok, _ := objStore.Exists("racing-bk/nodes/m1/cf/kv_data"); !ok {
		t.Fatalf("in-flight backup's objects were collected (TOCTOU): %v deleted", deleted)
	}
	if ok, _ := objStore.Exists("dead-bk/cluster.manifest.json"); ok {
		t.Fatalf("true orphan dead-bk not deleted")
	}
	if ok, _ := objStore.Exists("keep-bk/cluster.manifest.json"); !ok {
		t.Fatalf("live keep-bk wrongly deleted")
	}
}

// TestReconcileOrphansKeepsLiveCoordinatorBackup writes a backup through the REAL coordinator/agent export
// layout (<id>/cluster.manifest.json + <id>/nodes/<member>/cf/...) with its intent live, then runs
// reconciliation — which must delete NOTHING. (This is the layout the production bug reaped.)
func TestReconcileOrphansKeepsLiveCoordinatorBackup(t *testing.T) {
	ctx := context.Background()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1")
	coord := newCoord(t, objStore, meta, nodes, fakeAssigner{assignments: map[string]Selector{"m1": {}}})

	id, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	manifestKey := id + "/cluster.manifest.json"
	if ok, _ := objStore.Exists(manifestKey); !ok {
		t.Fatalf("expected the real export to write %q", manifestKey)
	}

	deleted, err := ReconcileOrphans(ctx, meta, recOpts(objStore))
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("reconcile DELETED live backup objects: %v", deleted)
	}
	if ok, _ := objStore.Exists(manifestKey); !ok {
		t.Fatalf("DATA LOSS: live backup manifest %q was reaped", manifestKey)
	}
}

// TestReconcileOrphansEmptyCatalogReapsNothing is the data-loss guard: an EMPTY intent catalog (e.g. a
// node whose meta-shard view is empty) must reap NOTHING even with objects present — an empty catalog is
// never "everything is an orphan".
func TestReconcileOrphansEmptyCatalogReapsNothing(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore() // no intents at all
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"bk-1/cluster.manifest.json", "bk-1/nodes/m1/cf/kv_data"}
	for _, k := range keys {
		if err := objStore.Put(k, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := ReconcileOrphans(ctx, store, recOpts(objStore))
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("empty-catalog reconcile reaped objects (data loss): %v", deleted)
	}
	for _, k := range keys {
		if ok, _ := objStore.Exists(k); !ok {
			t.Fatalf("DATA LOSS: object %q reaped against an empty catalog", k)
		}
	}
}

// TestReconcileOrphansAgeGrace proves the age grace: with a NON-EMPTY catalog, a genuine orphan OLDER than
// the grace is still reaped, but a freshly-written orphan (younger than the grace) is kept — so a sweep
// racing a just-finished backup cannot destroy it.
func TestReconcileOrphansAgeGrace(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A live intent makes the catalog non-empty (so the empty-catalog guard isn't what's tested here).
	if err := PutIntent(ctx, store, &Intent{BackupID: "live-bk", Status: StatusComplete}); err != nil {
		t.Fatal(err)
	}
	if err := objStore.Put("live-bk/cluster.manifest.json", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatal(err)
	}
	// A genuine orphan (no intent), written now.
	if err := objStore.Put("orphan-bk/cluster.manifest.json", bytes.NewReader([]byte("x")), 1); err != nil {
		t.Fatal(err)
	}
	const graceMs = int64(60 * 60 * 1000) // 1h
	realNow := time.Now().UnixMilli()

	// Young orphan (now): age < grace → KEPT.
	opt := ReconcileOptions{StoreFor: toStore(objStore), DefaultStore: objStore, NowMs: realNow, GraceMs: graceMs}
	if deleted, err := ReconcileOrphans(ctx, store, opt); err != nil || len(deleted) != 0 {
		t.Fatalf("young orphan: deleted %v err %v, want none kept by grace", deleted, err)
	}
	if ok, _ := objStore.Exists("orphan-bk/cluster.manifest.json"); !ok {
		t.Fatalf("young orphan reaped despite age grace")
	}

	// Same orphan seen 2h later: age > grace → REAPED; the live backup is untouched.
	opt.NowMs = realNow + 2*graceMs
	if _, err := ReconcileOrphans(ctx, store, opt); err != nil {
		t.Fatalf("ReconcileOrphans (aged): %v", err)
	}
	if ok, _ := objStore.Exists("orphan-bk/cluster.manifest.json"); ok {
		t.Fatalf("old orphan not reaped after the grace elapsed")
	}
	if ok, _ := objStore.Exists("live-bk/cluster.manifest.json"); !ok {
		t.Fatalf("live backup wrongly reaped")
	}
}
