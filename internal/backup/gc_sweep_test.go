package backup

import (
	"bytes"
	"context"
	"testing"

	"wavesdb/objstore"
)

// TestSweepIntents covers the lifecycle sweep: a RUNNING intent past its lease becomes FAILED (with a
// retention deadline set); a terminal intent past its retention is deleted (intent + objects); not-yet-due
// intents are untouched; and a second sweep is a no-op (idempotent).
func TestSweepIntents(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	const now = int64(1_000_000)
	const retain = int64(5_000)
	past := now - 1_000
	future := now + 1_000_000

	seedObj := func(key string) {
		if err := objStore.Put(key, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}
	put := func(in *BackupIntent) {
		if err := PutIntent(ctx, store, in); err != nil {
			t.Fatal(err)
		}
	}

	put(&BackupIntent{BackupID: "run-expired", Status: StatusRunning, LeaseDeadlineMs: past})
	put(&BackupIntent{BackupID: "run-active", Status: StatusRunning, LeaseDeadlineMs: future})
	put(&BackupIntent{BackupID: "done-old", Status: StatusComplete, RetainUntilMs: past})
	seedObj("done-old/cluster.manifest.json")
	put(&BackupIntent{BackupID: "done-fresh", Status: StatusComplete, RetainUntilMs: future})
	seedObj("done-fresh/cluster.manifest.json")

	stats, err := SweepIntents(ctx, store, objStore, now, retain)
	if err != nil {
		t.Fatalf("SweepIntents: %v", err)
	}
	if stats.Failed != 1 || stats.Deleted != 1 {
		t.Fatalf("stats = %+v, want Failed 1 Deleted 1", stats)
	}

	// RUNNING past lease → FAILED + retention scheduled.
	got, found, _ := GetIntent(ctx, store, "run-expired")
	if !found || got.Status != StatusFailed || got.RetainUntilMs != now+retain {
		t.Fatalf("run-expired = %+v, want FAILED retain %d", got, now+retain)
	}
	// RUNNING not yet expired → untouched.
	if got, _, _ := GetIntent(ctx, store, "run-active"); got.Status != StatusRunning {
		t.Fatalf("run-active status = %v, want RUNNING", got.Status)
	}
	// Terminal past retention → intent + objects gone.
	if _, found, _ := GetIntent(ctx, store, "done-old"); found {
		t.Fatalf("done-old intent still present")
	}
	if ok, _ := objStore.Exists("done-old/cluster.manifest.json"); ok {
		t.Fatalf("done-old objects not deleted")
	}
	// Terminal within retention → kept.
	if _, found, _ := GetIntent(ctx, store, "done-fresh"); !found {
		t.Fatalf("done-fresh intent wrongly deleted")
	}
	if ok, _ := objStore.Exists("done-fresh/cluster.manifest.json"); !ok {
		t.Fatalf("done-fresh objects wrongly deleted")
	}

	// Idempotent: a second sweep at the same time changes nothing (the freshly-FAILED intent now has a
	// future retention deadline).
	stats2, err := SweepIntents(ctx, store, objStore, now, retain)
	if err != nil {
		t.Fatalf("second SweepIntents: %v", err)
	}
	if stats2.Failed != 0 || stats2.Deleted != 0 {
		t.Fatalf("second sweep stats = %+v, want all zero", stats2)
	}
}

// TestSweepRetentionDefersToLiveChild proves the retention deletion is chain-aware: a base past its
// retention is NOT deleted while a live incremental child still depends on it.
func TestSweepRetentionDefersToLiveChild(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()
	objStore, _ := objstore.NewFS(t.TempDir())

	const now = int64(1_000_000)
	past := now - 1_000
	future := now + 1_000_000

	_ = PutIntent(ctx, store, &BackupIntent{BackupID: "B0", Status: StatusComplete, RetainUntilMs: past})
	_ = PutIntent(ctx, store, &BackupIntent{BackupID: "B1", Status: StatusComplete, Parent: "B0", RetainUntilMs: future})

	if _, err := SweepIntents(ctx, store, objStore, now, 5_000); err != nil {
		t.Fatalf("SweepIntents: %v", err)
	}
	if _, found, _ := GetIntent(ctx, store, "B0"); !found {
		t.Fatalf("B0 deleted despite live child B1")
	}
}
