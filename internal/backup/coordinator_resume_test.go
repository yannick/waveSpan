package backup

import (
	"context"
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// TestCoordinatorResumeAfterPrepare simulates coordinator loss after the prepare phase: the durable
// intent survives in the (shared) meta store, a brand-new coordinator resumes from it, completes the
// export + commit, and the backup reaches COMPLETE. Re-running resume on the finished backup is a safe
// no-op (idempotent), proving a crashed-and-retried coordinator never corrupts a committed backup.
func TestCoordinatorResumeAfterPrepare(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore() // the durable meta shard — survives coordinator loss
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	assigner := fakeAssigner{assignments: map[string]Selector{"m1": {}, "m2": {}, "m3": {}}}

	ctx := context.Background()

	// Coordinator #1 crashes right after prepare (persists Phase=EXPORT, exports nothing).
	coord1 := newCoord(t, objStore, meta, nodes, assigner)
	coord1.failAfterPhase = PhasePrepare
	id, err := coord1.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	st, _ := coord1.BackupStatus(ctx, id)
	if st.GetStatus() != wavespanv1.BackupStatus_BACKUP_RUNNING || st.GetPhase() != wavespanv1.BackupPhase_BACKUP_PHASE_EXPORT {
		t.Fatalf("after crash: status/phase = %v/%v, want RUNNING/EXPORT", st.GetStatus(), st.GetPhase())
	}
	for id, n := range nodes {
		if n.prepares != 1 || n.exports != 0 {
			t.Fatalf("node %s after crash: prepares=%d exports=%d, want 1/0", id, n.prepares, n.exports)
		}
	}

	// Coordinator #2 (fresh, same durable meta + object store) resumes from the intent and finishes.
	coord2 := newCoord(t, objStore, meta, nodes, assigner)
	if err := coord2.resume(ctx, id); err != nil {
		t.Fatalf("resume: %v", err)
	}
	st, _ = coord2.BackupStatus(ctx, id)
	if st.GetStatus() != wavespanv1.BackupStatus_BACKUP_COMPLETE {
		t.Fatalf("after resume: status = %v, want COMPLETE", st.GetStatus())
	}
	for id, n := range nodes {
		if n.exports != 1 {
			t.Fatalf("node %s after resume: exports=%d, want 1", id, n.exports)
		}
	}
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil || cm.Status != "COMPLETE" || len(cm.PerNode) != 3 {
		t.Fatalf("cluster manifest after resume = %+v err=%v", cm, err)
	}

	// A second resume of the completed backup is idempotent: no error, still COMPLETE, no re-export.
	if err := coord2.resume(ctx, id); err != nil {
		t.Fatalf("idempotent re-resume: %v", err)
	}
	for id, n := range nodes {
		if n.exports != 1 {
			t.Fatalf("node %s re-resume re-exported: exports=%d, want 1", id, n.exports)
		}
	}
}
