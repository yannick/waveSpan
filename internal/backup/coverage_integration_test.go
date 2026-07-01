package backup

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// newCoverageCoord builds a coordinator wired for the F1 coverage checks: an expected data-shard count, a
// roster whose expected members may exceed the live set, and per-node hosted shards set on the fakes.
func newCoverageCoord(t *testing.T, objStore ObjectStore, nodes map[string]*memberNode, live, members []string, expectedDataShards uint64) *Coordinator {
	t.Helper()
	assignments := map[string]Selector{}
	for _, id := range live {
		assignments[id] = Selector{}
	}
	return NewCoordinator(Config{
		Self:               "m1",
		Meta:               newFakeMetaStore(),
		ObjStore:           objStore,
		Roster:             fakeRoster{live: live, members: members},
		ClientFor:          func(id string) (NodeClient, error) { return nodes[id], nil },
		Assigner:           fakeAssigner{assignments: assignments},
		ExpectedDataShards: expectedDataShards,
	})
}

// TestCoordinator_AllShardsCovered_Complete: every data shard is hosted by a live exporting node and every
// expected member exported → COMPLETE.
func TestCoordinator_AllShardsCovered_Complete(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodes := buildCluster(t, objStore, "m1", "m2")
	nodes["m1"].heldShards = []uint64{collections.FirstDataShard + 0, collections.FirstDataShard + 1}
	nodes["m2"].heldShards = []uint64{collections.FirstDataShard + 2, collections.FirstDataShard + 3}
	coord := newCoverageCoord(t, objStore, nodes, []string{"m1", "m2"}, []string{"m1", "m2"}, 4)

	id, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatalf("ReadClusterManifest: %v", err)
	}
	if cm.Status != "COMPLETE" || len(cm.Gaps) != 0 {
		t.Fatalf("status/gaps = %s/%v, want COMPLETE/[]", cm.Status, cm.Gaps)
	}
}

// TestCoordinator_UncoveredShard_Partial: a data shard hosted by no live node → PARTIAL with that shard gap.
func TestCoordinator_UncoveredShard_Partial(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodes := buildCluster(t, objStore, "m1", "m2")
	nodes["m1"].heldShards = []uint64{collections.FirstDataShard + 0, collections.FirstDataShard + 1}
	nodes["m2"].heldShards = []uint64{collections.FirstDataShard + 2} // shard FirstDataShard+3 (=5) uncovered
	coord := newCoverageCoord(t, objStore, nodes, []string{"m1", "m2"}, []string{"m1", "m2"}, 4)

	id, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatalf("ReadClusterManifest: %v", err)
	}
	if cm.Status != "PARTIAL" {
		t.Fatalf("status = %s, want PARTIAL", cm.Status)
	}
	if !containsString(cm.Gaps, "collections-shard:5") {
		t.Fatalf("gaps = %v, want to include collections-shard:5", cm.Gaps)
	}
}

// TestCoordinator_MissingMember_Partial: an expected member absent from the live set (down at backup time)
// → PARTIAL with a member gap, even when the collections check is disabled.
func TestCoordinator_MissingMember_Partial(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodes := buildCluster(t, objStore, "m1", "m2") // only m1,m2 live; m3 expected but down
	coord := newCoverageCoord(t, objStore, nodes, []string{"m1", "m2"}, []string{"m1", "m2", "m3"}, 0)

	id, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatalf("ReadClusterManifest: %v", err)
	}
	if cm.Status != "PARTIAL" {
		t.Fatalf("status = %s, want PARTIAL", cm.Status)
	}
	if !containsString(cm.Gaps, "member:m3") {
		t.Fatalf("gaps = %v, want to include member:m3", cm.Gaps)
	}
}

// TestPrepareLocal_ReportsHostedShards: the node-internal PrepareLocal derives real held ranges from its
// HostedDataShards closure and returns them as shard tokens — the same []string that travels over the gRPC
// PrepareBackupResult.held_ranges to the coordinator.
func TestPrepareLocal_ReportsHostedShards(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	coord := NewCoordinator(Config{
		Self:             "m1",
		LocalStore:       mem,
		HostedDataShards: func() []uint64 { return []uint64{collections.FirstDataShard + 1, collections.FirstDataShard} },
	})
	res, err := coord.PrepareLocal(context.Background(), &wavespanv1.PrepareBackupRequest{BackupId: "bk", FrontierT: 1})
	if err != nil {
		t.Fatalf("PrepareLocal: %v", err)
	}
	want := []string{shardToken(collections.FirstDataShard), shardToken(collections.FirstDataShard + 1)}
	if got := res.GetHeldRanges(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("held_ranges = %v, want %v (ascending shard tokens)", got, want)
	}
}
