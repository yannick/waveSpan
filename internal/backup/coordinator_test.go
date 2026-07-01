package backup

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

type fakeRoster struct {
	live    []string
	members []string // expected (non-forgotten) members; defaults to live when nil
}

func (r fakeRoster) Live() []string { return r.live }

func (r fakeRoster) Members() []string {
	if r.members == nil {
		return r.live
	}
	return r.members
}

type fakeAssigner struct {
	assignments map[string]Selector
	gaps        []string
}

func (a fakeAssigner) Assign(_ []string) (map[string]Selector, []string) {
	return a.assignments, a.gaps
}

// memberNode is an in-process NodeClient backed by a real Agent over the member's own store, writing to
// the shared object store — so the coordinator's prepare/export fan-out produces real objects.
type memberNode struct {
	id         string
	store      storage.LocalStore
	objStore   ObjectStore
	agent      *Agent
	heldShards []uint64 // data shards this node hosts (F1 coverage); nil = report none
	prepares   int
	exports    int
}

func (n *memberNode) Prepare(ctx context.Context, backupID string, frontierT int64) (PrepareResult, error) {
	n.prepares++
	// Mirror the production PrepareLocal: a node reports its hosted data shards as "shard:<id>" tokens.
	return n.agent.Prepare(ctx, n.store, backupID, frontierT, formatHeldShards(n.heldShards))
}

func (n *memberNode) Export(ctx context.Context, req ExportRequest) (ExportResult, error) {
	n.exports++
	objStore := n.objStore
	if req.ObjStore != nil { // honour the coordinator's resolved per-backup destination
		objStore = req.ObjStore
	}
	// Node-side parent resolution (Phase 3c Task 0): each node resolves its own parent checkpoint from the
	// destination store, mirroring the production ExportLocal path.
	parentCkpt, err := resolveParentCheckpoint(objStore, req.ParentBackupID, req.MemberID)
	if err != nil {
		return ExportResult{}, err
	}
	return n.agent.Export(ctx, n.store, objStore, req.BackupID, req.MemberID, req.Assignment, req.Planes, req.FrontierT, parentCkpt)
}

// buildCluster seeds count members, each with one namespace of KV+collections data, sharing objStore.
func buildCluster(t *testing.T, objStore ObjectStore, ids ...string) map[string]*memberNode {
	t.Helper()
	nodes := map[string]*memberNode{}
	for _, id := range ids {
		mem := storage.NewMemStore()
		t.Cleanup(func() { _ = mem.Close() })
		mustPut(t, mem, storage.CFSys, []byte(storageIdentityKey), []byte("uuid-"+id))
		mustPut(t, mem, storage.CFKVData, kvKey("ns-"+id, "k"), []byte("v-"+id))
		mustPut(t, mem, storage.CFReplData, replDataKey("ns-"+id, "c", "row", 4), []byte("set"))
		nodes[id] = &memberNode{id: id, store: mem, objStore: objStore, agent: NewAgent(nil)}
	}
	return nodes
}

func newCoord(t *testing.T, objStore ObjectStore, meta MetaStore, nodes map[string]*memberNode, assigner Assigner) *Coordinator {
	t.Helper()
	live := make([]string, 0, len(nodes))
	for id := range nodes {
		live = append(live, id)
	}
	return NewCoordinator(Config{
		Self:      "m1",
		Meta:      meta,
		ObjStore:  objStore,
		Roster:    fakeRoster{live: live},
		ClientFor: func(id string) (NodeClient, error) { return nodes[id], nil },
		Assigner:  assigner,
	})
}

// TestCoordinatorFullBackupComplete drives assign→prepare→export→commit across 3 fake nodes and asserts
// the backup reaches COMPLETE, every node is exported, and a cluster.manifest references each node's
// sub-manifest.
func TestCoordinatorFullBackupComplete(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	assigner := fakeAssigner{assignments: map[string]Selector{"m1": {}, "m2": {}, "m3": {}}}
	coord := newCoord(t, objStore, meta, nodes, assigner)

	ctx := context.Background()
	id, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}

	st, err := coord.BackupStatus(ctx, id)
	if err != nil {
		t.Fatalf("BackupStatus: %v", err)
	}
	if st.GetStatus() != wavespanv1.BackupStatus_BACKUP_COMPLETE {
		t.Fatalf("status = %v, want COMPLETE", st.GetStatus())
	}
	if st.GetOverallPct() != 100 {
		t.Fatalf("overall pct = %v, want 100", st.GetOverallPct())
	}
	if len(st.GetPerNode()) != 3 {
		t.Fatalf("per_node = %d, want 3", len(st.GetPerNode()))
	}
	for _, np := range st.GetPerNode() {
		if !np.GetDone() || np.GetObjects() == 0 {
			t.Fatalf("node %s not done/empty: %+v", np.GetMemberId(), np)
		}
	}

	// Every node was prepared and exported exactly once.
	for id, n := range nodes {
		if n.prepares != 1 || n.exports != 1 {
			t.Fatalf("node %s prepares=%d exports=%d, want 1/1", id, n.prepares, n.exports)
		}
	}

	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatalf("ReadClusterManifest: %v", err)
	}
	if cm.Status != "COMPLETE" || len(cm.PerNode) != 3 {
		t.Fatalf("cluster manifest = %+v, want COMPLETE with 3 nodes", cm)
	}
	for _, ref := range cm.PerNode {
		if ok, _ := objStore.Exists(ref.Ref); !ok {
			t.Fatalf("sub-manifest %q referenced but missing", ref.Ref)
		}
	}

	// Each node's stable storage identity is captured in the manifest topology (needed by 3c restore).
	if len(cm.SourceTopology) != 3 {
		t.Fatalf("source topology = %d entries, want 3", len(cm.SourceTopology))
	}
	for _, te := range cm.SourceTopology {
		if te.StorageUUID != "uuid-"+te.MemberID {
			t.Fatalf("topology %s StorageUUID = %q, want %q", te.MemberID, te.StorageUUID, "uuid-"+te.MemberID)
		}
	}
}

// TestCoordinatorPartialOnGap asserts an uncovered range (no live holder) commits PARTIAL with the gap
// enumerated — never a silent COMPLETE.
func TestCoordinatorPartialOnGap(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2")
	assigner := fakeAssigner{
		assignments: map[string]Selector{"m1": {}, "m2": {}},
		gaps:        []string{"range-x..range-y"},
	}
	coord := newCoord(t, objStore, meta, nodes, assigner)

	ctx := context.Background()
	id, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	st, err := coord.BackupStatus(ctx, id)
	if err != nil {
		t.Fatalf("BackupStatus: %v", err)
	}
	if st.GetStatus() != wavespanv1.BackupStatus_BACKUP_PARTIAL {
		t.Fatalf("status = %v, want PARTIAL", st.GetStatus())
	}
	if len(st.GetGaps()) != 1 || st.GetGaps()[0] != "range-x..range-y" {
		t.Fatalf("gaps = %v, want [range-x..range-y]", st.GetGaps())
	}
	cm, _ := ReadClusterManifest(objStore, id)
	if cm.Status != "PARTIAL" || len(cm.Gaps) != 1 {
		t.Fatalf("cluster manifest status/gaps = %s/%v, want PARTIAL/1", cm.Status, cm.Gaps)
	}
}

// TestCoordinatorListAndDelete covers the catalog surface: List returns committed backups, Delete drops
// one, and BackupStatus on a deleted/unknown id is NotFound.
func TestCoordinatorListAndDelete(t *testing.T) {
	objStore, _ := objstore.NewFS(t.TempDir())
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1")
	assigner := fakeAssigner{assignments: map[string]Selector{"m1": {}}}
	coord := newCoord(t, objStore, meta, nodes, assigner)

	ctx := context.Background()
	id1, _ := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	id2, _ := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})

	list, err := coord.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListBackups = %d, want 2", len(list))
	}

	// Deleting an existing backup reports deleted=true.
	if deleted, err := coord.DeleteBackup(ctx, id1, false); err != nil || !deleted {
		t.Fatalf("DeleteBackup(existing) = %v err %v, want true nil", deleted, err)
	}
	if _, err := coord.BackupStatus(ctx, id1); err == nil {
		t.Fatalf("BackupStatus(deleted) = nil err, want NotFound")
	}
	// Deleting an unknown id reports deleted=false, not a silent success.
	if deleted, err := coord.DeleteBackup(ctx, "bk-does-not-exist", false); err != nil || deleted {
		t.Fatalf("DeleteBackup(unknown) = %v err %v, want false nil", deleted, err)
	}
	if _, err := coord.BackupStatus(ctx, id2); err != nil {
		t.Fatalf("BackupStatus(%s): %v", id2, err)
	}
}
