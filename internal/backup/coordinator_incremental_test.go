package backup

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// buildPhysicalCluster seeds members backed by REAL wavesdb stores (the physical plane needs SSTables),
// each with a storage identity + one key, sharing objStore.
func buildPhysicalCluster(t *testing.T, objStore ObjectStore, ids ...string) map[string]*memberNode {
	t.Helper()
	nodes := map[string]*memberNode{}
	for _, id := range ids {
		src, err := storage.OpenWavesdb(t.TempDir())
		if err != nil {
			t.Fatalf("OpenWavesdb(%s): %v", id, err)
		}
		t.Cleanup(func() { _ = src.Close() })
		mustPut(t, src, storage.CFSys, []byte(storageIdentityKey), []byte("uuid-"+id))
		mustPut(t, src, storage.CFKVData, []byte("k0-"+id), []byte("v0"))
		nodes[id] = &memberNode{id: id, store: src, objStore: objStore, agent: NewAgent(nil)}
	}
	return nodes
}

func physicalSpec(parent string) *wavespanv1.BackupSpec {
	return &wavespanv1.BackupSpec{Planes: []wavespanv1.BackupPlane{wavespanv1.BackupPlane_BACKUP_PLANE_PHYSICAL}, Parent: parent}
}

// TestCoordinatorPhysicalIncremental drives a full physical backup B0 across two real-store nodes, then
// an incremental B1 parented on B0: each node resolves its parent checkpoint and uploads only its delta,
// and B1's cluster.manifest records parent=B0.
func TestCoordinatorPhysicalIncremental(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildPhysicalCluster(t, objStore, "m1", "m2")
	coord := newCoord(t, objStore, meta, nodes, AllExportAssigner{})
	ctx := context.Background()

	// B0: full physical.
	b0, err := coord.BeginBackup(ctx, physicalSpec(""))
	if err != nil {
		t.Fatalf("BeginBackup B0: %v", err)
	}
	st0, _ := coord.BackupStatus(ctx, b0)
	if st0.GetStatus() != wavespanv1.BackupStatus_BACKUP_COMPLETE {
		t.Fatalf("B0 status = %v, want COMPLETE", st0.GetStatus())
	}
	cm0, err := ReadClusterManifest(objStore, b0)
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(cm0.Planes, "physical") {
		t.Fatalf("B0 planes = %v, want physical", cm0.Planes)
	}
	// Per-node physical manifests recorded + present.
	base := map[string]*PhysicalManifest{}
	for _, ref := range cm0.PerNode {
		if ref.PhysicalManifest == "" {
			t.Fatalf("B0 node %s missing physical manifest ref", ref.MemberID)
		}
		pm, err := ReadPhysicalManifest(objStore, ref.PhysicalManifest)
		if err != nil {
			t.Fatalf("read B0 physical manifest %s: %v", ref.MemberID, err)
		}
		base[ref.MemberID] = pm
	}

	// Mutate each node so the incremental has new SSTables.
	for id, n := range nodes {
		mustPut(t, n.store, storage.CFKVData, []byte("k1-"+id), []byte("v1"))
	}

	// B1: incremental physical, parent B0.
	b1, err := coord.BeginBackup(ctx, physicalSpec(b0))
	if err != nil {
		t.Fatalf("BeginBackup B1: %v", err)
	}
	cm1, err := ReadClusterManifest(objStore, b1)
	if err != nil {
		t.Fatal(err)
	}
	if cm1.Parent != b0 {
		t.Fatalf("B1 parent = %q, want %q", cm1.Parent, b0)
	}

	// Each node's incremental uploaded only its NEW SSTables (full set minus parent), proving the diff.
	for _, ref := range cm1.PerNode {
		pm1, err := ReadPhysicalManifest(objStore, ref.PhysicalManifest)
		if err != nil {
			t.Fatalf("read B1 physical manifest %s: %v", ref.MemberID, err)
		}
		n0 := len(base[ref.MemberID].Tables)
		n1 := len(pm1.Tables)
		if n1 <= n0 {
			t.Fatalf("node %s: B1 tables %d, want > B0 %d", ref.MemberID, n1, n0)
		}
		if pm1.ParentGlobalSeq != base[ref.MemberID].GlobalSeq {
			t.Fatalf("node %s: B1 parent seq %d, want %d", ref.MemberID, pm1.ParentGlobalSeq, base[ref.MemberID].GlobalSeq)
		}
		got := countKlogs(t, objStore, b1+"/nodes/"+ref.MemberID+"/physical")
		if got != n1-n0 {
			t.Fatalf("node %s: B1 uploaded %d klogs, want only the %d new (full %d, parent %d)", ref.MemberID, got, n1-n0, n1, n0)
		}
	}
}

// TestCoordinatorRejectsBadParent covers the two up-front rejections: a logical-plane new backup with a
// parent, and a physical incremental whose parent has no physical plane.
func TestCoordinatorRejectsBadParent(t *testing.T) {
	objStore, _ := objstore.NewFS(t.TempDir())
	meta := newFakeMetaStore()
	nodes := buildPhysicalCluster(t, objStore, "m1")
	coord := newCoord(t, objStore, meta, nodes, AllExportAssigner{})
	ctx := context.Background()

	// A full physical base to parent from.
	b0, err := coord.BeginBackup(ctx, physicalSpec(""))
	if err != nil {
		t.Fatalf("BeginBackup B0: %v", err)
	}

	// Logical new backup with a parent → InvalidArgument (logical is full-only).
	_, err = coord.BeginBackup(ctx, &wavespanv1.BackupSpec{
		Planes: []wavespanv1.BackupPlane{wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL},
		Parent: b0,
	})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("logical+parent err = %v, want InvalidArgument", err)
	}

	// A physical incremental whose parent is a logical-only backup → FailedPrecondition.
	bLogical, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{
		Planes: []wavespanv1.BackupPlane{wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL},
	})
	if err != nil {
		t.Fatalf("BeginBackup logical base: %v", err)
	}
	_, err = coord.BeginBackup(ctx, physicalSpec(bLogical))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("physical-from-logical-parent err = %v, want FailedPrecondition", err)
	}
}
