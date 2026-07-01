package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// faultClient wraps a real member node but can simulate an unreachable/hung peer: failPrepare/failExport
// return a dial-timeout-like error; block hangs until the context is cancelled (a dead peer).
type faultClient struct {
	delegate    NodeClient
	failPrepare bool
	failExport  bool
	block       bool
}

func (f faultClient) Prepare(ctx context.Context, backupID string, frontierT int64) (PrepareResult, error) {
	if f.block {
		<-ctx.Done()
		return PrepareResult{}, ctx.Err()
	}
	if f.failPrepare {
		return PrepareResult{}, fmt.Errorf("dial tcp 10.0.0.9:7800: i/o timeout")
	}
	return f.delegate.Prepare(ctx, backupID, frontierT)
}

func (f faultClient) Export(ctx context.Context, req ExportRequest) (ExportResult, error) {
	if f.block {
		<-ctx.Done()
		return ExportResult{}, ctx.Err()
	}
	if f.failExport {
		return ExportResult{}, fmt.Errorf("dial tcp 10.0.0.9:7800: i/o timeout")
	}
	return f.delegate.Export(ctx, req)
}

// newFaultCoord builds a coordinator over nodes where the member ids in `faults` are wrapped with the given
// fault behavior. self is m1. live == members == all node ids (the down member is stale-but-still-in-roster,
// as on the churned stag cluster).
func newFaultCoord(t *testing.T, objStore ObjectStore, meta MetaStore, nodes map[string]*memberNode, faults map[string]faultClient, memberTimeout time.Duration) *Coordinator {
	t.Helper()
	ids := make([]string, 0, len(nodes))
	assignments := map[string]Selector{}
	for id := range nodes {
		ids = append(ids, id)
		assignments[id] = Selector{}
	}
	return NewCoordinator(Config{
		Self:     "m1",
		Meta:     meta,
		ObjStore: objStore,
		Roster:   fakeRoster{live: ids, members: ids},
		ClientFor: func(id string) (NodeClient, error) {
			if fc, ok := faults[id]; ok {
				fc.delegate = nodes[id]
				return fc, nil
			}
			return nodes[id], nil
		},
		Assigner:      fakeAssigner{assignments: assignments},
		MemberTimeout: memberTimeout,
	})
}

// TestFanout_UnreachableMemberPrepare_PartialNotFatal: a member that is unreachable at PREPARE is skipped
// (not fatal); the backup completes as PARTIAL with the reachable members' objects and a member gap.
func TestFanout_UnreachableMemberPrepare_PartialNotFatal(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	coord := newFaultCoord(t, objStore, meta, nodes, map[string]faultClient{"m3": {failPrepare: true}}, 0)

	ctx := context.Background()
	id, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup must NOT hard-fail on one unreachable member: %v", err)
	}
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatalf("ReadClusterManifest: %v", err)
	}
	if cm.Status != "PARTIAL" {
		t.Fatalf("status = %s, want PARTIAL (one member skipped)", cm.Status)
	}
	if !containsString(cm.Gaps, "member:m3") {
		t.Fatalf("gaps = %v, want member:m3", cm.Gaps)
	}
	if len(cm.PerNode) != 2 {
		t.Fatalf("per_node refs = %d, want 2 (m1,m2 exported; m3 skipped)", len(cm.PerNode))
	}
	for _, ref := range cm.PerNode {
		if ref.MemberID == "m3" {
			t.Fatal("skipped member m3 must not have a sub-manifest ref")
		}
		if ok, _ := objStore.Exists(ref.Ref); !ok {
			t.Fatalf("reachable member %s sub-manifest %q missing", ref.MemberID, ref.Ref)
		}
	}
}

// TestFanout_UnreachableMemberExport_Partial: a member reachable at prepare but failing at EXPORT is
// skipped → PARTIAL with a member gap.
func TestFanout_UnreachableMemberExport_Partial(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	coord := newFaultCoord(t, objStore, meta, nodes, map[string]faultClient{"m3": {failExport: true}}, 0)

	id, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{})
	if err != nil {
		t.Fatalf("BeginBackup must tolerate an export failure: %v", err)
	}
	cm, _ := ReadClusterManifest(objStore, id)
	if cm.Status != "PARTIAL" || !containsString(cm.Gaps, "member:m3") {
		t.Fatalf("status/gaps = %s/%v, want PARTIAL with member:m3", cm.Status, cm.Gaps)
	}
}

// TestFanout_SelfExportFatal: the coordinator's OWN node failing is fatal — a backup missing self cannot be
// produced.
func TestFanout_SelfExportFatal(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	coord := newFaultCoord(t, objStore, meta, nodes, map[string]faultClient{"m1": {failExport: true}}, 0)

	if _, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{}); err == nil {
		t.Fatal("self (m1) export failure must be fatal, got nil error")
	}
}

// TestFanout_AllMembersFail_Fatal: if NO member exports successfully, the backup fails (not a vacuous empty
// PARTIAL).
func TestFanout_AllMembersFail_Fatal(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	faults := map[string]faultClient{"m1": {failExport: true}, "m2": {failExport: true}, "m3": {failExport: true}}
	coord := newFaultCoord(t, objStore, meta, nodes, faults, 0)

	if _, err := coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{}); err == nil {
		t.Fatal("all-members-fail must fail the backup, got nil error")
	}
}

// TestFanout_HungPeerBounded: a peer that hangs is bounded by MemberTimeout — the backup still completes
// (as PARTIAL) rather than hanging on the dead peer.
func TestFanout_HungPeerBounded(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2", "m3")
	coord := newFaultCoord(t, objStore, meta, nodes, map[string]faultClient{"m3": {block: true}}, 150*time.Millisecond)

	done := make(chan struct{})
	var id string
	var berr error
	go func() {
		id, berr = coord.BeginBackup(context.Background(), &wavespanv1.BackupSpec{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("BeginBackup hung on a dead peer — MemberTimeout did not bound the fan-out")
	}
	if berr != nil {
		t.Fatalf("hung peer must be bounded + skipped, not fatal: %v", berr)
	}
	cm, _ := ReadClusterManifest(objStore, id)
	if cm.Status != "PARTIAL" || !containsString(cm.Gaps, "member:m3") {
		t.Fatalf("status/gaps = %s/%v, want PARTIAL with member:m3", cm.Status, cm.Gaps)
	}
}
