package backup

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

type fixedClock struct{ ms int64 }

func (f fixedClock) NowMs() int64 { return f.ms }

func kvMemberWith(t *testing.T, member string, key string, hlcMs uint64) storage.LocalStore {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", member, version.NewClock(nil, 500), version.NewSequencer(0))
	v := version.Version{HLCPhysicalMs: hlcMs, WriterClusterID: "dev", WriterMemberID: member, WriterSequence: 1}
	if _, err := rs.Apply(rs.BuildRecord("app", []byte(key), []byte("v"), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	return mem
}

// TestClusterWideCutExcludesPostT proves the frontier T is cluster-wide: the coordinator picks ONE T and
// every node's export applies it. With T = fixedNow + frontierLeaseMs, m1's ≤T KV record is captured and
// m2's >T record is excluded from the union (its CFKVData ends up empty → no object).
func TestClusterWideCutExcludesPostT(t *testing.T) {
	ctx := context.Background()
	const fixedNow = int64(1_000_000)
	frontierT := fixedNow + frontierLeaseMs

	objStore, _ := objstore.NewFS(t.TempDir())
	mem1 := kvMemberWith(t, "m1", "k1", uint64(fixedNow-1000)) // ≤ T → captured
	mem2 := kvMemberWith(t, "m2", "k2", uint64(frontierT+10000)) // > T → excluded
	nodes := map[string]*memberNode{
		"m1": {id: "m1", store: mem1, objStore: objStore, agent: NewAgent(nil)},
		"m2": {id: "m2", store: mem2, objStore: objStore, agent: NewAgent(nil)},
	}
	coord := NewCoordinator(Config{
		Self:      "m1",
		Meta:      newFakeMetaStore(),
		ObjStore:  objStore,
		Roster:    fakeRoster{live: []string{"m1", "m2"}},
		ClientFor: func(id string) (NodeClient, error) { return nodes[id], nil },
		Assigner:  AllExportAssigner{},
		Clock:     fixedClock{ms: fixedNow},
	})

	id, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{}) // logical, default destination
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}

	// The cluster-wide T is recorded in the manifest (= fixedNow + frontierLeaseMs).
	cm, err := ReadClusterManifest(objStore, id)
	if err != nil {
		t.Fatal(err)
	}
	if cm.FrontierT != frontierT {
		t.Fatalf("manifest FrontierT = %d, want %d (cluster-wide)", cm.FrontierT, frontierT)
	}

	// m1's ≤T record captured.
	if ok, _ := objStore.Exists(id + "/nodes/m1/cf/kv_data"); !ok {
		t.Fatal("m1 ≤T KV record should be captured")
	}
	if v := kvVersionsInObject(t, objStore, id+"/nodes/m1/cf/kv_data"); !has(v, uint64(fixedNow-1000)) {
		t.Fatalf("m1 kv_data versions %v, want the ≤T record", v)
	}
	// m2's only record is >T → excluded → empty CFKVData → no object written (same T applied on m2).
	if ok, _ := objStore.Exists(id + "/nodes/m2/cf/kv_data"); ok {
		t.Fatal("m2 >T KV record must be excluded by the cluster-wide cut (no kv_data object expected)")
	}
}
