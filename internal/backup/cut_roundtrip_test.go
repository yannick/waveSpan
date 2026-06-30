package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// TestCutRoundTripLatestAsOfT is THE 3a.1 correctness gate. A key k is written v1(ms=100,"a") then
// v2(ms=200,"b"); a backup at frontierT=150 must, after restore, present the ≤T winner "a" with
// found=true and NO dangling latest pointer (the CFKVMeta winner v2 was dropped by the cut, so a verbatim
// copy would dangle — instead it is rebuilt to point at the surviving v1). A key written only after T is
// absent entirely.
func TestCutRoundTripLatestAsOfT(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))

	v1 := version.Version{HLCPhysicalMs: 100, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: 1}
	v2 := version.Version{HLCPhysicalMs: 200, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: 2}
	late := version.Version{HLCPhysicalMs: 300, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: 3}
	apply := func(key, val string, v version.Version) {
		if _, err := rs.Apply(rs.BuildRecord("app", []byte(key), []byte(val), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
			t.Fatal(err)
		}
	}
	apply("k", "a", v1)
	apply("k", "b", v2)
	apply("late", "z", late) // written only after T → must be absent post-restore

	objStore, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(mem, objStore, "bk/nodes/m1", DefaultRegistry(), 150, Selector{}); err != nil {
		t.Fatalf("ExportLogical: %v", err)
	}

	// Restore into a fresh store (CFKVMeta is derived → rebuilt by the KV RebuildAfterRestore hook).
	mem2 := storage.NewMemStore()
	t.Cleanup(func() { _ = mem2.Close() })
	if err := RestoreLogical(mem2, objStore, "bk/nodes/m1", DefaultRegistry(), RestoreInfo{}); err != nil {
		t.Fatalf("RestoreLogical: %v", err)
	}

	rs2 := recordstore.NewStore(mem2, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found {
		t.Fatal("CRUX: Get(k) not found — dangling latest pointer (the bug this prevents)")
	}
	if string(out.Value) != "a" {
		t.Fatalf("CRUX: Get(k) = %q, want the ≤T winner \"a\"", out.Value)
	}
	if out.Version.HLCPhysicalMs != 100 {
		t.Fatalf("CRUX: Get(k) version ms = %d, want 100 (the ≤T winner)", out.Version.HLCPhysicalMs)
	}

	if lateOut, err := rs2.Get("app", []byte("late")); err != nil {
		t.Fatalf("Get(late): %v", err)
	} else if lateOut.Found {
		t.Fatal("CRUX: a key written only after T must be absent after the cut")
	}
}
