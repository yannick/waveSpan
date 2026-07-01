package recordstore

import (
	"testing"

	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func rver(ms, seq uint64) version.Version {
	return version.Version{HLCPhysicalMs: ms, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: seq}
}

func delData(t *testing.T, mem storage.LocalStore, ns string, key []byte, v version.Version) {
	t.Helper()
	if err := mem.Batch([]storage.StoreOp{{CF: storage.CFKVData, Key: dataKey(ns, key, v), Delete: true}}); err != nil {
		t.Fatal(err)
	}
}

func latestPointer(t *testing.T, mem storage.LocalStore, ns string, key []byte) *wavespanv1.LatestPointer {
	t.Helper()
	b, found, err := mem.Get(storage.CFKVMeta, latestKey(ns, key))
	if err != nil || !found {
		t.Fatalf("latest pointer for %q: found=%v err=%v", key, found, err)
	}
	lp, err := storage.DecodeLatestPointer(b)
	if err != nil {
		t.Fatal(err)
	}
	return lp
}

// TestRepairCutMeta_PreservesLEQSiblingsOnRepoint: a conflicted key whose winner is after T is repointed
// to the max surviving ≤T version, and its ≤T concurrent siblings are PRESERVED (not dropped). A >T sibling
// is correctly excluded.
func TestRepairCutMeta_PreservesLEQSiblingsOnRepoint(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := NewStore(mem, "dev", "n1", version.NewClock(nil, 500), version.NewSequencer(0))

	vA, vB, vC, vBig := rver(100, 1), rver(200, 2), rver(5000, 3), rver(6000, 4)
	recs := []*wavespanv1.StoredRecord{
		rs.BuildRecord("app", []byte("k"), []byte("a"), vA, false, nil),
		rs.BuildRecord("app", []byte("k"), []byte("b"), vB, false, nil),
		rs.BuildRecord("app", []byte("k"), []byte("c"), vC, false, nil),
		rs.BuildRecord("app", []byte("k"), []byte("big"), vBig, false, nil),
	}
	if err := rs.ApplySiblings("app", []byte("k"), recs); err != nil { // winner=vBig, siblings=[vA,vB,vC]
		t.Fatal(err)
	}
	// Simulate a ≤T cut at T=1000: the export dropped the >T CFKVData records (vC, vBig); CFKVMeta is verbatim.
	delData(t, mem, "app", []byte("k"), vC)
	delData(t, mem, "app", []byte("k"), vBig)

	if err := RepairCutMeta(mem); err != nil {
		t.Fatalf("RepairCutMeta: %v", err)
	}

	lp := latestPointer(t, mem, "app", []byte("k"))
	if got := version.FromProto(lp.GetWinner()); got != vB {
		t.Fatalf("winner = %+v, want vB(200) — the max surviving ≤T version", got)
	}
	sibs := lp.GetSiblingVersions()
	if len(sibs) != 1 || version.FromProto(sibs[0]) != vA {
		var got []version.Version
		for _, s := range sibs {
			got = append(got, version.FromProto(s))
		}
		t.Fatalf("siblings = %+v, want exactly [vA(100)] (vB is winner, vC was >T-cut)", got)
	}
}

// TestRepairCutMeta_NonConflictRepointHasNoSiblings: the crux/non-conflict case — a key with plain version
// history (no LP siblings) whose winner is after T repoints to the latest ≤T version with NO siblings.
func TestRepairCutMeta_NonConflictRepointHasNoSiblings(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := NewStore(mem, "dev", "n1", version.NewClock(nil, 500), version.NewSequencer(0))

	v1, v2 := rver(100, 1), rver(200, 2)
	if _, err := rs.Apply(rs.BuildRecord("app", []byte("k"), []byte("a"), v1, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.Apply(rs.BuildRecord("app", []byte("k"), []byte("b"), v2, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	delData(t, mem, "app", []byte("k"), v2) // v2 > T=150, cut

	if err := RepairCutMeta(mem); err != nil {
		t.Fatalf("RepairCutMeta: %v", err)
	}
	lp := latestPointer(t, mem, "app", []byte("k"))
	if got := version.FromProto(lp.GetWinner()); got != v1 {
		t.Fatalf("winner = %+v, want v1(100)", got)
	}
	if len(lp.GetSiblingVersions()) != 0 {
		t.Fatalf("non-conflict repoint must have no siblings, got %d", len(lp.GetSiblingVersions()))
	}
}
