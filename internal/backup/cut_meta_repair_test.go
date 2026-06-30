package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// newKVStore returns a fresh in-memory recordstore over its backing MemStore (returned too, since
// ExportLogical/RestoreLogical operate on the LocalStore directly).
func newKVStore(t *testing.T) (*recordstore.Store, storage.LocalStore) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	return rs, mem
}

func ver(ms uint64, seq uint64) version.Version {
	return version.Version{HLCPhysicalMs: ms, WriterClusterID: "dev", WriterMemberID: "m", WriterSequence: seq}
}

// roundTrip exports src under cut frontier captureMs (<=0 disables the cut) and restores into a fresh
// store, returning a recordstore over the restored data (CFKVMeta is repaired by the KV rebuild hook).
func roundTrip(t *testing.T, src storage.LocalStore, captureMs int64) (*recordstore.Store, storage.LocalStore) {
	t.Helper()
	objStore, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, objStore, "bk/nodes/m1", DefaultRegistry(), captureMs, Selector{}); err != nil {
		t.Fatalf("ExportLogical(captureMs=%d): %v", captureMs, err)
	}
	rs2, mem2 := newKVStore(t)
	if err := RestoreLogical(mem2, objStore, "bk/nodes/m1", DefaultRegistry(), RestoreInfo{}); err != nil {
		t.Fatalf("RestoreLogical: %v", err)
	}
	return rs2, mem2
}

// TestSiblingsPreservedWhenCutExcludesNothing is the no-regression gate AND the real-cluster common case:
// the cut frontier T = now + lease excludes essentially nothing, so CFKVMeta restores verbatim and a key
// with concurrent siblings keeps its conflict state (ConflictNone stays false). RepairCutMeta is a no-op
// here (every winner survives).
func TestSiblingsPreservedWhenCutExcludesNothing(t *testing.T) {
	rs, mem := newKVStore(t)
	a := rs.BuildRecord("app", []byte("k"), []byte("a"), ver(100, 1), false, nil)
	b := rs.BuildRecord("app", []byte("k"), []byte("b"), ver(200, 2), false, nil) // higher → winner
	if err := rs.ApplySiblings("app", []byte("k"), []*wavespanv1.StoredRecord{a, b}); err != nil {
		t.Fatal(err)
	}
	if src, err := rs.Get("app", []byte("k")); err != nil || src.ConflictNone {
		t.Fatalf("source must be in conflict (siblings present): out=%+v err=%v", src, err)
	}

	rs2, _ := roundTrip(t, mem, 1000) // cut at T=1000 excludes nothing (both versions ≤ T)

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found || string(out.Value) != "b" {
		t.Fatalf("winner value must survive: found=%v val=%q", out.Found, out.Value)
	}
	if out.ConflictNone {
		t.Fatal("REGRESSION: a cut that excludes nothing must preserve sibling/conflict state verbatim")
	}
}

// TestCutRepointsDanglingWinner exercises the only lossy path: a key whose WINNER was written after T is
// dropped from CFKVData, so its verbatim latest pointer dangles → repair repoints it to the surviving ≤T
// version (dropping that key's siblings). A sibling-laden key whose versions are all ≤T is untouched —
// loss is confined to the after-T key.
func TestCutRepointsDanglingWinner(t *testing.T) {
	rs, mem := newKVStore(t)
	ttl := int64(3_600_000) // 1h, on the surviving version

	// "kept": both sibling versions ≤ T → preserved verbatim.
	ka := rs.BuildRecord("app", []byte("kept"), []byte("a"), ver(100, 1), false, nil)
	kb := rs.BuildRecord("app", []byte("kept"), []byte("b"), ver(200, 2), false, nil)
	if err := rs.ApplySiblings("app", []byte("kept"), []*wavespanv1.StoredRecord{ka, kb}); err != nil {
		t.Fatal(err)
	}
	// "repointed": winner version is far after T; sibling survivor v1 (with TTL) is ≤ T.
	rv1 := rs.BuildRecord("app", []byte("repointed"), []byte("x"), ver(100, 3), false, &ttl)
	rfuture := rs.BuildRecord("app", []byte("repointed"), []byte("y"), ver(5000, 4), false, nil) // > T → winner, cut
	if err := rs.ApplySiblings("app", []byte("repointed"), []*wavespanv1.StoredRecord{rv1, rfuture}); err != nil {
		t.Fatal(err)
	}

	rs2, mem2 := roundTrip(t, mem, 1000) // cut at T=1000: drops repointed's winner (ms=5000) only

	// kept: untouched, siblings intact.
	if out, err := rs2.Get("app", []byte("kept")); err != nil || !out.Found || string(out.Value) != "b" || out.ConflictNone {
		t.Fatalf("kept must be verbatim (winner 'b', siblings intact): found=%v val=%q conflictNone=%v err=%v", out.Found, out.Value, out.ConflictNone, err)
	}
	// repointed: winner ms=5000 was cut → repoint to ms=100 survivor, siblings dropped, TTL carried.
	out, err := rs2.Get("app", []byte("repointed"))
	if err != nil {
		t.Fatalf("Get(repointed): %v", err)
	}
	if !out.Found || string(out.Value) != "x" || out.Version.HLCPhysicalMs != 100 {
		t.Fatalf("repointed must resolve to the ≤T survivor 'x'@100: found=%v val=%q ver=%d", out.Found, out.Value, out.Version.HLCPhysicalMs)
	}
	if !out.ConflictNone {
		t.Fatal("a repointed key (winner cut) must drop its sibling/conflict state")
	}
	if out.ExpiresAtMs == nil {
		t.Fatal("the surviving version's TTL must be carried onto the repointed pointer")
	}
	// The repointed survivor's TTL bucket index entry must exist (so the sweeper still expires it).
	it, err := mem2.Scan(storage.CFKVMeta, []byte{0xff}, nil, 0)
	if err != nil {
		t.Fatalf("scan TTL index: %v", err)
	}
	present := it.Valid()
	_ = it.Close()
	if !present {
		t.Fatal("repointed survivor's TTL bucket entry not reconstructed")
	}
}

// TestCutTombstoneWinnerRestore: a tombstone winner (≤T, present) restores verbatim → Get Found=false.
func TestCutTombstoneWinnerRestore(t *testing.T) {
	rs, mem := newKVStore(t)
	put := func(key, val string, v version.Version, tomb bool) {
		if _, err := rs.Apply(rs.BuildRecord("app", []byte(key), []byte(val), v, tomb, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
			t.Fatal(err)
		}
	}
	put("k", "a", ver(100, 1), false)
	put("k", "", ver(200, 2), true) // delete wins (≤ T)
	put("live", "z", ver(100, 3), false)

	rs2, _ := roundTrip(t, mem, 250)

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if out.Found {
		t.Fatal("tombstone winner must restore as Found=false (delete resurrected)")
	}
	if !out.Tombstone || out.Version.HLCPhysicalMs != 200 {
		t.Fatalf("expected tombstone winner at ms=200, got tombstone=%v version=%d", out.Tombstone, out.Version.HLCPhysicalMs)
	}
	if live, err := rs2.Get("app", []byte("live")); err != nil || !live.Found || string(live.Value) != "z" {
		t.Fatalf("control key 'live' should survive: found=%v val=%q err=%v", live.Found, live.Value, err)
	}
}

// TestCutTTLPreservedVerbatim: a TTL'd key whose version is ≤T restores verbatim — the latest pointer
// keeps its expiry and the CFKVMeta TTL bucket index entry survives (no repair needed).
func TestCutTTLPreservedVerbatim(t *testing.T) {
	rs, mem := newKVStore(t)
	ttl := int64(3_600_000) // 1h
	rec := rs.BuildRecord("app", []byte("k"), []byte("v"), ver(100, 1), false, &ttl)
	if _, err := rs.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	srcOut, err := rs.Get("app", []byte("k"))
	if err != nil || srcOut.ExpiresAtMs == nil {
		t.Fatalf("source key must carry expiry: out=%+v err=%v", srcOut, err)
	}

	rs2, mem2 := roundTrip(t, mem, 150) // cut excludes nothing (version ms=100 ≤ T)

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found || out.ExpiresAtMs == nil || *out.ExpiresAtMs != *srcOut.ExpiresAtMs {
		t.Fatalf("expiry must be preserved verbatim: found=%v expiry=%v (want %d)", out.Found, out.ExpiresAtMs, *srcOut.ExpiresAtMs)
	}
	it, err := mem2.Scan(storage.CFKVMeta, []byte{0xff}, nil, 0)
	if err != nil {
		t.Fatalf("scan TTL index: %v", err)
	}
	present := it.Valid()
	_ = it.Close()
	if !present {
		t.Fatal("TTL bucket index entry must survive verbatim")
	}
}
