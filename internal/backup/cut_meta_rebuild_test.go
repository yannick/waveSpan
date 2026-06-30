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

// roundTrip exports src under a cut frontier captureMs (<=0 disables the cut → full backup) and restores
// into a fresh store, returning a recordstore over the restored data for read-path assertions.
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

// TestCutTombstoneWinnerRestore: a key whose ≤T winner is a tombstone restores (via the rebuilt CFKVMeta)
// as Found=false — the delete is honoured, not resurrected.
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

	rs2, _ := roundTrip(t, mem, 250) // cut active → CFKVMeta rebuilt

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

// TestCutTTLRebuild: a TTL'd key's expiry survives the cut rebuild — the latest pointer carries the
// expiry AND the CFKVMeta TTL bucket index entry is reconstructed (so the sweeper still expires it).
func TestCutTTLRebuild(t *testing.T) {
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

	rs2, mem2 := roundTrip(t, mem, 150) // cut active → CFKVMeta rebuilt

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found || out.ExpiresAtMs == nil {
		t.Fatalf("restored key must be found with expiry, got found=%v expiry=%v", out.Found, out.ExpiresAtMs)
	}
	if *out.ExpiresAtMs != *srcOut.ExpiresAtMs {
		t.Fatalf("expiry not preserved: restored %d, source %d", *out.ExpiresAtMs, *srcOut.ExpiresAtMs)
	}
	// The TTL bucket index entry (0xff sentinel) must be reconstructed.
	it, err := mem2.Scan(storage.CFKVMeta, []byte{0xff}, nil, 0)
	if err != nil {
		t.Fatalf("scan TTL index: %v", err)
	}
	present := it.Valid()
	_ = it.Close()
	if !present {
		t.Fatal("TTL bucket index entry not reconstructed by RebuildMeta")
	}
}

// TestSiblingsPreservedOnFullBackup: a full (non-cut) backup exports CFKVMeta verbatim, so a key with
// concurrent siblings keeps its SiblingVersions / conflict-present state across restore (no regression of
// the existing full-backup path).
func TestSiblingsPreservedOnFullBackup(t *testing.T) {
	rs, mem := newKVStore(t)
	a := rs.BuildRecord("app", []byte("k"), []byte("a"), ver(100, 1), false, nil)
	b := rs.BuildRecord("app", []byte("k"), []byte("b"), ver(200, 2), false, nil) // higher → winner
	if err := rs.ApplySiblings("app", []byte("k"), []*wavespanv1.StoredRecord{a, b}); err != nil {
		t.Fatal(err)
	}
	if src, err := rs.Get("app", []byte("k")); err != nil || src.ConflictNone {
		t.Fatalf("source must be in conflict (siblings present): out=%+v err=%v", src, err)
	}

	rs2, _ := roundTrip(t, mem, 0) // full backup (no cut) → CFKVMeta verbatim, no rebuild

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found || string(out.Value) != "b" {
		t.Fatalf("winner value must survive: found=%v val=%q", out.Found, out.Value)
	}
	if out.ConflictNone {
		t.Fatal("REGRESSION: full backup dropped sibling/conflict state (ConflictNone flipped to true)")
	}
}

// TestSiblingsCollapseOnCut: a ≤T cut omits CFKVMeta and rebuilds it as the LWW winner — concurrent
// siblings collapse to the winner (documented cut-only limitation). The winner VALUE is still correct and
// the sibling values survive as distinct CFKVData versions.
func TestSiblingsCollapseOnCut(t *testing.T) {
	rs, mem := newKVStore(t)
	a := rs.BuildRecord("app", []byte("k"), []byte("a"), ver(100, 1), false, nil)
	b := rs.BuildRecord("app", []byte("k"), []byte("b"), ver(200, 2), false, nil) // higher → winner
	if err := rs.ApplySiblings("app", []byte("k"), []*wavespanv1.StoredRecord{a, b}); err != nil {
		t.Fatal(err)
	}

	rs2, mem2 := roundTrip(t, mem, 300) // cut includes both siblings → CFKVMeta rebuilt as LWW winner

	out, err := rs2.Get("app", []byte("k"))
	if err != nil {
		t.Fatalf("Get(k): %v", err)
	}
	if !out.Found || string(out.Value) != "b" {
		t.Fatalf("winner value must be correct after collapse: found=%v val=%q", out.Found, out.Value)
	}
	if !out.ConflictNone {
		t.Fatal("a ≤T cut must collapse siblings to the LWW winner (ConflictNone should be true)")
	}
	// Both sibling values survive as distinct CFKVData versions (only the conflict metadata is lost).
	it, err := mem2.Scan(storage.CFKVData, nil, nil, 0)
	if err != nil {
		t.Fatalf("scan CFKVData: %v", err)
	}
	var n int
	for it.Valid() {
		n++
		it.Next()
	}
	_ = it.Close()
	if n != 2 {
		t.Fatalf("expected both sibling records (2) preserved in CFKVData, got %d", n)
	}
}
