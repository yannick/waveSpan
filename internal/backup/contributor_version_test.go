package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

func contributorByName(t *testing.T, name string) Contributor {
	t.Helper()
	for _, c := range DefaultRegistry().Contributors() {
		if c.Name() == name {
			return c
		}
	}
	t.Fatalf("contributor %q not registered", name)
	return nil
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestVersionOf pins the per-contributor HLC version extraction used by the consistent cut: KV/graph/
// vector decode their record version; derived/index CFs, system config, and the raft-consistent
// collections CF return ok=false.
func TestVersionOf(t *testing.T) {
	kv := contributorByName(t, "kv")
	kvVal, err := storage.EncodeStoredRecord(&wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 1234}})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := kv.VersionOf(storage.CFKVData, []byte("k"), kvVal); !ok || v.HLCPhysicalMs != 1234 {
		t.Fatalf("kv CFKVData VersionOf = %v ok %v, want 1234 true", v.HLCPhysicalMs, ok)
	}
	if _, ok := kv.VersionOf(storage.CFKVMeta, []byte("k"), kvVal); ok {
		t.Fatalf("kv CFKVMeta VersionOf should be false (derived latest-pointer)")
	}

	g := contributorByName(t, "graph")
	if v, ok := g.VersionOf(storage.CFGraphData, graph.NodeKey("g", "n1"), mustMarshal(t, &wavespanv1.NodeRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 50}})); !ok || v.HLCPhysicalMs != 50 {
		t.Fatalf("graph node VersionOf = %v ok %v, want 50 true", v.HLCPhysicalMs, ok)
	}
	if v, ok := g.VersionOf(storage.CFGraphData, graph.EdgeKey("g", "e1"), mustMarshal(t, &wavespanv1.EdgeRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 60}})); !ok || v.HLCPhysicalMs != 60 {
		t.Fatalf("graph edge VersionOf = %v ok %v, want 60 true", v.HLCPhysicalMs, ok)
	}
	if _, ok := g.VersionOf(storage.CFGraphIndex, []byte("idx"), []byte("x")); ok {
		t.Fatalf("graph CFGraphIndex VersionOf should be false (derived)")
	}

	vec := contributorByName(t, "vector")
	if v, ok := vec.VersionOf(storage.CFVectorRaw, []byte("vr"), mustMarshal(t, &wavespanv1.VectorRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 77}})); !ok || v.HLCPhysicalMs != 77 {
		t.Fatalf("vector VersionOf = %v ok %v, want 77 true", v.HLCPhysicalMs, ok)
	}

	if _, ok := contributorByName(t, "system").VersionOf(storage.CFSys, []byte("/sys/config"), []byte("v")); ok {
		t.Fatalf("system VersionOf should be false")
	}
	if _, ok := contributorByName(t, "collections").VersionOf(storage.CFReplData, []byte("k"), []byte("v")); ok {
		t.Fatalf("collections VersionOf should be false (raft-consistent, not version-cut)")
	}
}

// TestVersionLEQ pins the ceiling test: at-or-below T passes (equal ms included), above T fails.
func TestVersionLEQ(t *testing.T) {
	if !versionLEQ(version.Version{HLCPhysicalMs: 100}, 150) {
		t.Fatal("100 <= 150 should pass")
	}
	if !versionLEQ(version.Version{HLCPhysicalMs: 150}, 150) {
		t.Fatal("150 <= 150 should pass (equal included)")
	}
	if versionLEQ(version.Version{HLCPhysicalMs: 151}, 150) {
		t.Fatal("151 <= 150 should fail")
	}
}
