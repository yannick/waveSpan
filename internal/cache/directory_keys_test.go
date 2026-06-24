package cache

import (
	"fmt"
	"reflect"
	"testing"
)

// fixedClock returns a monotonic-ish clock so each summary generation advances.
func fixedClock() func() int64 {
	n := int64(0)
	return func() int64 { n++; return n }
}

func TestDirectoryDistinctEstimateDedupsReplicas(t *testing.T) {
	// Two nodes that both hold the SAME 10k keys (full replication). The cluster distinct count must
	// be ~10k, not 20k, because each key hashes identically on both holders.
	a := NewDirectory("a", fixedClock())
	b := NewDirectory("b", fixedClock())
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		a.AddHeldKey("default", key)
		b.AddHeldKey("default", key)
	}
	a.ApplyPeerSummary(b.OwnSummary())

	distinct := float64(a.DistinctKeysEstimate(nil))
	if distinct < 9_600 || distinct > 10_400 {
		t.Fatalf("distinct estimate = %v, want ~10000 (replicas should dedup)", distinct)
	}
}

func TestDirectoryReplicaSumAndAliveFilter(t *testing.T) {
	a := NewDirectory("a", fixedClock())
	// Peer b reports 700 keys, peer c reports 300; both alive → 1000.
	bs := HolderSummaryWire{MemberID: "b", ApproxKeys: 700, GeneratedAtUnixMs: 1, Bloom: NewBloom().Bytes(), HLL: NewHLL().Bytes()}
	cs := HolderSummaryWire{MemberID: "c", ApproxKeys: 300, GeneratedAtUnixMs: 1, Bloom: NewBloom().Bytes(), HLL: NewHLL().Bytes()}
	a.ApplyPeerSummary(bs)
	a.ApplyPeerSummary(cs)

	if got := a.PeerReplicaSum(nil); got != 1000 {
		t.Fatalf("PeerReplicaSum(all) = %d, want 1000", got)
	}
	// Only b alive → 700.
	alive := func(m string) bool { return m == "b" }
	if got := a.PeerReplicaSum(alive); got != 700 {
		t.Fatalf("PeerReplicaSum(alive=b) = %d, want 700", got)
	}
}

func TestDirectoryNamespacesUnion(t *testing.T) {
	a := NewDirectory("a", fixedClock())
	a.AddHeldKey("orders", []byte("k1"))
	a.AddHeldKey("users", []byte("k2"))
	a.ApplyPeerSummary(HolderSummaryWire{
		MemberID: "b", GeneratedAtUnixMs: 1, Bloom: NewBloom().Bytes(), HLL: NewHLL().Bytes(),
		Namespaces: []string{"users", "products"},
	})
	got := a.Namespaces(nil)
	want := []string{"orders", "products", "users"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Namespaces union = %v, want %v", got, want)
	}
}

func TestDirectoryOwnSummaryCarriesNamespacesSorted(t *testing.T) {
	d := NewDirectory("a", fixedClock())
	d.AddHeldKey("zeta", []byte("x"))
	d.AddHeldKey("alpha", []byte("y"))
	d.AddHeldKey("alpha", []byte("z")) // duplicate namespace must not duplicate
	got := d.OwnSummary().Namespaces
	want := []string{"alpha", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OwnSummary namespaces = %v, want %v", got, want)
	}
}
