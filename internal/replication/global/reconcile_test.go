package global

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/conflict"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func putRec(t *testing.T, s *recordstore.Store, cluster, member string, seq, phys uint64, key, val string) {
	t.Helper()
	v := versionOf(cluster, member, seq, phys)
	rec := &wavespanv1.StoredRecord{
		LogicalKey: []byte(key), Namespace: "default", Version: v,
		Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte(val)}},
	}
	if _, err := s.Apply(rec, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

func versionOf(cluster, member string, seq, phys uint64) *wavespanv1.Version {
	return &wavespanv1.Version{HlcPhysicalMs: phys, WriterClusterId: cluster, WriterMemberId: member, WriterSequence: seq}
}

func TestAntiEntropyRepairsMissedMutation(t *testing.T) {
	// cluster A (source of truth) has a,b,c; cluster B missed 'b'
	aStore := newRecStore(t, "a1")
	putRec(t, aStore, "test-a", "a1", 1, 100, "a", "va")
	putRec(t, aStore, "test-a", "a1", 2, 100, "b", "vb")
	putRec(t, aStore, "test-a", "a1", 3, 100, "c", "vc")

	bStore := newRecStore(t, "b1")
	putRec(t, bStore, "test-a", "a1", 1, 100, "a", "va")
	putRec(t, bStore, "test-a", "a1", 3, 100, "c", "vc")

	// A serves GlobalReplication (with anti-entropy over its store)
	addr := serveGlobal(t, &grpcGlobalServer{applier: NewApplier(aStore, conflict.NewRegistry(), nil), ae: NewAntiEntropy(aStore)})

	// B reconciles against A
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	peer := config.ClusterPeer{ClusterID: "test-a", ReplEndpoint: addr}
	rec := NewReconciler(NewAntiEntropy(bStore), NewApplier(bStore, conflict.NewRegistry(), nil), NewOutLog(mem, 0), []config.ClusterPeer{peer}, []string{"default"}, nil, nil)

	if got := rec.ReconcileOnce(context.Background()); got == 0 {
		t.Fatal("expected a divergent range to be repaired")
	}
	if out, _ := bStore.Get("default", []byte("b")); !out.Found || string(out.Value) != "vb" {
		t.Fatalf("anti-entropy did not repair missed key 'b': %+v", out)
	}
	// a second round converges -> no divergence
	if got := rec.ReconcileOnce(context.Background()); got != 0 {
		t.Fatalf("after repair the ranges should match, still divergent: %d", got)
	}
}

func TestAntiEntropyAdvancesCheckpoint(t *testing.T) {
	bStore := newRecStore(t, "b1")
	aStore := newRecStore(t, "a1")
	addr := serveGlobal(t, &grpcGlobalServer{applier: NewApplier(aStore, conflict.NewRegistry(), nil), ae: NewAntiEntropy(aStore)})

	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	outlog := NewOutLog(mem, 0)
	// pretend we shipped 3 entries to peer test-a on partition p
	part := Partition("default", []byte("k"))
	for i := 1; i <= 3; i++ {
		_ = outlog.Append("test-a", mut(i, "default", "k"), false)
	}
	peer := config.ClusterPeer{ClusterID: "test-a", ReplEndpoint: addr}
	rec := NewReconciler(NewAntiEntropy(bStore), NewApplier(bStore, conflict.NewRegistry(), nil), outlog, []config.ClusterPeer{peer}, []string{"default"}, nil, nil)
	rec.ReconcileOnce(context.Background())

	// checkpoint advanced -> compaction can now reclaim the shipped entries
	n, _ := outlog.CompactBelowCheckpoint("test-a", part)
	if n != 3 {
		t.Fatalf("anti-entropy should have advanced the checkpoint enabling compaction of 3 entries, got %d", n)
	}
}
