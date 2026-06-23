package local

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func aeStore(t *testing.T, member string) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return recordstore.NewStore(mem, "dev", member, version.NewClock(func() uint64 { return 1000 }, 500), version.NewSequencer(0))
}

func putVer(t *testing.T, s *recordstore.Store, key string, phys uint64, writer, val string) {
	t.Helper()
	v := version.Version{HLCPhysicalMs: phys, WriterClusterID: "dev", WriterMemberID: writer, WriterSequence: phys}
	if _, err := s.Apply(s.BuildRecord("default", []byte(key), []byte(val), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

type aeFakeCluster struct{ members []membership.MemberView }

func (c aeFakeCluster) Members() []membership.MemberView { return c.members }

func TestIntraAntiEntropyAdoptsNewerPeerVersion(t *testing.T) {
	// nodeA holds the WINNER (higher HLC); nodeB holds a stale version of the same key.
	nodeA := aeStore(t, "A")
	nodeB := aeStore(t, "B")
	putVer(t, nodeA, "reg", 200, "A", "winner")
	putVer(t, nodeB, "reg", 100, "B", "stale")

	stores := map[string]*recordstore.Store{"addrA": nodeA, "addrB": nodeB}
	fetch := PeerFetch(func(_ context.Context, addr, ns string, key []byte) (*wavespanv1.StoredRecord, bool) {
		rec, found, _ := stores[addr].GetRecord(ns, key)
		return rec, found
	})
	cluster := aeFakeCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "A", DataAddr: "addrA"}, State: membership.StateAlive},
		{Member: membership.Member{MemberID: "B", DataAddr: "addrB"}, State: membership.StateAlive},
	}}

	// before: nodeB is stale
	if out, _ := nodeB.Get("default", []byte("reg")); string(out.Value) != "stale" {
		t.Fatalf("setup: nodeB should be stale, got %q", out.Value)
	}

	ae := NewIntraAntiEntropy(nodeB, membership.Member{MemberID: "B"}, cluster, fetch, []string{"default"})
	if n := ae.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("expected 1 key reconciled, got %d", n)
	}
	// after: nodeB converged to the winner
	if out, _ := nodeB.Get("default", []byte("reg")); string(out.Value) != "winner" {
		t.Fatalf("nodeB should have adopted the newer version, got %q", out.Value)
	}
	// nodeA (already the winner) is unchanged + a second pass is a no-op
	aeA := NewIntraAntiEntropy(nodeA, membership.Member{MemberID: "A"}, cluster, fetch, []string{"default"})
	if n := aeA.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("the winner node should not change, reconciled %d", n)
	}
}
