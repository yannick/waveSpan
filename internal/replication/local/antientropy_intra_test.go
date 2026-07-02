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

// TestIntraAntiEntropyDigestSkipsConvergedPeers pins design/37 P2.11: when a peer's range digest
// matches the local batch, the per-key fetch phase for that peer must be skipped entirely — the
// old behavior was one FetchReplica per (key, peer) per tick even with zero divergence.
func TestIntraAntiEntropyDigestSkipsConvergedPeers(t *testing.T) {
	nodeA := aeStore(t, "A")
	nodeB := aeStore(t, "B")
	// Identical content on both nodes (same versions).
	for _, k := range []string{"k1", "k2", "k3"} {
		putVer(t, nodeA, k, 100, "A", "v-"+k)
		putVer(t, nodeB, k, 100, "A", "v-"+k)
	}

	stores := map[string]*recordstore.Store{"addrA": nodeA, "addrB": nodeB}
	fetches := 0
	fetch := PeerFetch(func(_ context.Context, addr, ns string, key []byte) (*wavespanv1.StoredRecord, bool) {
		fetches++
		rec, found, _ := stores[addr].GetRecord(ns, key)
		return rec, found
	})
	digest := PeerDigest(func(_ context.Context, addr, ns string, start, end []byte) ([]byte, uint64, bool) {
		recs, err := stores[addr].ScanRecords(ns, start, end)
		if err != nil {
			return nil, 0, false
		}
		return DigestRecords(recs), uint64(len(recs)), true
	})
	cluster := aeFakeCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "A", DataAddr: "addrA"}, State: membership.StateAlive},
		{Member: membership.Member{MemberID: "B", DataAddr: "addrB"}, State: membership.StateAlive},
	}}

	ae := NewIntraAntiEntropy(nodeB, membership.Member{MemberID: "B"}, cluster, fetch, []string{"default"}).WithDigest(digest)
	if n := ae.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("converged cluster reconciled %d keys", n)
	}
	if fetches != 0 {
		t.Fatalf("digest match must skip per-key fetches, saw %d", fetches)
	}

	// Diverge one key on A: digest mismatches -> fetch phase runs -> B adopts the winner.
	putVer(t, nodeA, "k2", 200, "A", "newer")
	if n := ae.ReconcileOnce(context.Background()); n != 1 {
		t.Fatalf("diverged cluster reconciled %d keys, want 1", n)
	}
	if fetches == 0 {
		t.Fatal("digest mismatch must trigger per-key fetches")
	}
	if out, _ := nodeB.Get("default", []byte("k2")); string(out.Value) != "newer" {
		t.Fatalf("nodeB k2 = %q, want newer", out.Value)
	}

	// A peer that cannot answer digests (old version) falls back to per-key fetches.
	fetches = 0
	aeNoDigest := NewIntraAntiEntropy(nodeB, membership.Member{MemberID: "B"}, cluster, fetch, []string{"default"}).
		WithDigest(func(context.Context, string, string, []byte, []byte) ([]byte, uint64, bool) { return nil, 0, false })
	if n := aeNoDigest.ReconcileOnce(context.Background()); n != 0 {
		t.Fatalf("converged cluster reconciled %d keys", n)
	}
	if fetches == 0 {
		t.Fatal("digest-incapable peer must fall back to per-key fetches")
	}
}
