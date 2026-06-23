package local

import (
	"context"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestBootstrapperStreamsEverywhereRecords: a fresh node pulls all records of an "everywhere"
// namespace from a peer and applies them locally.
func TestBootstrapperStreamsEverywhereRecords(t *testing.T) {
	source := aeStore(t, "node1")
	fresh := aeStore(t, "node2")
	for i := 0; i < 20; i++ {
		putVer(t, source, string(rune('a'+i%26))+string(rune('0'+i/26)), uint64(100+i), "node1", "v")
	}

	// the peer's Backfill = a page from the source's record scan
	fetch := BackfillFetch(func(_ context.Context, _, ns string, cursor []byte, limit int) ([]*wavespanv1.StoredRecord, []byte, error) {
		return source.ScanRecordsFrom(ns, cursor, limit)
	})
	cluster := aeFakeCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "node2"}, State: membership.StateAlive},
		{Member: membership.Member{MemberID: "node1", DataAddr: "node1:7800"}, State: membership.StateAlive},
	}}

	b := NewBootstrapper(fresh, membership.Member{MemberID: "node2"}, cluster, fetch, []string{"default"})
	applied := b.BootstrapOnce(context.Background())
	if applied != 20 {
		t.Fatalf("expected 20 records streamed, got %d", applied)
	}
	// every source record is now present on the fresh node
	src, _, _ := source.ScanRecordsFrom("default", nil, 1000)
	for _, rec := range src {
		out, err := fresh.Get("default", rec.GetLogicalKey())
		if err != nil || !out.Found {
			t.Fatalf("fresh node missing backfilled key %q", rec.GetLogicalKey())
		}
	}
}

// TestBootstrapperNoPeer: with no alive peer, bootstrap applies nothing and does not hang.
func TestBootstrapperNoPeer(t *testing.T) {
	fresh := aeStore(t, "node2")
	cluster := aeFakeCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "node2"}, State: membership.StateAlive},
	}}
	fetch := BackfillFetch(func(context.Context, string, string, []byte, int) ([]*wavespanv1.StoredRecord, []byte, error) {
		t.Fatal("fetch should not be called without a peer")
		return nil, nil, nil
	})
	b := NewBootstrapper(fresh, membership.Member{MemberID: "node2"}, cluster, fetch, []string{"default"})
	if n := b.BootstrapOnce(context.Background()); n != 0 {
		t.Fatalf("no peer should apply 0 records, got %d", n)
	}
}
