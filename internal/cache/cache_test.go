package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	local "github.com/cwire/wavespan/internal/replication/local"
	"github.com/cwire/wavespan/internal/storage"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	b := NewBloom()
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("ns\x00k1")}
	for _, k := range keys {
		b.Add(k)
	}
	for _, k := range keys {
		if !b.MaybeContains(k) {
			t.Fatalf("bloom false negative for %q", k)
		}
	}
	if b.MaybeContains([]byte("definitely-absent-xyz")) {
		// possible false positive, but extremely unlikely for one probe; just informational
		t.Log("rare bloom false positive (acceptable)")
	}
	// serialize round-trip preserves membership
	b2 := BloomFromBytes(b.Bytes())
	for _, k := range keys {
		if !b2.MaybeContains(k) {
			t.Fatalf("round-trip bloom lost %q", k)
		}
	}
}

func TestDirectoryResolvesHolderViaGossipedBloom(t *testing.T) {
	now := int64(1000)
	dir := NewDirectory("node3", func() int64 { return now })

	// node1 advertises it holds default/foo
	src := NewDirectory("node1", func() int64 { return now })
	src.AddHeldKey("default", []byte("foo"))
	dir.ApplyPeerSummary(src.OwnSummary())

	holders := dir.ResolveHolders("default", []byte("foo"))
	if len(holders) != 1 || holders[0] != "node1" {
		t.Fatalf("directory should resolve node1 as holder, got %v", holders)
	}
	// an unknown key resolves to nothing (modulo rare bloom FPs)
	if got := dir.ResolveHolders("default", []byte("never-written")); len(got) > 0 && got[0] != "node1" {
		t.Fatalf("unexpected holder for unknown key: %v", got)
	}
}

// TestFetchFromClosestHolder stands up a real holder over the Connect ReplicationService and
// fetches a key through the directory + Fetcher, with no broadcast.
func TestFetchFromClosestHolder(t *testing.T) {
	// holder node1 with default/foo = bar
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rec := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	v := rec.NextVersion()
	sr := rec.BuildRecord("default", []byte("foo"), []byte("bar"), v, false, nil)
	if _, err := rec.Apply(sr, wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
	server := local.NewReplicaServer(local.NewReceiver(rec, "node1", local.NewIdempotency(0)), rec, "node1", "", nil)
	mux := http.NewServeMux()
	mux.Handle(server.Handler())
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	addr := strings.TrimPrefix(ts.URL, "http://")

	// node3's directory learns node1 holds foo
	now := int64(1)
	dir := NewDirectory("node3", func() int64 { return now })
	srcDir := NewDirectory("node1", func() int64 { return now })
	srcDir.AddHeldKey("default", []byte("foo"))
	dir.ApplyPeerSummary(srcDir.OwnSummary())

	self := membership.Member{ClusterID: "dev", MemberID: "node3"}
	cluster := staticCluster{{Member: membership.Member{MemberID: "node1", DataAddr: addr}, State: membership.StateAlive}}
	f := NewFetcher(self, dir, cluster, latencygraph.New(latencygraph.DefaultConfig()), http.DefaultClient)

	res, err := f.Fetch(context.Background(), "default", []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || res.Source != "node1" || string(res.Record.GetValue().GetInline()) != "bar" {
		t.Fatalf("fetch result = %+v, want found bar from node1", res)
	}
}

type staticCluster []membership.MemberView

func (c staticCluster) Members() []membership.MemberView { return []membership.MemberView(c) }