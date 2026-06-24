package cache

import (
	"context"
	"net"
	"testing"

	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	local "github.com/yannick/wavespan/internal/replication/local"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
)

// testReplicaServer is a minimal gRPC ReplicationService for the cache tests. It serves only the two
// methods the cache clients exercise (FetchReplica + SubscribeKey) directly over the record store, so
// the test does not import grpcsrv (which would create a grpcsrv→kv→cache import cycle).
type testReplicaServer struct {
	wavespanv1.UnimplementedReplicationServiceServer
	rec      *recordstore.Store
	self     string
	dataAddr string
	source   local.SubscriptionSource
}

func (s *testReplicaServer) FetchReplica(_ context.Context, req *wavespanv1.FetchReplicaRequest) (*wavespanv1.FetchReplicaResponse, error) {
	rec, found, err := s.rec.GetRecord(req.GetNamespace(), req.GetKey())
	if err != nil {
		return nil, err
	}
	resp := &wavespanv1.FetchReplicaResponse{Found: found, Record: rec}
	if found && req.GetWantSubscriptionOffer() {
		resp.SubscriptionOffer = &wavespanv1.SubscriptionOffer{SourceMemberId: s.self, SourceDataAddr: s.dataAddr}
	}
	return resp, nil
}

func (s *testReplicaServer) SubscribeKey(req *wavespanv1.SubscribeKeyRequest, stream grpc.ServerStreamingServer[wavespanv1.CacheUpdate]) error {
	if s.source != nil {
		return s.source.Subscribe(stream.Context(), req, stream.Send)
	}
	rec, found, err := s.rec.GetRecord(req.GetNamespace(), req.GetKey())
	if err != nil {
		return err
	}
	if found {
		return stream.Send(&wavespanv1.CacheUpdate{Namespace: req.GetNamespace(), Key: req.GetKey(), Record: rec, StreamSequence: 1})
	}
	return nil
}

// serveTestReplication serves the given testReplicaServer on a loopback port and returns its
// host:port; the server is stopped via t.Cleanup.
func serveTestReplication(t *testing.T, srv *testReplicaServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	wavespanv1.RegisterReplicationServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

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
	addr := serveTestReplication(t, &testReplicaServer{rec: rec, self: "node1"})

	// node3's directory learns node1 holds foo
	now := int64(1)
	dir := NewDirectory("node3", func() int64 { return now })
	srcDir := NewDirectory("node1", func() int64 { return now })
	srcDir.AddHeldKey("default", []byte("foo"))
	dir.ApplyPeerSummary(srcDir.OwnSummary())

	self := membership.Member{ClusterID: "dev", MemberID: "node3"}
	cluster := staticCluster{{Member: membership.Member{MemberID: "node1", DataAddr: addr}, State: membership.StateAlive}}
	f := NewFetcher(self, dir, cluster, latencygraph.New(latencygraph.DefaultConfig()), nil)

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
