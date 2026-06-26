package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/latencygraph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/security"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

type fakeCluster struct{}

func (fakeCluster) Members() []membership.MemberView { return nil }
func (fakeCluster) Graph() *latencygraph.Graph {
	return latencygraph.New(latencygraph.DefaultConfig())
}

func newObsServer(t *testing.T, resolver ClusterKeyResolver) (wavespanv1connect.ObservabilityServiceClient, *recordstore.Store) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(64), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs)
	if resolver != nil {
		obs.WithClusterResolver(resolver)
	}
	mux := http.NewServeMux()
	mux.Handle(obs.Handler())
	wrapped := security.Identity{DevMode: true}.EnforceHTTP(mux)
	ts := httptest.NewServer(wrapped)
	t.Cleanup(ts.Close)
	return wavespanv1connect.NewObservabilityServiceClient(ts.Client(), ts.URL), rs
}

func seedKV(t *testing.T, rs *recordstore.Store, key, val string) {
	t.Helper()
	v := rs.NextVersion()
	if _, err := rs.Apply(rs.BuildRecord("default", []byte(key), []byte(val), v, false, nil), wavespanv1.MutationKind_MUTATION_KIND_PUT); err != nil {
		t.Fatal(err)
	}
}

func inspectLocal(t *testing.T, client wavespanv1connect.ObservabilityServiceClient, role string, includeValue bool) []*wavespanv1.InspectKey {
	t.Helper()
	req := connect.NewRequest(&wavespanv1.InspectLocalRequest{Namespace: "default", IncludeValue: includeValue})
	req.Header().Set("X-WaveSpan-Role", role)
	stream, err := client.InspectLocal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var keys []*wavespanv1.InspectKey
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			keys = append(keys, k)
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestInspectLocalRedactsByDefault(t *testing.T) {
	client, rs := newObsServer(t, nil)
	seedKV(t, rs, "k1", "secret")

	// reader, include_value -> still redacted (not admin)
	keys := inspectLocal(t, client, "reader", true)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
	if len(keys[0].GetValue()) != 0 {
		t.Fatal("non-admin must not see the value")
	}
	if keys[0].GetKeyHash() == "" {
		t.Fatal("key_hash must always be present")
	}

	// admin + include_value -> value revealed
	adminKeys := inspectLocal(t, client, "admin", true)
	if string(adminKeys[0].GetValue()) != "secret" {
		t.Fatalf("admin with include_value should see the value, got %q", adminKeys[0].GetValue())
	}

	// admin WITHOUT include_value -> still redacted
	noVal := inspectLocal(t, client, "admin", false)
	if len(noVal[0].GetValue()) != 0 {
		t.Fatal("include_value=false must redact even for admin")
	}
}

// membersCluster reports a fixed alive membership for cluster-wide fan-out tests.
type membersCluster struct{ members []membership.MemberView }

func (c membersCluster) Members() []membership.MemberView { return c.members }
func (membersCluster) Graph() *latencygraph.Graph {
	return latencygraph.New(latencygraph.DefaultConfig())
}

// fakeScanner returns canned ScanLocal rows per member id (the cluster fan-out target).
type fakeScanner struct {
	rows map[string][]*wavespanv1.ScanLocalRow
}

func (f fakeScanner) ScanLocal(_ context.Context, target membership.Member, _ string, _, _ []byte, _ int) ([]*wavespanv1.ScanLocalRow, error) {
	return f.rows[target.MemberID], nil
}

func TestInspectLocalClusterWideMergesAcrossNodes(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	seedKV(t, rs, "k1", "local") // node1 holds k1 at a normal (lower) version

	// node2 holds k1 at a HIGHER version (should win) plus k2 which node1 never saw.
	hi := &wavespanv1.Version{HlcPhysicalMs: 9_000_000_000_000, WriterClusterId: "dev", WriterMemberId: "node2"}
	scanner := fakeScanner{rows: map[string][]*wavespanv1.ScanLocalRow{
		"node2": {
			{Key: []byte("k1"), Value: []byte("peerwins"), Version: hi},
			{Key: []byte("k2"), Value: []byte("only-on-node2"), Version: hi},
		},
	}}
	cluster := membersCluster{members: []membership.MemberView{
		{Member: membership.Member{MemberID: "node1"}, State: membership.StateAlive},
		{Member: membership.Member{MemberID: "node2"}, State: membership.StateAlive},
	}}
	obs := NewObsService(NewGossipRing(64), cluster, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).WithClusterScan(scanner)
	mux := http.NewServeMux()
	mux.Handle(obs.Handler())
	ts := httptest.NewServer(security.Identity{DevMode: true}.EnforceHTTP(mux))
	t.Cleanup(ts.Close)
	client := wavespanv1connect.NewObservabilityServiceClient(ts.Client(), ts.URL)

	req := connect.NewRequest(&wavespanv1.InspectLocalRequest{Namespace: "default", IncludeValue: true, ClusterWide: true})
	req.Header().Set("X-WaveSpan-Role", "admin")
	stream, err := client.InspectLocal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]*wavespanv1.InspectKey{}
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			got[k.GetLogicalPath()] = k
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("cluster-wide browse should merge to 2 keys (k1,k2), got %d: %v", len(got), got)
	}
	k1 := got["default/k1"]
	if k1 == nil {
		t.Fatal("k1 missing")
	}
	if got, want := holderIDs(k1), []string{"node1", "node2"}; !equalStrings(got, want) {
		t.Fatalf("k1 holders = %v, want %v (both nodes hold it)", got, want)
	}
	if string(k1.GetValue()) != "peerwins" || k1.GetVersion().GetWriterMemberId() != "node2" {
		t.Fatalf("latest version should win for k1: value=%q writer=%q", k1.GetValue(), k1.GetVersion().GetWriterMemberId())
	}
	k2 := got["default/k2"]
	if k2 == nil || !equalStrings(holderIDs(k2), []string{"node2"}) || string(k2.GetValue()) != "only-on-node2" {
		t.Fatalf("k2 (peer-only) not surfaced correctly: %+v", k2)
	}
}

func holderIDs(k *wavespanv1.InspectKey) []string {
	var ids []string
	for _, h := range k.GetHolders() {
		ids = append(ids, h.GetMemberId())
	}
	return ids
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// fakeClusterResolver is a ClusterKeyResolver that returns canned results.
type fakeClusterResolver struct {
	holders  []*wavespanv1.InspectHolder
	best     *wavespanv1.StoredRecord
	complete bool
	warnings []string
}

func (f fakeClusterResolver) ResolveKey(_ context.Context, _ string, _ []byte, _ bool) ([]*wavespanv1.InspectHolder, *wavespanv1.StoredRecord, bool, []string) {
	return f.holders, f.best, f.complete, f.warnings
}

// fakePeerInspector is a PeerKeyInspector that returns canned results.
type fakePeerInspector struct {
	holders  []*wavespanv1.InspectHolder
	best     *wavespanv1.StoredRecord
	complete bool
	warnings []string
}

func (f fakePeerInspector) InspectPeers(_ context.Context, _ string, _ []byte, _ bool) ([]*wavespanv1.InspectHolder, *wavespanv1.StoredRecord, bool, []string) {
	return f.holders, f.best, f.complete, f.warnings
}

func TestInspectGlobalCompletenessOnMissedHolder(t *testing.T) {
	// Layer 1 resolver returns incomplete with a warning (mimics old fakeInspector behaviour).
	resolver := fakeClusterResolver{
		holders:  nil,
		best:     nil,
		complete: false,
		warnings: []string{"holder node2 unreachable"},
	}
	client, _ := newObsServer(t, resolver)

	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{Namespace: "default", Key: []byte("k1")})
	req.Header().Set("X-WaveSpan-Role", "reader")
	stream, err := client.InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var trailer *wavespanv1.InspectTrailer
	for stream.Receive() {
		if tr := stream.Msg().GetTrailer(); tr != nil {
			trailer = tr
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if trailer == nil || trailer.GetFinalCompleteness() != wavespanv1.Completeness_PARTIAL {
		t.Fatalf("an unreachable holder must yield PARTIAL completeness: %+v", trailer)
	}
	if len(trailer.GetWarnings()) == 0 {
		t.Fatal("a warning naming the unreachable holder is required")
	}
}

// TestInspectGlobal_BothLayersMergedAndSorted verifies that holders from Layer 1 and Layer 2 are
// merged and sorted by (peer_cluster_id, member_id), and that the best value is taken from
// whichever layer has the higher version (even when Layer 1 has no record for this key).
func TestInspectGlobal_BothLayersMergedAndSorted(t *testing.T) {
	l1version := &wavespanv1.Version{HlcPhysicalMs: 1_000, WriterClusterId: "dev", WriterMemberId: "node-a"}
	l2version := &wavespanv1.Version{HlcPhysicalMs: 9_000, WriterClusterId: "peer", WriterMemberId: "peer-z"}

	resolver := fakeClusterResolver{
		holders: []*wavespanv1.InspectHolder{
			{MemberId: "node-b", HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: l1version},
			{MemberId: "node-a", HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: l1version},
		},
		best: &wavespanv1.StoredRecord{
			Version: l1version,
			Value:   &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("l1-value")}},
		},
		complete: true,
		warnings: nil,
	}
	peer := fakePeerInspector{
		holders: []*wavespanv1.InspectHolder{
			{PeerClusterId: "peer", MemberId: "peer-z", HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: l2version},
		},
		best: &wavespanv1.StoredRecord{
			Version: l2version,
			Value:   &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("peer-value")}},
		},
		complete: true,
		warnings: nil,
	}

	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(64), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).
		WithClusterResolver(resolver).
		WithPeerInspector(peer)
	mux := http.NewServeMux()
	mux.Handle(obs.Handler())
	ts := httptest.NewServer(security.Identity{DevMode: true}.EnforceHTTP(mux))
	t.Cleanup(ts.Close)
	client := wavespanv1connect.NewObservabilityServiceClient(ts.Client(), ts.URL)

	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{
		Namespace:           "default",
		Key:                 []byte("mykey"),
		IncludeValue:        true,
		IncludePeerClusters: true,
	})
	req.Header().Set("X-WaveSpan-Role", "admin")
	stream, err := client.InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	var ik *wavespanv1.InspectKey
	var trailer *wavespanv1.InspectTrailer
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			ik = k
		}
		if tr := stream.Msg().GetTrailer(); tr != nil {
			trailer = tr
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}

	// All three holders must appear.
	if ik == nil {
		t.Fatal("expected an InspectKey row")
	}
	if len(ik.GetHolders()) != 3 {
		t.Fatalf("expected 3 holders (2 L1 + 1 L2), got %d: %+v", len(ik.GetHolders()), ik.GetHolders())
	}

	// Sorted: empty peer_cluster_id (L1) first, then "peer" cluster; within each group by member_id.
	h := ik.GetHolders()
	if h[0].GetMemberId() != "node-a" || h[0].GetPeerClusterId() != "" {
		t.Errorf("holder[0] should be node-a (L1), got %+v", h[0])
	}
	if h[1].GetMemberId() != "node-b" || h[1].GetPeerClusterId() != "" {
		t.Errorf("holder[1] should be node-b (L1), got %+v", h[1])
	}
	if h[2].GetMemberId() != "peer-z" || h[2].GetPeerClusterId() != "peer" {
		t.Errorf("holder[2] should be peer-z (L2), got %+v", h[2])
	}

	// The peer's higher version should win as best.
	if string(ik.GetValue()) != "peer-value" {
		t.Errorf("best value should come from the higher-versioned peer, got %q", ik.GetValue())
	}
	if ik.GetVersion().GetWriterMemberId() != "peer-z" {
		t.Errorf("best version writer should be peer-z, got %q", ik.GetVersion().GetWriterMemberId())
	}

	// Both layers complete => COMPLETE trailer.
	if trailer == nil || trailer.GetFinalCompleteness() != wavespanv1.Completeness_COMPLETE {
		t.Fatalf("both layers complete => COMPLETE trailer, got: %+v", trailer)
	}
	if trailer.GetRowsReturned() != 1 {
		t.Errorf("rows_returned should be 1, got %d", trailer.GetRowsReturned())
	}
}

// TestInspectGlobal_PartialWhenEitherLayerIncomplete verifies PARTIAL when Layer 2 is incomplete.
func TestInspectGlobal_PartialWhenEitherLayerIncomplete(t *testing.T) {
	resolver := fakeClusterResolver{
		holders:  []*wavespanv1.InspectHolder{{MemberId: "node-a"}},
		best:     &wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 1}},
		complete: true,
		warnings: nil,
	}
	peer := fakePeerInspector{
		holders:  nil,
		best:     nil,
		complete: false,
		warnings: []string{"peer cluster B unreachable"},
	}

	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(64), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).
		WithClusterResolver(resolver).
		WithPeerInspector(peer)
	mux := http.NewServeMux()
	mux.Handle(obs.Handler())
	ts := httptest.NewServer(security.Identity{DevMode: true}.EnforceHTTP(mux))
	t.Cleanup(ts.Close)
	client := wavespanv1connect.NewObservabilityServiceClient(ts.Client(), ts.URL)

	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{
		Namespace: "default", Key: []byte("k"), IncludePeerClusters: true,
	})
	req.Header().Set("X-WaveSpan-Role", "reader")
	stream, err := client.InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var trailer *wavespanv1.InspectTrailer
	for stream.Receive() {
		if tr := stream.Msg().GetTrailer(); tr != nil {
			trailer = tr
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if trailer == nil || trailer.GetFinalCompleteness() != wavespanv1.Completeness_PARTIAL {
		t.Fatalf("Layer 2 incomplete => PARTIAL, got: %+v", trailer)
	}
	warnFound := false
	for _, w := range trailer.GetWarnings() {
		if w == "peer cluster B unreachable" {
			warnFound = true
		}
	}
	if !warnFound {
		t.Errorf("Layer 2 warning not propagated: %v", trailer.GetWarnings())
	}
}

// TestInspectGlobal_NoPeerInspector verifies that without a peerInspector only L1 holders appear
// and a complete L1 result yields COMPLETE (regression guard: old stub always returned PARTIAL).
func TestInspectGlobal_NoPeerInspector(t *testing.T) {
	l1version := &wavespanv1.Version{HlcPhysicalMs: 5_000, WriterClusterId: "dev", WriterMemberId: "node-x"}
	resolver := fakeClusterResolver{
		holders: []*wavespanv1.InspectHolder{
			{MemberId: "node-x", HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE, Version: l1version},
		},
		best: &wavespanv1.StoredRecord{
			Version: l1version,
			Value:   &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("hello")}},
		},
		complete: true,
		warnings: nil,
	}
	client, _ := newObsServer(t, resolver) // no peerInspector wired

	req := connect.NewRequest(&wavespanv1.InspectGlobalRequest{
		Namespace: "default", Key: []byte("thekey"), IncludeValue: true, IncludePeerClusters: false,
	})
	req.Header().Set("X-WaveSpan-Role", "admin")
	stream, err := client.InspectGlobal(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	var ik *wavespanv1.InspectKey
	var trailer *wavespanv1.InspectTrailer
	for stream.Receive() {
		if k := stream.Msg().GetKey(); k != nil {
			ik = k
		}
		if tr := stream.Msg().GetTrailer(); tr != nil {
			trailer = tr
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}

	if ik == nil {
		t.Fatal("expected an InspectKey row")
	}
	if len(ik.GetHolders()) != 1 || ik.GetHolders()[0].GetMemberId() != "node-x" {
		t.Errorf("expected only L1 holder node-x, got %+v", ik.GetHolders())
	}
	// Regression guard: complete L1 without peer inspection MUST yield COMPLETE.
	if trailer == nil || trailer.GetFinalCompleteness() != wavespanv1.Completeness_COMPLETE {
		t.Fatalf("complete L1 without peer inspector must yield COMPLETE, got: %+v", trailer)
	}
}
