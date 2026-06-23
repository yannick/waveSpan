package observability

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/security"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func node(g, id string, labels []string, props map[string]*wavespanv1.Value) *wavespanv1.NodeRecord {
	return &wavespanv1.NodeRecord{GraphId: g, NodeId: id, Labels: labels, Properties: props, Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
}

func newGraphObs(t *testing.T) (*ObsService, *graph.Store) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	gs := graph.NewStore(mem)
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(8), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).WithGraph(gs)
	return obs, gs
}

func explore(t *testing.T, obs *ObsService, req *wavespanv1.GraphExploreRequest, role string) *wavespanv1.GraphExploreResponse {
	t.Helper()
	ctx := context.Background()
	if role != "" {
		ctx = security.WithRole(ctx, security.Role(role))
	}
	resp, err := obs.GraphExplore(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	return resp.Msg
}

func TestGraphExploreWholeGraph(t *testing.T) {
	obs, gs := newGraphObs(t)
	_ = gs.CreateNode(node("g", "a", []string{"User"}, map[string]*wavespanv1.Value{"name": {Value: &wavespanv1.Value_StringValue{StringValue: "alice"}}}))
	_ = gs.CreateNode(node("g", "b", []string{"User"}, nil))
	_ = gs.CreateEdge(&wavespanv1.EdgeRecord{GraphId: "g", EdgeId: "a|FOLLOWS|b", StartNode: "a", EndNode: "b", Type: "FOLLOWS", Version: &wavespanv1.Version{HlcPhysicalMs: 1}})

	resp := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g"}, "reader")
	if len(resp.GetNodes()) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(resp.GetNodes()))
	}
	if len(resp.GetEdges()) != 1 || resp.GetEdges()[0].GetType() != "FOLLOWS" {
		t.Fatalf("expected the FOLLOWS edge: %+v", resp.GetEdges())
	}
	// reader role -> properties redacted (not revealed)
	for _, n := range resp.GetNodes() {
		if len(n.GetProperties()) != 0 {
			t.Fatalf("non-admin must not see node properties: %+v", n)
		}
	}
	// admin + include_value -> properties revealed
	adm := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g", IncludeValue: true}, "admin")
	revealed := false
	for _, n := range adm.GetNodes() {
		if n.GetNodeId() == "a" && n.GetProperties()["name"].GetStringValue() == "alice" {
			revealed = true
		}
	}
	if !revealed {
		t.Fatal("admin with include_value should see node properties")
	}
}

func TestGraphExploreBFSFromSeed(t *testing.T) {
	obs, gs := newGraphObs(t)
	for _, id := range []string{"a", "b", "c", "d"} {
		_ = gs.CreateNode(node("g", id, []string{"N"}, nil))
	}
	mk := func(s, d string) {
		_ = gs.CreateEdge(&wavespanv1.EdgeRecord{GraphId: "g", EdgeId: s + "->" + d, StartNode: s, EndNode: d, Type: "E", Version: &wavespanv1.Version{HlcPhysicalMs: 1}})
	}
	mk("a", "b")
	mk("b", "c")
	mk("c", "d")

	// depth 1 from a -> {a, b}
	r1 := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g", SeedNodeId: "a", Depth: 1}, "reader")
	if len(r1.GetNodes()) != 2 {
		t.Fatalf("depth-1 BFS from a should reach 2 nodes, got %d", len(r1.GetNodes()))
	}
	// depth 2 from a -> {a, b, c}
	r2 := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g", SeedNodeId: "a", Depth: 2}, "reader")
	if len(r2.GetNodes()) != 3 {
		t.Fatalf("depth-2 BFS from a should reach 3 nodes, got %d", len(r2.GetNodes()))
	}
}

func TestGraphExploreLimitTruncates(t *testing.T) {
	obs, gs := newGraphObs(t)
	for i := 0; i < 10; i++ {
		_ = gs.CreateNode(node("g", string(rune('a'+i)), nil, nil))
	}
	resp := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g", Limit: 3}, "reader")
	if len(resp.GetNodes()) != 3 || !resp.GetTruncated() {
		t.Fatalf("limit should cap at 3 and set truncated: nodes=%d truncated=%v", len(resp.GetNodes()), resp.GetTruncated())
	}
}
