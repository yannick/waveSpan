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

func newSampleObs(t *testing.T, enabled bool) *ObsService {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	gs := graph.NewStore(mem)
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	return NewObsService(NewGossipRing(8), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).
		WithGraph(gs).
		WithSampleDataset(enabled, func() *wavespanv1.Version { return rs.NextVersion().ToProto() })
}

func adminContext() context.Context {
	return security.WithRole(context.Background(), security.RoleAdmin)
}

func TestLoadSampleDatasetPopulatesGraph(t *testing.T) {
	obs := newSampleObs(t, true)
	resp, err := obs.LoadSampleDataset(adminContext(), connect.NewRequest(&wavespanv1.LoadSampleDatasetRequest{GraphId: "g"}))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Msg.GetOk() {
		t.Fatalf("expected ok, got error %q", resp.Msg.GetError())
	}
	if resp.Msg.GetNodesCreated() == 0 || resp.Msg.GetEdgesCreated() == 0 {
		t.Fatalf("expected nodes+edges created, got %d/%d", resp.Msg.GetNodesCreated(), resp.Msg.GetEdgesCreated())
	}
	if resp.Msg.GetDatasetName() != "Movies" {
		t.Fatalf("dataset name = %q, want Movies", resp.Msg.GetDatasetName())
	}

	// A known hub resolves and its film neighbours are reachable at depth 1, with admin property reveal.
	exp := explore(t, obs, &wavespanv1.GraphExploreRequest{GraphId: "g", SeedNodeId: "TomHanks", Depth: 1, IncludeValue: true}, "admin")
	if len(exp.GetNodes()) < 4 {
		t.Fatalf("Tom Hanks depth-1 should include several films, got %d nodes", len(exp.GetNodes()))
	}
	sawName := false
	for _, n := range exp.GetNodes() {
		if n.GetNodeId() == "TomHanks" && n.GetProperties()["name"].GetStringValue() == "Tom Hanks" {
			sawName = true
		}
	}
	if !sawName {
		t.Fatal("admin explore should reveal Tom Hanks' name property")
	}
}

func TestLoadSampleDatasetDisabledReturnsError(t *testing.T) {
	obs := newSampleObs(t, false)
	resp, err := obs.LoadSampleDataset(adminContext(), connect.NewRequest(&wavespanv1.LoadSampleDatasetRequest{}))
	if err != nil {
		t.Fatalf("disabled loader should not be a transport error: %v", err)
	}
	if resp.Msg.GetOk() {
		t.Fatal("disabled loader must return ok=false")
	}
	if resp.Msg.GetError() == "" {
		t.Fatal("disabled loader must explain itself via the error field")
	}
}

func TestGraphSubgraphInducesEdgesAndExpands(t *testing.T) {
	obs := newSampleObs(t, true)
	if _, err := obs.LoadSampleDataset(adminContext(), connect.NewRequest(&wavespanv1.LoadSampleDatasetRequest{GraphId: "g"})); err != nil {
		t.Fatal(err)
	}

	// Two adjacent nodes (Keanu ACTED_IN The Matrix) + one bogus id: only the 2 real nodes return,
	// and the edge between them appears even at neighbor_depth 0.
	resp, err := obs.GraphSubgraph(adminContext(), connect.NewRequest(&wavespanv1.GraphSubgraphRequest{
		GraphId: "g", NodeIds: []string{"KeanuReeves", "TheMatrix", "doesNotExist"}, IncludeValue: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Msg.GetNodes()) != 2 {
		t.Fatalf("expected exactly the 2 resolvable nodes, got %d", len(resp.Msg.GetNodes()))
	}
	foundEdge := false
	for _, e := range resp.Msg.GetEdges() {
		if e.GetType() == "ACTED_IN" && e.GetSource() == "KeanuReeves" && e.GetTarget() == "TheMatrix" {
			foundEdge = true
		}
	}
	if !foundEdge {
		t.Fatalf("induced subgraph must contain the ACTED_IN edge: %+v", resp.Msg.GetEdges())
	}

	// neighbor_depth 1 from just The Matrix pulls in its cast/crew.
	expanded, err := obs.GraphSubgraph(adminContext(), connect.NewRequest(&wavespanv1.GraphSubgraphRequest{
		GraphId: "g", NodeIds: []string{"TheMatrix"}, NeighborDepth: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(expanded.Msg.GetNodes()) < 5 {
		t.Fatalf("neighbor_depth 1 from The Matrix should pull several people, got %d", len(expanded.Msg.GetNodes()))
	}
}
