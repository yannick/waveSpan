package observability

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// newGraphWriteObs wires an ObsService with both a graph store and a version stamper, enabling
// AdminPutGraph (mirrors the construction style in inspect_graph_test.go + WithSampleDataset).
func newGraphWriteObs(t *testing.T) (*ObsService, *graph.Store) {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	gs := graph.NewStore(mem)
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	var nextHLC uint64
	newVersion := func() *wavespanv1.Version {
		nextHLC++
		return &wavespanv1.Version{HlcPhysicalMs: nextHLC}
	}
	obs := NewObsService(NewGossipRing(8), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs).
		WithGraph(gs).
		WithSampleDataset(true, newVersion)
	return obs, gs
}

func putGraph(t *testing.T, obs *ObsService, req *wavespanv1.AdminPutGraphRequest) *wavespanv1.AdminPutGraphResponse {
	t.Helper()
	resp, err := obs.AdminPutGraph(context.Background(), connect.NewRequest(req))
	if err != nil {
		t.Fatalf("AdminPutGraph transport error: %v", err)
	}
	return resp.Msg
}

func TestAdminPutGraphUpsertNode(t *testing.T) {
	obs, gs := newGraphWriteObs(t)
	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		GraphId: "g",
		Target: &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{
			NodeId: "a",
			Labels: []string{"User", "Admin"},
			Properties: map[string]*wavespanv1.Value{
				"name": {Value: &wavespanv1.Value_StringValue{StringValue: "alice"}},
				"age":  {Value: &wavespanv1.Value_IntValue{IntValue: 30}},
			},
		}},
	})
	if !resp.GetOk() || resp.GetError() != "" {
		t.Fatalf("expected ok upsert, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}
	if resp.GetVersion() == nil {
		t.Fatal("expected a stamped version in the response")
	}

	got, found, err := gs.GetNode("g", "a")
	if err != nil || !found {
		t.Fatalf("GetNode after upsert: found=%v err=%v", found, err)
	}
	if len(got.GetLabels()) != 2 || got.GetLabels()[0] != "User" || got.GetLabels()[1] != "Admin" {
		t.Fatalf("labels did not round-trip: %v", got.GetLabels())
	}
	if got.GetProperties()["name"].GetStringValue() != "alice" {
		t.Fatalf("name prop did not round-trip: %v", got.GetProperties())
	}
	if got.GetProperties()["age"].GetIntValue() != 30 {
		t.Fatalf("age prop did not round-trip: %v", got.GetProperties())
	}
	if got.GetVersion().GetHlcPhysicalMs() == 0 {
		t.Fatal("node should carry the stamped version")
	}

	all, err := gs.AllNodes("g")
	if err != nil || len(all) != 1 {
		t.Fatalf("AllNodes: n=%d err=%v", len(all), err)
	}
}

func TestAdminPutGraphUpsertEdge(t *testing.T) {
	obs, gs := newGraphWriteObs(t)
	for _, id := range []string{"a", "b"} {
		putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
			GraphId: "g",
			Target:  &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{NodeId: id, Labels: []string{"N"}}},
		})
	}
	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		GraphId: "g",
		Target: &wavespanv1.AdminPutGraphRequest_Edge{Edge: &wavespanv1.EdgeRecord{
			EdgeId: "a|FOLLOWS|b", StartNode: "a", EndNode: "b", Type: "FOLLOWS",
		}},
	})
	if !resp.GetOk() || resp.GetError() != "" {
		t.Fatalf("expected ok edge upsert, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}

	got, found, err := gs.GetEdge("g", "a|FOLLOWS|b")
	if err != nil || !found {
		t.Fatalf("GetEdge after upsert: found=%v err=%v", found, err)
	}
	if got.GetType() != "FOLLOWS" || got.GetStartNode() != "a" || got.GetEndNode() != "b" {
		t.Fatalf("edge did not round-trip: %+v", got)
	}
	out, err := gs.ScanOutgoing("g", "a", "")
	if err != nil || len(out) != 1 {
		t.Fatalf("ScanOutgoing: n=%d err=%v", len(out), err)
	}
}

func TestAdminPutGraphDeleteNodeTombstones(t *testing.T) {
	obs, gs := newGraphWriteObs(t)
	putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		GraphId: "g",
		Target:  &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{NodeId: "a", Labels: []string{"N"}}},
	})
	if _, found, _ := gs.GetNode("g", "a"); !found {
		t.Fatal("node should exist before delete")
	}

	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		GraphId: "g",
		Delete:  true,
		Target:  &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{NodeId: "a"}},
	})
	if !resp.GetOk() || resp.GetError() != "" {
		t.Fatalf("expected ok delete, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}
	// GetNode filters tombstones, so the node must now read as absent.
	if _, found, err := gs.GetNode("g", "a"); found || err != nil {
		t.Fatalf("deleted node should be tombstoned (not live): found=%v err=%v", found, err)
	}
	if all, _ := gs.AllNodes("g"); len(all) != 0 {
		t.Fatalf("AllNodes should exclude the tombstoned node, got %d", len(all))
	}
}

func TestAdminPutGraphEmptyGraphIDError(t *testing.T) {
	obs, _ := newGraphWriteObs(t)
	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		Target: &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{NodeId: "a"}},
	})
	if resp.GetOk() || resp.GetError() == "" {
		t.Fatalf("empty graph_id should be an error body, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}
}

func TestAdminPutGraphExactlyOneTargetError(t *testing.T) {
	obs, _ := newGraphWriteObs(t)
	// Neither node nor edge set.
	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{GraphId: "g"})
	if resp.GetOk() || resp.GetError() == "" {
		t.Fatalf("missing target should be an error body, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}
}

func TestAdminPutGraphNotEnabledError(t *testing.T) {
	// An ObsService without WithGraph/WithSampleDataset has neither graph nor newGraphVersion set.
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	rs := recordstore.NewStore(mem, "dev", "node1", version.NewClock(nil, 500), version.NewSequencer(0))
	obs := NewObsService(NewGossipRing(8), fakeCluster{}, membership.Member{ClusterID: "dev", MemberID: "node1"}, rs)

	resp := putGraph(t, obs, &wavespanv1.AdminPutGraphRequest{
		GraphId: "g",
		Target:  &wavespanv1.AdminPutGraphRequest_Node{Node: &wavespanv1.NodeRecord{NodeId: "a"}},
	})
	if resp.GetOk() || resp.GetError() != "graph write not enabled" {
		t.Fatalf("expected graph-not-enabled error body, got ok=%v error=%q", resp.GetOk(), resp.GetError())
	}
}
