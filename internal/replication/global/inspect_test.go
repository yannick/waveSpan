package global

import (
	"context"
	"errors"
	"testing"

	"github.com/yannick/wavespan/internal/config"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
)

type fakeGRClient struct {
	resp *wavespanv1.InspectKeyResponse
	err  error
}

func (f *fakeGRClient) InspectKey(_ context.Context, _ *wavespanv1.InspectKeyRequest, _ ...grpc.CallOption) (*wavespanv1.InspectKeyResponse, error) {
	return f.resp, f.err
}

func TestPeerInspector_TagsAndAggregates(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-b", ReplEndpoint: "b1:7800"}}
	dial := func(_ string) (inspectKeyClient, error) {
		return &fakeGRClient{resp: &wavespanv1.InspectKeyResponse{
			Holders:  []*wavespanv1.InspectHolder{{MemberId: "b1", PeerClusterId: "test-b", Version: &wavespanv1.Version{HlcPhysicalMs: 9}}},
			Best:     &wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 9}},
			Complete: true,
		}}, nil
	}
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	holders, best, complete, warnings := pi.InspectPeers(context.Background(), "ns", []byte("k"), true)
	if !complete || len(warnings) != 0 {
		t.Fatalf("want complete no warnings, got %v %v", complete, warnings)
	}
	if len(holders) != 1 || holders[0].PeerClusterId != "test-b" {
		t.Fatalf("peer holder not surfaced/tagged: %v", holders)
	}
	if best.GetVersion().GetHlcPhysicalMs() != 9 {
		t.Fatalf("best not surfaced")
	}
}

func TestPeerInspector_UnreachablePeerPartial(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-b", ReplEndpoint: "b1:7800"}}
	dial := func(_ string) (inspectKeyClient, error) { return &fakeGRClient{err: errors.New("down")}, nil }
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	_, _, complete, warnings := pi.InspectPeers(context.Background(), "ns", []byte("k"), false)
	if complete || len(warnings) != 1 {
		t.Fatalf("want PARTIAL + warning, got complete=%v warns=%v", complete, warnings)
	}
}

func TestPeerInspector_SkipsSelfClusterAndEmptyEndpoint(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-a", ReplEndpoint: "a2:7800"}, {ClusterID: "test-b", ReplEndpoint: ""}}
	called := false
	dial := func(_ string) (inspectKeyClient, error) {
		called = true
		return &fakeGRClient{resp: &wavespanv1.InspectKeyResponse{Complete: true}}, nil
	}
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	_, _, complete, _ := pi.InspectPeers(context.Background(), "ns", []byte("k"), false)
	if called {
		t.Fatal("must not dial self-cluster or empty-endpoint peers")
	}
	if !complete {
		t.Fatal("no reachable peers => complete (nothing to be incomplete about)")
	}
}

func TestBuildPeerResponse_TagsSelfCluster(t *testing.T) {
	holders := []*wavespanv1.InspectHolder{{MemberId: "b1"}, {MemberId: "b2"}}
	best := &wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	resp := BuildPeerResponse("test-b", holders, best, true, nil)
	for _, h := range resp.GetHolders() {
		if h.GetPeerClusterId() != "test-b" {
			t.Fatalf("holder not tagged: %v", h)
		}
	}
	if !resp.GetComplete() || resp.GetBest() == nil {
		t.Fatal("completeness/best not carried")
	}
}
