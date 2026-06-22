package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// ClusterSource snapshots membership + the latency graph (satisfied by membership.Service).
type ClusterSource interface {
	Members() []membership.MemberView
	Graph() *latencygraph.Graph
}

// GlobalInspector resolves a key's holders across the cluster (and peer clusters) for InspectGlobal
// (implemented in M13.B). It is best-effort/eventual.
type GlobalInspector interface {
	InspectKey(ctx context.Context, namespace string, key []byte, includePeerClusters, includeValue bool) (holders []*wavespanv1.InspectHolder, warnings []string, complete bool)
}

// ObsService is the ObservabilityService handler powering the embedded UI and wavespanctl
// introspection (design/26). It is mounted on the admin port behind admin auth; values are redacted
// by default.
type ObsService struct {
	ring            *GossipRing
	cluster         ClusterSource
	self            membership.Member
	rstore          *recordstore.Store
	underReplicated func() uint64

	// InspectGlobal support (M13.B); nil disables cross-cluster resolution.
	globalInspector GlobalInspector
	// Visual node explorer support; nil disables GraphExplore.
	graph *graph.Store
}

// NewObsService wires the observability service.
func NewObsService(ring *GossipRing, cluster ClusterSource, self membership.Member, rstore *recordstore.Store) *ObsService {
	return &ObsService{ring: ring, cluster: cluster, self: self, rstore: rstore}
}

// WithUnderReplicated supplies the under-replication estimate for the cluster view.
func (s *ObsService) WithUnderReplicated(fn func() uint64) *ObsService {
	s.underReplicated = fn
	return s
}

// WithGlobalInspector enables InspectGlobal cross-holder resolution.
func (s *ObsService) WithGlobalInspector(g GlobalInspector) *ObsService {
	s.globalInspector = g
	return s
}

// StreamGossip subscribes to the gossip ring (with optional backfill) and streams events until the
// client disconnects.
func (s *ObsService) StreamGossip(ctx context.Context, req *connect.Request[wavespanv1.StreamGossipRequest], stream *connect.ServerStream[wavespanv1.GossipEvent]) error {
	ch, cancel := s.ring.Subscribe(req.Msg.GetFilter(), req.Msg.GetBackfill())
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(e); err != nil {
				return err
			}
		}
	}
}

// GetClusterView snapshots membership, the latency graph, and repair pressure (design/26).
func (s *ObsService) GetClusterView(_ context.Context, _ *connect.Request[wavespanv1.GetClusterViewRequest]) (*connect.Response[wavespanv1.GetClusterViewResponse], error) {
	resp := &wavespanv1.GetClusterViewResponse{}
	for _, mv := range s.cluster.Members() {
		resp.Members = append(resp.Members, membership.MemberStateToProto(mv))
	}
	for _, e := range s.cluster.Graph().Edges() {
		resp.Edges = append(resp.Edges, &wavespanv1.LatencyEdge{
			FromMemberId: s.self.MemberID, ToMemberId: e.To,
			EwmaRttMs: e.EWMARttMs, P95RttMs: e.P95RttMs, PacketLoss: e.PacketLoss,
			LastSuccessUnixMs: e.LastSuccessMs, LastFailureUnixMs: e.LastFailureMs, SampleCount: e.SampleCount,
		})
	}
	if s.underReplicated != nil {
		resp.UnderReplicatedEstimate = s.underReplicated()
	}
	return connect.NewResponse(resp), nil
}
