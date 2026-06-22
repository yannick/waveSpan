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

// holderScanner scans a remote member's local store over a subrange — the same fan-out the routed KV
// scanner uses (internal/kv/scan.go). It powers cluster-wide InspectLocal so the Data Browser can
// see keys on every node, not just this one. Satisfied by *local.ConnectReplicator.
type holderScanner interface {
	ScanLocal(ctx context.Context, target membership.Member, namespace string, start, end []byte, limit int) ([]*wavespanv1.ScanLocalRow, error)
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
	// Cluster-wide InspectLocal fan-out; nil disables cluster_wide (falls back to local-only).
	clusterScan holderScanner
	// Test/admin KV write forwarder; nil disables AdminPut. Forwards a Put to a chosen member's
	// data port so that member coordinates the write.
	kvWriter KvWriter
	// Test/admin KV delete forwarder; nil disables AdminDelete. Forwards a Delete (tombstone) to a
	// chosen member's data port, mirroring kvWriter.
	kvDeleter KvDeleter
}

// KvWriter forwards a KV Put to a target member's data-port KvService, returning the result. It lets
// the node UI write a record coordinated by a chosen cluster member (design/26). Wired in the node
// from the shared replication HTTP client.
type KvWriter func(ctx context.Context, target membership.Member, req *wavespanv1.PutRequest) (*wavespanv1.PutResult, error)

// KvDeleter forwards a KV Delete (tombstone write) to a target member's data-port KvService. It lets
// the node UI delete a record from the Data Browser coordinated by a chosen cluster member.
type KvDeleter func(ctx context.Context, target membership.Member, req *wavespanv1.DeleteRequest) (*wavespanv1.DeleteResult, error)

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

// WithClusterScan enables cluster-wide InspectLocal: a fan-out scan over every alive member so the
// Data Browser shows the whole cluster's KV, not just this node's.
func (s *ObsService) WithClusterScan(h holderScanner) *ObsService {
	s.clusterScan = h
	return s
}

// WithKvWriter enables AdminPut: the node-UI KV write tool that forwards to a chosen coordinator.
func (s *ObsService) WithKvWriter(w KvWriter) *ObsService {
	s.kvWriter = w
	return s
}

// WithKvDeleter enables AdminDelete: the Data Browser delete action, forwarded to a chosen coordinator.
func (s *ObsService) WithKvDeleter(d KvDeleter) *ObsService {
	s.kvDeleter = d
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
