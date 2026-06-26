package grpcsrv

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/replication/global"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// PeerKeyInspector runs this cluster's within-cluster resolution for one key and returns a
// peer-facing InspectKey response (holders tagged with this cluster's id). Satisfied by a small
// adapter over holderinspect.ClusterResolver wired in main.go. nil disables InspectKey.
type PeerKeyInspector interface {
	InspectKeyLocal(ctx context.Context, namespace string, key []byte, includeValue bool) (*wavespanv1.InspectKeyResponse, error)
}

// GlobalReplication is the gRPC GlobalReplication adapter (inter-cluster, data-port). It mirrors the
// Connect Server in internal/replication/global, delegating to the SAME exported cores: an *Applier
// for idempotent inbound mutation application and an optional *AntiEntropy for range summaries and
// range fetches. Only the transport (gRPC vs Connect) differs.
type GlobalReplication struct {
	wavespanv1.UnimplementedGlobalReplicationServer
	applier *global.Applier
	ae      *global.AntiEntropy
	peer    PeerKeyInspector
}

// NewGlobalReplication wires the gRPC GlobalReplication adapter over the same dependencies the Connect
// Server takes (see global.NewServer). ae may be nil (RangeSummary/FetchRange become no-ops); peer
// may be nil (InspectKey returns Unimplemented).
func NewGlobalReplication(applier *global.Applier, ae *global.AntiEntropy, peer PeerKeyInspector) *GlobalReplication {
	return &GlobalReplication{applier: applier, ae: ae, peer: peer}
}

// PushGlobal applies a batch of inbound mutations from a peer cluster.
func (s *GlobalReplication) PushGlobal(_ context.Context, m *wavespanv1.PushGlobalRequest) (*wavespanv1.PushGlobalAck, error) {
	var applied uint64
	for _, mut := range m.GetMutations() {
		ok, err := s.applier.Apply(mut)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if ok {
			applied++
		}
	}
	return &wavespanv1.PushGlobalAck{AppliedCount: applied}, nil
}

// RangeSummary returns per-range content hashes for anti-entropy comparison.
func (s *GlobalReplication) RangeSummary(_ context.Context, m *wavespanv1.RangeSummaryRequest) (*wavespanv1.RangeSummaryResponse, error) {
	resp := &wavespanv1.RangeSummaryResponse{}
	if s.ae != nil {
		resp.Hashes = s.ae.Summarize(m.GetRanges())
	}
	return resp, nil
}

// InspectKey answers a peer cluster's "who holds this key?" by running this cluster's within-
// cluster resolution (Layer 1) and tagging holders with this cluster's id.
func (s *GlobalReplication) InspectKey(ctx context.Context, m *wavespanv1.InspectKeyRequest) (*wavespanv1.InspectKeyResponse, error) {
	if s.peer == nil {
		return nil, status.Error(codes.Unimplemented, "peer inspect not configured")
	}
	return s.peer.InspectKeyLocal(ctx, m.GetNamespace(), m.GetKey(), m.GetIncludeValue())
}

// FetchRange streams the local records in a range so a diverged peer can repair. It delegates to the
// SAME producer (ae.FetchRange) used by the Connect handler, forwarding each mutation via stream.Send.
// Like the Connect impl, the producer materializes the range up front, so this terminates after the
// last mutation; a stream.Send error (e.g. client cancellation) aborts the stream.
func (s *GlobalReplication) FetchRange(m *wavespanv1.FetchRangeRequest, stream grpc.ServerStreamingServer[wavespanv1.GlobalMutation]) error {
	if s.ae == nil {
		return nil
	}
	for _, mut := range s.ae.FetchRange(m.GetRange()) {
		if err := stream.Send(mut); err != nil {
			return err
		}
	}
	return nil
}
