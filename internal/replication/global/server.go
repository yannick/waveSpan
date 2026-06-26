package global

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Server is the GlobalReplication Connect handler: it applies inbound mutations idempotently and
// serves anti-entropy range queries (design/06). It is mounted on the data port.
type Server struct {
	applier *Applier
	ae      *AntiEntropy // optional; nil until M7.D wires it
}

// NewServer builds the GlobalReplication server over an applier.
func NewServer(applier *Applier, ae *AntiEntropy) *Server {
	return &Server{applier: applier, ae: ae}
}

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *Server) Handler() (string, http.Handler) {
	return wavespanv1connect.NewGlobalReplicationHandler(s, rpcopts.Handler()...)
}

// PushGlobal applies a batch of inbound mutations from a peer cluster.
func (s *Server) PushGlobal(_ context.Context, req *connect.Request[wavespanv1.PushGlobalRequest]) (*connect.Response[wavespanv1.PushGlobalAck], error) {
	var applied uint64
	for _, m := range req.Msg.GetMutations() {
		ok, err := s.applier.Apply(m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if ok {
			applied++
		}
	}
	return connect.NewResponse(&wavespanv1.PushGlobalAck{AppliedCount: applied}), nil
}

// RangeSummary returns per-range content hashes for anti-entropy comparison.
func (s *Server) RangeSummary(_ context.Context, req *connect.Request[wavespanv1.RangeSummaryRequest]) (*connect.Response[wavespanv1.RangeSummaryResponse], error) {
	resp := &wavespanv1.RangeSummaryResponse{}
	if s.ae != nil {
		resp.Hashes = s.ae.Summarize(req.Msg.GetRanges())
	}
	return connect.NewResponse(resp), nil
}

// FetchRange streams the local records in a range so a diverged peer can repair.
func (s *Server) FetchRange(_ context.Context, req *connect.Request[wavespanv1.FetchRangeRequest], stream *connect.ServerStream[wavespanv1.GlobalMutation]) error {
	if s.ae == nil {
		return nil
	}
	for _, m := range s.ae.FetchRange(req.Msg.GetRange()) {
		if err := stream.Send(m); err != nil {
			return err
		}
	}
	return nil
}

// InspectKey is a stub for the Global Data Browser cross-cluster key inspection RPC (design/26).
// The handler implementation is added in a later task.
func (s *Server) InspectKey(_ context.Context, _ *connect.Request[wavespanv1.InspectKeyRequest]) (*connect.Response[wavespanv1.InspectKeyResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
