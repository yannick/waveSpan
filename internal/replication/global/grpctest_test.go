package global

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcGlobalServer is a minimal in-test gRPC GlobalReplication server that delegates to the same
// applier/anti-entropy a *Server uses. Internal senders/reconcilers now dial peers over gRPC, so the
// tests must serve gRPC. It lives in this package (rather than reusing grpcsrv) to avoid the
// grpcsrv -> global -> grpcsrv test import cycle. The `up` flag gates PushGlobal so a disconnect can
// be simulated (mirroring the old httptest 503 gate).
type grpcGlobalServer struct {
	wavespanv1.UnimplementedGlobalReplicationServer
	applier *Applier
	ae      *AntiEntropy
	up      *atomic.Bool
}

func (s *grpcGlobalServer) PushGlobal(_ context.Context, req *wavespanv1.PushGlobalRequest) (*wavespanv1.PushGlobalAck, error) {
	if s.up != nil && !s.up.Load() {
		return nil, status.Error(codes.Unavailable, "peer down")
	}
	var applied uint64
	for _, m := range req.GetMutations() {
		ok, err := s.applier.Apply(m)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if ok {
			applied++
		}
	}
	return &wavespanv1.PushGlobalAck{AppliedCount: applied}, nil
}

func (s *grpcGlobalServer) RangeSummary(_ context.Context, req *wavespanv1.RangeSummaryRequest) (*wavespanv1.RangeSummaryResponse, error) {
	resp := &wavespanv1.RangeSummaryResponse{}
	if s.ae != nil {
		resp.Hashes = s.ae.Summarize(req.GetRanges())
	}
	return resp, nil
}

func (s *grpcGlobalServer) FetchRange(req *wavespanv1.FetchRangeRequest, stream grpc.ServerStreamingServer[wavespanv1.GlobalMutation]) error {
	if s.ae == nil {
		return nil
	}
	for _, m := range s.ae.FetchRange(req.GetRange()) {
		if err := stream.Send(m); err != nil {
			return err
		}
	}
	return nil
}

// serveGlobal serves the given server on a loopback port and returns its host:port; it is stopped
// via t.Cleanup.
func serveGlobal(t *testing.T, srv *grpcGlobalServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	wavespanv1.RegisterGlobalReplicationServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}
