package rpcopts

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// grpcConnPool caches one *grpc.ClientConn per peer address. gRPC multiplexes all concurrent RPCs
// for a peer over that single connection, so a per-address cache (not per-call dial) is what keeps
// internal node→node traffic on shared, pooled HTTP/2 connections.
var (
	grpcConnMu   sync.Mutex
	grpcConnPool = map[string]*grpc.ClientConn{}
)

// GRPCConn returns a cached *grpc.ClientConn for addr (a host:port, NOT a "http://..." URL). The
// first call for an address dials it; subsequent calls return the same conn. The connection is
// cleartext/insecure for the dev/cluster path (production TLS for grpc clients is a separate
// follow-up). grpc.NewClient is lazy: it does not block on a live connection here; the first RPC
// drives connection establishment.
func GRPCConn(addr string) (grpc.ClientConnInterface, error) {
	grpcConnMu.Lock()
	defer grpcConnMu.Unlock()
	if c, ok := grpcConnPool[addr]; ok {
		return c, nil
	}
	// Use the passthrough resolver for a plain host:port (already-resolved advertise addr): grpc dials
	// the address directly via the OS resolver rather than grpc.NewClient's default dns resolver, which
	// can stall on a dual-stack "localhost" — and skips a needless DNS round-trip for IP addrs.
	target := addr
	if !strings.Contains(target, "://") {
		target = "passthrough:///" + target
	}
	cc, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	grpcConnPool[addr] = cc
	return cc, nil
}

// GRPCMetricsUnaryInterceptor returns a unary server interceptor that records each inbound RPC
// (by method and access kind) before invoking the handler. It mirrors the connect
// metricsInterceptor; observeRPC is a no-op until InstallMetrics is called.
func GRPCMetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		observeRPC(info.FullMethod)
		return handler(ctx, req)
	}
}

// GRPCMetricsStreamInterceptor returns a streaming server interceptor that records each inbound
// streaming RPC before invoking the handler.
func GRPCMetricsStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		observeRPC(info.FullMethod)
		return handler(srv, ss)
	}
}
