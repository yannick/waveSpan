package rpcopts

import (
	"context"

	"google.golang.org/grpc"
)

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
