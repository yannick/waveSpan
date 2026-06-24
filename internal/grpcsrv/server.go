// Package grpcsrv builds the WaveSpan data-plane gRPC server, wiring the identity/authorization and
// RPC-metrics interceptors that previously lived as net/http middleware on the Connect server.
package grpcsrv

import (
	"context"
	"crypto/tls"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/security"
)

// Admission-control defaults (load shedding). A client flood must be REJECTED, not accepted unboundedly:
// an overloaded node that keeps accepting work marches toward OOM/timeout collapse and takes the tier
// with it. These bounds cap concurrency so excess load fast-fails with ResourceExhausted while the node
// stays up and serves admitted requests normally.
const (
	// DefaultMaxConcurrentStreams bounds simultaneous HTTP/2 streams per connection (a single client can't
	// open unbounded in-flight RPCs on one conn).
	DefaultMaxConcurrentStreams uint32 = 2048
	// DefaultMaxInflightUnary bounds total concurrent unary RPCs across all connections; past it, new
	// unary calls are shed with ResourceExhausted.
	DefaultMaxInflightUnary int64 = 4096
)

// Options configures the gRPC server.
type Options struct {
	// TLS, when non-nil, enables mTLS transport credentials; otherwise the server is insecure.
	TLS *tls.Config
	// Identity derives the caller's role and authorizes each procedure.
	Identity security.Identity
	// MaxConcurrentStreams bounds simultaneous HTTP/2 streams per connection (0 = default).
	MaxConcurrentStreams uint32
	// MaxInflightUnary bounds total concurrent unary RPCs; new calls past it are shed with
	// ResourceExhausted (0 = default, negative = disabled).
	MaxInflightUnary int64
}

// inflightLimitInterceptor returns a unary interceptor that admits at most limit concurrent unary RPCs,
// rejecting excess with codes.ResourceExhausted (graceful load shedding). A non-positive limit disables it.
func inflightLimitInterceptor(limit int64) grpc.UnaryServerInterceptor {
	var inflight atomic.Int64
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if limit <= 0 {
			return handler(ctx, req)
		}
		if n := inflight.Add(1); n > limit {
			inflight.Add(-1)
			return nil, status.Errorf(codes.ResourceExhausted, "wavespan: server overloaded (%d in flight), retry", n-1)
		}
		defer inflight.Add(-1)
		return handler(ctx, req)
	}
}

// New builds a *grpc.Server with the admission-control limiter, identity/authorization, and metrics
// interceptors chained (limiter first so a flood is shed before any auth/handler work) for both unary and
// streaming RPCs, plus a bounded MaxConcurrentStreams.
func New(opts Options) *grpc.Server {
	maxStreams := opts.MaxConcurrentStreams
	if maxStreams == 0 {
		maxStreams = DefaultMaxConcurrentStreams
	}
	maxInflight := opts.MaxInflightUnary
	if maxInflight == 0 {
		maxInflight = DefaultMaxInflightUnary
	}
	serverOpts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(maxStreams),
		grpc.ChainUnaryInterceptor(
			inflightLimitInterceptor(maxInflight),
			opts.Identity.GRPCUnaryInterceptor(),
			rpcopts.GRPCMetricsUnaryInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			opts.Identity.GRPCStreamInterceptor(),
			rpcopts.GRPCMetricsStreamInterceptor(),
		),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}
	return grpc.NewServer(serverOpts...)
}
