// Package grpcsrv builds the WaveSpan data-plane gRPC server, wiring the identity/authorization and
// RPC-metrics interceptors that previously lived as net/http middleware on the Connect server.
package grpcsrv

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/security"
)

// Options configures the gRPC server.
type Options struct {
	// TLS, when non-nil, enables mTLS transport credentials; otherwise the server is insecure.
	TLS *tls.Config
	// Identity derives the caller's role and authorizes each procedure.
	Identity security.Identity
}

// New builds a *grpc.Server with the identity/authorization and metrics interceptors chained
// (auth first, then metrics) for both unary and streaming RPCs.
func New(opts Options) *grpc.Server {
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
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
