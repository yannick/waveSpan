package rpcopts

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Throughput/liveness tuning for internal node→node links (design/37 P1.6). These are shared by
// every pooled client conn; the server side mirrors them in grpcsrv.
const (
	// DialStreamWindow / DialConnWindow replace gRPC's 64KiB HTTP/2 flow-control defaults, which cap
	// per-stream throughput at window/RTT on any non-loopback link. Setting them explicitly disables
	// grpc-go's BDP auto-tuning, so they are sized for datacenter links, not WAN discovery.
	DialStreamWindow int32 = 1 << 20  // 1MiB per stream
	DialConnWindow   int32 = 16 << 20 // 16MiB per connection (many concurrent streams)
	// DialMaxRecvMsgSize raises the 4MiB default receive cap: batched replication (P1.4), backup
	// chunks, and merged scan responses can legitimately exceed it.
	DialMaxRecvMsgSize = 64 << 20
	// Keepalive: detect a dead peer (node loss, VPN restart — design/13) in ~25s instead of relying
	// on TCP timeouts. PermitWithoutStream keeps idle pooled conns probed so the first RPC after an
	// idle period doesn't eat the discovery timeout. The server's EnforcementPolicy MinTime must be
	// ≤ this Time or clients get GOAWAY-ed for pinging too often.
	DialKeepaliveTime    = 20 * time.Second
	DialKeepaliveTimeout = 5 * time.Second
)

// grpcConnPool caches one *grpc.ClientConn per peer address. gRPC multiplexes all concurrent RPCs
// for a peer over that single connection, so a per-address cache (not per-call dial) is what keeps
// internal node→node traffic on shared, pooled HTTP/2 connections.
var (
	grpcConnMu   sync.Mutex
	grpcConnPool = map[string]*grpc.ClientConn{}
	dialTLS      *tls.Config
)

// ConfigureDialTLS installs the client mTLS config used by every subsequent GRPCConn dial (nil =
// plaintext, dev mode only). Call it once at startup BEFORE anything dials: conns are cached, so a
// conn dialed earlier keeps its original credentials. It fails loudly on that misuse rather than
// leaving a silently-plaintext pooled conn behind.
func ConfigureDialTLS(cfg *tls.Config) {
	grpcConnMu.Lock()
	defer grpcConnMu.Unlock()
	if len(grpcConnPool) != 0 {
		panic("rpcopts: ConfigureDialTLS called after connections were dialed")
	}
	dialTLS = cfg
}

// GRPCConn returns a cached *grpc.ClientConn for addr (a host:port, NOT a "http://..." URL). The
// first call for an address dials it; subsequent calls return the same conn. Credentials come from
// ConfigureDialTLS (mTLS with session resumption in production, plaintext only in dev mode), and
// the conn carries the datacenter tuning above. grpc.NewClient is lazy: it does not block on a live
// connection here; the first RPC drives connection establishment.
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
	creds := insecure.NewCredentials()
	if dialTLS != nil {
		creds = credentials.NewTLS(dialTLS)
	}
	cc, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithInitialWindowSize(DialStreamWindow),
		grpc.WithInitialConnWindowSize(DialConnWindow),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(DialMaxRecvMsgSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                DialKeepaliveTime,
			Timeout:             DialKeepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
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
