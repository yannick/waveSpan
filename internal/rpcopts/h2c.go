package rpcopts

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// WaveSpan's internal RPCs (and the dev/plaintext data port) ran over HTTP/1.1, which serializes
// in-flight requests per TCP connection — the throughput ceiling the profiler exposed (effective
// concurrency ~4.6 under 32 clients). HTTP/2 multiplexes hundreds of concurrent streams over one
// connection. For TLS the server already negotiates h2 via ALPN; on the plaintext dev/cluster path
// we use h2c (HTTP/2 cleartext) on both ends.

// H2CHandler wraps a handler so it serves BOTH h2c (HTTP/2 cleartext) and HTTP/1.1 — h2c clients are
// multiplexed; legacy HTTP/1.1 clients still work. Use only on plaintext servers (a TLS server gets
// HTTP/2 from ALPN automatically).
func H2CHandler(h http.Handler) http.Handler {
	return h2c.NewHandler(h, &http2.Server{MaxConcurrentStreams: 1024, IdleTimeout: 2 * time.Minute})
}

// sharedH2CClient multiplexes all plaintext internal/bench calls over pooled HTTP/2 connections.
var sharedH2CClient = &http.Client{
	Transport: &http2.Transport{
		AllowHTTP: true, // permit the "http" scheme
		// h2c: skip TLS, dial plain TCP. The cfg arg is ignored.
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
		ReadIdleTimeout:  30 * time.Second,
		PingTimeout:      10 * time.Second,
		WriteByteTimeout: 30 * time.Second,
	},
	Timeout: 30 * time.Second,
}

// H2CClient returns the shared h2c HTTP client (HTTP/2 cleartext, connection-multiplexed). Used by
// internal cluster clients and the benchmark so concurrent requests share connections instead of
// serializing on HTTP/1.1.
func H2CClient() *http.Client { return sharedH2CClient }

// sharedH2CClientNoTimeout shares the pooled h2c transport but imposes NO hard request Timeout, so a
// long-running call (e.g. a whole-namespace BulkRemove fan-out that proposes per collection) is
// bounded only by its context deadline rather than the 30s cap on the shared client.
var sharedH2CClientNoTimeout = &http.Client{Transport: sharedH2CClient.Transport}

// H2CClientNoTimeout returns an h2c client with no hard request timeout, for long calls bounded by
// their context deadline. It shares the connection pool with H2CClient.
func H2CClientNoTimeout() *http.Client { return sharedH2CClientNoTimeout }
