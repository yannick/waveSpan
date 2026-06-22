package security

import (
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// TransportTuning controls connection-pool behaviour for the shared inter-node HTTP client.
//
// The design goal (design/27 "Transport performance") is to make mTLS as cheap as possible WITHOUT
// weakening encryption. With AES-NI, bulk AEAD encryption runs at multiple GB/s and is negligible
// next to storage I/O; the real cost of TLS is the per-connection handshake (asymmetric crypto +
// round trips + certificate verification). So instead of lowering the cipher, we keep connections
// long-lived, pooled, and multiplexed (HTTP/2 via ALPN) so the handshake is amortised to ~zero
// across the many RPCs WaveSpan issues between nodes (gossip ticks, replication fanout, cache
// fetch/subscribe, vector scatter). TLS 1.3 session resumption (see ClientTLS) further removes the
// full handshake from the rare reconnect.
type TransportTuning struct {
	MaxIdleConns        int           // total idle connections kept warm across all peers
	MaxIdleConnsPerHost int           // idle connections kept warm per peer (high: peers are long-lived)
	IdleConnTimeout     time.Duration // how long an idle connection survives before close (long: survive bursts)
	TCPKeepAlive        time.Duration // OS-level TCP keepalive probe interval (keeps NAT/conntrack warm)
	DialTimeout         time.Duration // bound on establishing a new connection (incl. handshake)
	H2ReadIdleTimeout   time.Duration // send an HTTP/2 PING if no frame arrives for this long (0 disables)
	H2PingTimeout       time.Duration // close the connection if a PING is not answered within this long
}

// DefaultTransportTuning returns the cheap-mTLS defaults documented in design/27. They favour
// keeping connections warm for a long time so reconnect handshakes are rare.
func DefaultTransportTuning() TransportTuning {
	return TransportTuning{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     10 * time.Minute,
		TCPKeepAlive:        30 * time.Second,
		DialTimeout:         5 * time.Second,
		H2ReadIdleTimeout:   30 * time.Second,
		H2PingTimeout:       15 * time.Second,
	}
}

// configureH2KeepAlive enables HTTP/2 on tr and applies application-level PING keepalive. Unlike TCP
// keepalive, h2 PINGs detect a half-open multiplexed connection (e.g. a silently dropped peer or a
// stale NAT mapping) and evict it, so the next RPC dials a fresh healthy connection instead of
// hanging on a dead one — important because we keep connections warm for a long time. It returns the
// *http2.Transport so the settings are inspectable in tests; callers may ignore it.
func configureH2KeepAlive(tr *http.Transport, t TransportTuning) (*http2.Transport, error) {
	h2, err := http2.ConfigureTransports(tr)
	if err != nil {
		return nil, err
	}
	h2.ReadIdleTimeout = t.H2ReadIdleTimeout
	h2.PingTimeout = t.H2PingTimeout
	return h2, nil
}

// NewHTTPClient builds the single shared client used for ALL inter-node Connect RPCs. Sharing one
// pooled client across gossip, replication, cache, and vector scatter is what lets connections (and
// their completed TLS handshakes) be reused instead of re-established per subsystem.
//
// When TLS material is configured it dials mTLS with TLS 1.3 + session resumption and negotiates
// HTTP/2 via ALPN; in insecureDevMode with no certs it returns an equivalent plaintext pooled
// client so dev/compose keeps working. No Client.Timeout is set: cache subscriptions are long-lived
// server streams and rely on context cancellation, not a blanket deadline.
func (c TLSConfig) NewHTTPClient(t TransportTuning) (*http.Client, error) {
	dialer := &net.Dialer{Timeout: t.DialTimeout, KeepAlive: t.TCPKeepAlive}
	tr := &http.Transport{
		DialContext:         dialer.DialContext,
		ForceAttemptHTTP2:   true, // multiplex many RPCs over one connection -> one handshake
		MaxIdleConns:        t.MaxIdleConns,
		MaxIdleConnsPerHost: t.MaxIdleConnsPerHost,
		IdleConnTimeout:     t.IdleConnTimeout,
	}
	switch {
	case c.CertFile != "" || c.KeyFile != "" || c.CAFile != "":
		tlsCfg, err := c.ClientTLS()
		if err != nil {
			return nil, err
		}
		tr.TLSClientConfig = tlsCfg
		if _, err := configureH2KeepAlive(tr, t); err != nil { // h2 PING keepalive on the TLS path
			return nil, err
		}
	case !c.InsecureDevMode:
		return nil, ErrPlaintextRequiresDevMode
	}
	return &http.Client{Transport: tr}, nil
}
