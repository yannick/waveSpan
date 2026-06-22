package security

import (
	"net"
	"net/http"
	"time"
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
	}
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
	case !c.InsecureDevMode:
		return nil, ErrPlaintextRequiresDevMode
	}
	return &http.Client{Transport: tr}, nil
}
