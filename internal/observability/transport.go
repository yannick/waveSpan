package observability

import (
	"net"

	"github.com/prometheus/client_golang/prometheus"
)

// NetMetrics counts wire bandwidth and accepted connections per listener (data / gossip / admin), by
// wrapping the net.Listener each server runs on. Bytes are measured at the raw connection — encrypted
// on the wire for mTLS, plaintext for h2c — i.e. true bandwidth including TLS + framing overhead.
type NetMetrics struct {
	rx       *prometheus.CounterVec
	tx       *prometheus.CounterVec
	accepted *prometheus.CounterVec
}

// NewNetMetrics registers the transport counters against the registry.
func NewNetMetrics(reg *prometheus.Registry) *NetMetrics {
	m := &NetMetrics{
		rx:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wavespan_network_received_bytes_total", Help: "Bytes read from connections, by listener."}, []string{"listener"}),
		tx:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wavespan_network_transmitted_bytes_total", Help: "Bytes written to connections, by listener."}, []string{"listener"}),
		accepted: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wavespan_connections_accepted_total", Help: "Connections accepted, by listener."}, []string{"listener"}),
	}
	reg.MustRegister(m.rx, m.tx, m.accepted)
	return m
}

// Listen opens a TCP listener on addr whose accepted connections feed the listener's byte + accept
// counters. server is the metric label ("data" / "gossip" / "admin").
func (m *NetMetrics) Listen(addr, server string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &countingListener{
		Listener: ln,
		rx:       m.rx.WithLabelValues(server),
		tx:       m.tx.WithLabelValues(server),
		accepted: m.accepted.WithLabelValues(server),
	}, nil
}

type countingListener struct {
	net.Listener
	rx, tx, accepted prometheus.Counter
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return c, err
	}
	l.accepted.Inc()
	return &countingConn{Conn: c, rx: l.rx, tx: l.tx}, nil
}

type countingConn struct {
	net.Conn
	rx, tx prometheus.Counter
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.rx.Add(float64(n))
	}
	return n, err
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.tx.Add(float64(n))
	}
	return n, err
}
