package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns the process's Prometheus registry. Subsystems register their collectors
// against Registry; the /metrics endpoint is served from Handler.
type Metrics struct {
	Registry *prometheus.Registry
}

// NewMetrics builds a registry with the standard Go runtime and process collectors
// pre-registered (so GC/heap behaviour — a tracked risk on scan hot paths — is visible).
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return &Metrics{Registry: reg}
}

// Handler serves the Prometheus exposition format for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}
