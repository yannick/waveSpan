package observability

import (
	"net/http"
	"sync/atomic"
)

// Readiness is a process readiness flag. M0 marks it ready once config is valid; later
// milestones gate it on membership join (design/14, /readyz).
type Readiness struct {
	ready atomic.Bool
}

// NewReadiness returns a Readiness flag that starts not-ready.
func NewReadiness() *Readiness { return &Readiness{} }

// Set updates the readiness state.
func (r *Readiness) Set(ready bool) { r.ready.Store(ready) }

// Ready reports the current readiness state.
func (r *Readiness) Ready() bool { return r.ready.Load() }

// AdminMux builds the admin HTTP handler exposing /healthz (process up), /readyz
// (ready to serve), and /metrics. Later milestones mount the admin/observability gRPC
// services and the embedded UI on this same listener (design/26).
func AdminMux(m *Metrics, r *Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !r.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("/metrics", m.Handler())
	return mux
}
