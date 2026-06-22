package global

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the global-replication collectors (design/06 "Metrics"). Out/in lag are gauges fed by
// the sender and applier; the rest are counters.
type Metrics struct {
	OutLagSeconds              prometheus.Gauge
	InLagSeconds               prometheus.Gauge
	BytesSent                  prometheus.Counter
	BytesReceived              prometheus.Counter
	Conflicts                  prometheus.Counter
	ConflictsByPolicy          *prometheus.CounterVec
	AntiEntropyRuns            prometheus.Counter
	AntiEntropyDivergentRanges prometheus.Counter
	ApplyErrors                prometheus.Counter
}

// NewMetrics builds and registers the global-replication collectors.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		OutLagSeconds:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "global_repl_out_lag_seconds", Help: "age of the oldest un-shipped out-log entry"}),
		InLagSeconds:               prometheus.NewGauge(prometheus.GaugeOpts{Name: "global_repl_in_lag_seconds", Help: "age of the newest applied inbound mutation vs origin time"}),
		BytesSent:                  prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_bytes_sent_total", Help: "bytes shipped to peers"}),
		BytesReceived:              prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_bytes_received_total", Help: "bytes received from peers"}),
		Conflicts:                  prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_conflicts_total", Help: "conflicts resolved on apply"}),
		ConflictsByPolicy:          prometheus.NewCounterVec(prometheus.CounterOpts{Name: "global_repl_conflicts_by_policy_total", Help: "conflicts resolved, by policy"}, []string{"policy"}),
		AntiEntropyRuns:            prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_anti_entropy_runs_total", Help: "anti-entropy rounds run"}),
		AntiEntropyDivergentRanges: prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_anti_entropy_divergent_ranges_total", Help: "divergent ranges found by anti-entropy"}),
		ApplyErrors:                prometheus.NewCounter(prometheus.CounterOpts{Name: "global_repl_apply_errors_total", Help: "errors applying inbound mutations"}),
	}
	if reg != nil {
		reg.MustRegister(m.OutLagSeconds, m.InLagSeconds, m.BytesSent, m.BytesReceived,
			m.Conflicts, m.ConflictsByPolicy, m.AntiEntropyRuns, m.AntiEntropyDivergentRanges, m.ApplyErrors)
	}
	return m
}
