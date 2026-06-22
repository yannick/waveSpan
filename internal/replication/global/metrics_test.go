package global

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsExposesAllCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.ConflictsByPolicy.WithLabelValues("hlc-last-write-wins").Add(0) // a CounterVec is gathered only once labeled
	want := []string{
		"global_repl_out_lag_seconds",
		"global_repl_in_lag_seconds",
		"global_repl_bytes_sent_total",
		"global_repl_bytes_received_total",
		"global_repl_conflicts_total",
		"global_repl_conflicts_by_policy_total",
		"global_repl_anti_entropy_runs_total",
		"global_repl_anti_entropy_divergent_ranges_total",
		"global_repl_apply_errors_total",
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, mf := range mfs {
		have[mf.GetName()] = true
	}
	for _, w := range want {
		if !have[w] {
			t.Errorf("missing metric %s", w)
		}
	}
}
