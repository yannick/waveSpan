package alerts

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// knownMetrics are the metric names exported by the data node (internal/*/metrics, cmd/wavespan-node).
var knownMetrics = []string{
	"kv_under_replicated_keys_estimate",
	"kv_repair_queue_depth",
	"kv_ttl_tombstones_written_total",
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

func referencesKnownMetric(expr string) bool {
	for _, m := range knownMetrics {
		if strings.Contains(expr, m) {
			return true
		}
	}
	return false
}

type ruleGroups struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Alert string `yaml:"alert"`
			Expr  string `yaml:"expr"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func TestAlertRulesValidAndReferenceRealMetrics(t *testing.T) {
	data, err := os.ReadFile("wavespan_alerts.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var rg ruleGroups
	if err := yaml.Unmarshal(data, &rg); err != nil {
		t.Fatalf("alert rules do not parse: %v", err)
	}
	count := 0
	for _, g := range rg.Groups {
		for _, r := range g.Rules {
			if r.Alert == "" {
				continue
			}
			count++
			if r.Expr == "" {
				t.Fatalf("alert %q has no expr", r.Alert)
			}
			if !referencesKnownMetric(r.Expr) {
				t.Fatalf("alert %q expr %q references no known WaveSpan metric", r.Alert, r.Expr)
			}
		}
	}
	if count < 4 {
		t.Fatalf("expected several alerts, got %d", count)
	}
}

func TestDashboardParsesAndReferencesMetrics(t *testing.T) {
	data, err := os.ReadFile("../dashboards/wavespan_overview.json")
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Panels []struct {
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dash); err != nil {
		t.Fatalf("dashboard JSON invalid: %v", err)
	}
	if len(dash.Panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			if !referencesKnownMetric(tg.Expr) {
				t.Fatalf("dashboard panel target %q references no known metric", tg.Expr)
			}
		}
	}
}
