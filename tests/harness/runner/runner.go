package runner

// Config selects a workload x nemesis x checker matrix and the cluster tier (design/24/25). The
// PR-gate runs a small cluster + short workload + a few nemeses; the nightly soak runs large +
// two-cluster + all nemeses composed.
type Config struct {
	Tier         string // "pr-gate" | "nightly-soak"
	Seed         int64
	DurationMs   int64
	Workloads    []string
	NemesisKinds []string
	Members      []string
	ReproDir     string
}

// PRGateConfig is the fast, deterministic PR-gate matrix (Apple-container local path, design/24).
func PRGateConfig(seed int64) Config {
	return Config{
		Tier: "pr-gate", Seed: seed, DurationMs: 30_000,
		Workloads:    []string{"bank", "register", "idempotency", "durability", "completeness", "session", "ttl"},
		NemesisKinds: []string{"node-kill", "partition-halves", "kill-origin-after-ack", "clock-skew-bounded"},
		Members:      []string{"n1", "n2", "n3"},
		ReproDir:     "tests/harness/repro",
	}
}

// NightlySoakConfig is the long, exhaustive matrix (docker two-cluster path, design/24).
func NightlySoakConfig(seed int64) Config {
	c := PRGateConfig(seed)
	c.Tier = "nightly-soak"
	c.DurationMs = 60 * 60 * 1000
	c.Workloads = append(c.Workloads, "set", "listappend")
	c.NemesisKinds = append(c.NemesisKinds,
		"pause", "latency", "packet-loss", "clock-skew-beyond", "disk-fill", "cluster-partition", "rolling-drain")
	c.Members = []string{"a1", "a2", "a3", "b1", "b2", "b3"}
	return c
}

// Result is the outcome of a harness run.
type Result struct {
	History    *History
	Violations []Violation
	ReproPaths []string
}

// Checker is the subset of checker.Checker the runner needs (avoids an import cycle).
type Checker interface {
	Name() string
	Check(*History) []Violation
}

// Evaluate runs the checkers over a recorded history, and on any violation writes a forensic dump
// and a shrunk repro per offending property. (Live cluster bring-up + workload drive lives in the
// build-tagged harness entry point; Evaluate is the deterministic, cluster-free core.)
func Evaluate(h *History, checkers []Checker, reproDir string) Result {
	res := Result{History: h}
	for _, c := range checkers {
		vs := c.Check(h)
		if len(vs) == 0 {
			continue
		}
		res.Violations = append(res.Violations, vs...)
		// shrink to the minimal history that still trips THIS checker, then emit a repro.
		fails := func(hh *History) bool { return len(c.Check(hh)) > 0 }
		minH := Shrink(h, fails)
		if path, err := EmitRepro(minH, c.Name(), reproDir); err == nil {
			res.ReproPaths = append(res.ReproPaths, path)
		}
	}
	return res
}
