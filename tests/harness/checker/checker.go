// Package checker holds the model-aware correctness checkers (design/25, design/16). Each Checker
// verifies WaveSpan's DECLARED eventual-consistency model over a recorded history — never
// linearizability. Convergence/no-loss assertions are made only post-heal on acked ops; stale
// reads, partition-time divergence, and policy-legal LWW overwrites are NOT violations.
package checker

import "github.com/cwire/wavespan/tests/harness/runner"

// Checker maps a history to the violations it found (empty when clean).
type Checker interface {
	// Name is the property name (e.g. "convergence", "durability").
	Name() string
	// Check returns all violations of this checker's property in the history.
	Check(h *runner.History) []runner.Violation
}

// All returns the full checker suite (the five doc-16 properties plus the policy-aware extensions).
func All() []Checker {
	return []Checker{
		Durability{},
		Convergence{},
		LWWDeterminism{},
		CompletenessHonesty{},
		Idempotency{},
		NoLostUpdatePerPolicy{},
		SessionMonotonicity{},
		TTLBound{DefaultMaxVisibilityMs: 120_000},
	}
}

// Run executes every checker and returns all violations.
func Run(h *runner.History, checkers []Checker) []runner.Violation {
	var all []runner.Violation
	for _, c := range checkers {
		all = append(all, c.Check(h)...)
	}
	return all
}
