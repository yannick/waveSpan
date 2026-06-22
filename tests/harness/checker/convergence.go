package checker

import (
	"fmt"

	"github.com/cwire/wavespan/tests/harness/runner"
)

// Convergence (doc-16 property 2) asserts that AFTER all faults heal and writes stop, every live
// replica and every cluster agrees on each key's value. Divergence DURING a partition is permitted
// (design/13) and is never inspected — only PostHealReads are compared.
type Convergence struct{}

// Name returns the property name.
func (Convergence) Name() string { return "convergence" }

// Check flags keys whose post-heal reads disagree.
func (Convergence) Check(h *runner.History) []runner.Violation {
	var violations []runner.Violation
	for _, key := range keysWithPostHealReads(h) {
		reads := h.PostHealReads(key)
		values := map[string]bool{}
		for _, r := range reads {
			values[r.Value] = true
		}
		if len(values) > 1 {
			window := [2]int64{reads[0].StartMs, reads[len(reads)-1].StartMs}
			violations = append(violations, runner.Violation{
				Property: "convergence", Key: key, Window: window,
				Detail: fmt.Sprintf("post-heal replicas disagree on %q: %d distinct values", key, len(values)),
			})
		}
	}
	return violations
}

func keysWithPostHealReads(h *runner.History) []string {
	seen := map[string]bool{}
	var keys []string
	for _, op := range h.Ops {
		if op.Kind == runner.OpGet && op.Ack && !seen[op.Key] {
			if len(h.PostHealReads(op.Key)) > 1 {
				seen[op.Key] = true
				keys = append(keys, op.Key)
			}
		}
	}
	return keys
}

func allKeys(h *runner.History) []string {
	seen := map[string]bool{}
	var keys []string
	for _, op := range h.Ops {
		if !seen[op.Key] && op.Key != "" {
			seen[op.Key] = true
			keys = append(keys, op.Key)
		}
	}
	return keys
}
