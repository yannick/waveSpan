package checker

import (
	"fmt"

	"github.com/cwire/wavespan/tests/harness/runner"
)

// TTLBound asserts that an expired key disappears from all live replicas within
// maxExpiredVisibility = bucketSize + sweepInterval + replicationLag (design/03, design/25). It is
// NEVER asserted earlier: a read before the bound that still returns the value is fine (lazy TTL).
// A read AFTER the bound that still returns the live (non-tombstone) value is a violation.
type TTLBound struct {
	DefaultMaxVisibilityMs int64
}

// Name returns the property name.
func (TTLBound) Name() string { return "ttl-bound" }

// Check flags reads past the visibility bound that still return a live expired value.
func (t TTLBound) Check(h *runner.History) []runner.Violation {
	maxVis := t.DefaultMaxVisibilityMs
	if maxVis <= 0 {
		maxVis = 120_000
	}
	// the expiry deadline per key (from any op that carried one).
	expiry := map[string]int64{}
	for _, op := range h.Ops {
		if op.ExpiresAtMs > 0 {
			if e, ok := expiry[op.Key]; !ok || op.ExpiresAtMs < e {
				expiry[op.Key] = op.ExpiresAtMs
			}
		}
	}
	var violations []runner.Violation
	for _, op := range h.Ops {
		if op.Kind != runner.OpGet || !op.Ack {
			continue
		}
		deadline, ok := expiry[op.Key]
		if !ok {
			continue
		}
		// after the visibility bound the value must be gone (tombstone or empty).
		if op.StartMs > deadline+maxVis && !op.Tombstone && op.Value != "" {
			violations = append(violations, runner.Violation{
				Property: "ttl-bound", Key: op.Key, Window: [2]int64{op.StartMs, op.EndMs},
				Detail: fmt.Sprintf("expired key %q still live %dms past the visibility bound", op.Key, op.StartMs-deadline-maxVis),
			})
		}
	}
	return violations
}
