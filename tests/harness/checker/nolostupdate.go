package checker

import (
	"fmt"

	"github.com/yannick/wavespan/internal/version"
	"github.com/yannick/wavespan/tests/harness/runner"
)

// NoLostUpdatePerPolicy is policy-aware: under hlc-last-write-wins a concurrent overwrite legally
// erasing another write is NOT a loss (design/06) and is never flagged. It flags loss only for
// keep-siblings keys, where the policy promises every concurrent acked write survives as a sibling.
type NoLostUpdatePerPolicy struct{}

// Name returns the property name.
func (NoLostUpdatePerPolicy) Name() string { return "no-lost-update-per-policy" }

// Check flags acked concurrent writes dropped under a keep-siblings policy.
func (NoLostUpdatePerPolicy) Check(h *runner.History) []runner.Violation {
	var violations []runner.Violation
	for _, key := range allKeys(h) {
		if keyPolicy(h, key) != "keep-siblings" {
			continue // LWW loss is policy-legal
		}
		reads := h.PostHealReads(key)
		if len(reads) == 0 {
			continue
		}
		present := presentVersions(reads)
		writes := h.AckedWrites(key)
		for _, w := range writes {
			if w.WriterVersion == nil {
				continue
			}
			if !present[verKey(w.WriterVersion)] && !supersededBySameWriter(writes, w.WriterVersion) {
				violations = append(violations, runner.Violation{
					Property: "no-lost-update-per-policy", Key: key, Window: [2]int64{w.StartMs, w.EndMs},
					Detail: fmt.Sprintf("keep-siblings key %q dropped an acked concurrent write (version %s)", key, verKey(w.WriterVersion)),
				})
			}
		}
	}
	return violations
}

// presentVersions is the union of every version visible post-heal (winner + siblings).
func presentVersions(reads []runner.Op) map[string]bool {
	out := map[string]bool{}
	for _, r := range reads {
		if r.ObservedVersion != nil {
			out[verKey(r.ObservedVersion)] = true
		}
		for _, s := range r.Siblings {
			out[verKey(s)] = true
		}
	}
	return out
}

// supersededBySameWriter reports whether a later write from the same writer (higher sequence) made v
// causally obsolete (a sequential overwrite, not a concurrent loss).
func supersededBySameWriter(writes []runner.Op, v *version.Version) bool {
	for _, w := range writes {
		o := w.WriterVersion
		if o == nil || o == v {
			continue
		}
		if o.WriterClusterID == v.WriterClusterID && o.WriterMemberID == v.WriterMemberID && o.WriterSequence > v.WriterSequence {
			return true
		}
	}
	return false
}
