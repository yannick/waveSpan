package checker

import (
	"fmt"

	"github.com/yannick/wavespan/tests/harness/runner"
)

// Durability (doc-16 property 1) asserts that an acked origin+1 write is not silently lost: the
// last-acked winner for a key must be observed by some post-heal read. Loss is permitted only when
// both holders died (a kill-origin-after-ack fault overlapping a kill of the nearby replica) — that
// case is recorded as a fault window and excluded. Un-acked writes are never asserted (design/13).
type Durability struct{}

// Name returns the property name.
func (Durability) Name() string { return "durability" }

// Check flags keys whose last acked winner vanished post-heal with no double-holder-loss excuse.
func (Durability) Check(h *runner.History) []runner.Violation {
	var violations []runner.Violation
	for _, key := range allKeys(h) {
		winner := maximalWinner(h.AckedWrites(key))
		if winner == nil {
			continue
		}
		reads := h.PostHealReads(key)
		if len(reads) == 0 {
			continue // not observed post-heal; cannot assert presence without a read
		}
		observed := false
		for _, r := range reads {
			if r.ObservedVersion != nil && r.ObservedVersion.Compare(*winner) >= 0 {
				observed = true
				break
			}
		}
		if !observed && !doubleHolderLoss(h, key) {
			violations = append(violations, runner.Violation{
				Property: "durability", Key: key, Window: [2]int64{reads[0].StartMs, reads[len(reads)-1].StartMs},
				Detail: fmt.Sprintf("acked winner for %q is absent from all post-heal reads (no double-holder-loss)", key),
			})
		}
	}
	return violations
}

// doubleHolderLoss reports whether a fault window indicates both durable holders of the key were
// lost (the only legitimate origin+1 loss, design/13 property 1). Modeled as a recorded
// "double-holder-loss" fault.
func doubleHolderLoss(h *runner.History, key string) bool {
	for _, f := range h.Faults {
		if f.Kind == "double-holder-loss" {
			for _, t := range f.Targets {
				if t == key {
					return true
				}
			}
		}
	}
	return false
}
