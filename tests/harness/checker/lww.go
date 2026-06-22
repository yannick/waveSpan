package checker

import (
	"fmt"

	"github.com/cwire/wavespan/internal/version"
	"github.com/cwire/wavespan/tests/harness/runner"
)

// LWWDeterminism (doc-16 property 3) recomputes the doc-22-maximal winner over the acked writes of
// an hlc-last-write-wins key and asserts every post-heal read returns exactly that version,
// order-independent. Keep-siblings keys are checked by NoLostUpdatePerPolicy instead.
type LWWDeterminism struct{}

// Name returns the property name.
func (LWWDeterminism) Name() string { return "lww-determinism" }

// Check flags any post-heal read that disagrees with the recomputed maximal winner.
func (LWWDeterminism) Check(h *runner.History) []runner.Violation {
	var violations []runner.Violation
	for _, key := range allKeys(h) {
		if keyPolicy(h, key) == "keep-siblings" {
			continue
		}
		winner := maximalWinner(h.AckedWrites(key))
		if winner == nil {
			continue
		}
		for _, r := range h.PostHealReads(key) {
			if r.ObservedVersion == nil {
				continue
			}
			if r.ObservedVersion.Compare(*winner) != 0 {
				violations = append(violations, runner.Violation{
					Property: "lww-determinism", Key: key, Window: [2]int64{r.StartMs, r.EndMs},
					Detail: fmt.Sprintf("replica %s returned a non-maximal version for %q", r.ServedBy, key),
				})
			}
		}
	}
	return violations
}

// maximalWinner returns the doc-22-maximal version among acked writes (HLC physical, HLC logical,
// writer cluster, writer member, writer sequence). Tombstones with the winning version win.
func maximalWinner(writes []runner.Op) *version.Version {
	var winner *version.Version
	for i := range writes {
		v := writes[i].WriterVersion
		if v == nil {
			continue
		}
		if winner == nil || v.Compare(*winner) > 0 {
			winner = v
		}
	}
	return winner
}

func keyPolicy(h *runner.History, key string) string {
	for _, op := range h.Ops {
		if op.Key == key && op.Policy != "" {
			return op.Policy
		}
	}
	return "hlc-last-write-wins"
}
