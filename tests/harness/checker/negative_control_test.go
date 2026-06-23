package checker

import (
	"testing"

	"github.com/yannick/wavespan/tests/harness/runner"
)

// TestNegativeControlCaught proves the harness is NOT vacuously green: for each injected defect, the
// matching checker in the full suite fires (design/25 "Negative control", the testing-waves
// RepeatableRead-sweep discipline).
func TestNegativeControlCaught(t *testing.T) {
	cases := []struct {
		defect   string
		property string
		build    func() *runner.History
	}{
		{
			// disable-repair: an acked winner is lost (no holder restored it) and no double-holder-loss.
			defect: "disable-repair", property: "durability",
			build: func() *runner.History {
				h := &runner.History{}
				healed(h)
				h.Append(runner.Op{Kind: runner.OpPut, Key: "a", Ack: true, WriterVersion: v(300, 0, "n2", 1)})
				h.Append(runner.Op{Kind: runner.OpGet, Key: "a", Ack: true, ServedBy: "n1", StartMs: 100, ObservedVersion: v(100, 0, "n1", 1)})
				return h
			},
		},
		{
			// skip-dedupe: a retried request_id applied as two distinct mutations.
			defect: "skip-dedupe", property: "idempotency",
			build: func() *runner.History {
				h := &runner.History{}
				h.Append(runner.Op{Kind: runner.OpAppend, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(100, 0, "n1", 1)})
				h.Append(runner.Op{Kind: runner.OpAppend, Key: "c", RequestID: "r1", Ack: true, WriterVersion: v(200, 0, "n1", 2)})
				return h
			},
		},
		{
			// mislabel-complete: a partial scan labeled COMPLETE with no certificate.
			defect: "mislabel-complete", property: "completeness-honesty",
			build: func() *runner.History {
				h := &runner.History{}
				h.Append(runner.Op{Kind: runner.OpScan, Key: "ns", Completeness: "COMPLETE", HasCertificate: false, StartMs: 100, EndMs: 101})
				return h
			},
		},
	}
	for _, tc := range cases {
		vs := Run(tc.build(), All())
		if !hasViolation(vs, tc.property) {
			t.Fatalf("injected defect %q must be caught by the %q checker; suite returned %d violations", tc.defect, tc.property, len(vs))
		}
	}
}
