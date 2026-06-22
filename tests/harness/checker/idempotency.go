package checker

import (
	"fmt"

	"github.com/cwire/wavespan/internal/version"
	"github.com/cwire/wavespan/tests/harness/runner"
)

// Idempotency (doc-16 property 5) asserts that a request_id retried across partition + origin
// restart yields exactly ONE logical mutation. A correctly deduped retry collapses to the same
// writer version; two acked writes of one request_id with DISTINCT versions mean it was applied
// twice (a double counter/list entry).
type Idempotency struct{}

// Name returns the property name.
func (Idempotency) Name() string { return "idempotency" }

// Check flags request_ids that produced more than one distinct logical mutation.
func (Idempotency) Check(h *runner.History) []runner.Violation {
	byReq := map[string][]*version.Version{}
	window := map[string][2]int64{}
	for _, op := range h.Ops {
		if op.RequestID == "" || !op.Ack || op.WriterVersion == nil {
			continue
		}
		switch op.Kind {
		case runner.OpPut, runner.OpDelete, runner.OpAppend, runner.OpCAS:
			byReq[op.RequestID] = append(byReq[op.RequestID], op.WriterVersion)
			w := window[op.RequestID]
			if w[0] == 0 || op.StartMs < w[0] {
				w[0] = op.StartMs
			}
			if op.EndMs > w[1] {
				w[1] = op.EndMs
			}
			window[op.RequestID] = w
		}
	}
	var violations []runner.Violation
	for req, versions := range byReq {
		distinct := map[string]bool{}
		for _, v := range versions {
			distinct[verKey(v)] = true
		}
		if len(distinct) > 1 {
			w := window[req]
			violations = append(violations, runner.Violation{
				Property: "idempotency", Window: w,
				Detail: fmt.Sprintf("request_id %q applied %d distinct mutations (not deduped)", req, len(distinct)),
			})
		}
	}
	return violations
}

func verKey(v *version.Version) string {
	return fmt.Sprintf("%d.%d/%s/%s/%d", v.HLCPhysicalMs, v.HLCLogical, v.WriterClusterID, v.WriterMemberID, v.WriterSequence)
}
