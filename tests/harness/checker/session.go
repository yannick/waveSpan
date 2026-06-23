package checker

import (
	"fmt"
	"sort"

	"github.com/yannick/wavespan/internal/version"
	"github.com/yannick/wavespan/tests/harness/runner"
)

// SessionMonotonicity asserts read-your-writes / monotonic-reads WITHIN a session (design/00,
// design/25): a session never observes a version older than one it already observed or wrote,
// including across reconnect / cache-resubscribe. (Cross-session staleness is allowed.)
type SessionMonotonicity struct{}

// Name returns the property name.
func (SessionMonotonicity) Name() string { return "session-monotonicity" }

// Check flags a session read that regresses below a version the session already saw, per key.
func (SessionMonotonicity) Check(h *runner.History) []runner.Violation {
	// per (session, key): ordered ops carrying a version
	type sk struct{ session, key string }
	seq := map[sk][]runner.Op{}
	for _, op := range h.Ops {
		if op.Session == "" || !op.Ack {
			continue
		}
		v := op.ObservedVersion
		if op.WriterVersion != nil {
			v = op.WriterVersion
		}
		if v == nil {
			continue
		}
		key := sk{op.Session, op.Key}
		seq[key] = append(seq[key], op)
	}
	var violations []runner.Violation
	for k, ops := range seq {
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].StartMs < ops[j].StartMs })
		var high *version.Version
		for _, op := range ops {
			v := op.ObservedVersion
			isRead := op.Kind == runner.OpGet
			if op.WriterVersion != nil {
				v = op.WriterVersion
			}
			if v == nil {
				continue
			}
			if isRead && high != nil && v.Compare(*high) < 0 {
				violations = append(violations, runner.Violation{
					Property: "session-monotonicity", Key: k.key, Window: [2]int64{op.StartMs, op.EndMs},
					Detail: fmt.Sprintf("session %q read a version older than one it already observed for %q", k.session, k.key),
				})
			}
			if high == nil || v.Compare(*high) > 0 {
				high = v
			}
		}
	}
	return violations
}
