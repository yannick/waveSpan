package checker

import (
	"fmt"

	"github.com/yannick/wavespan/tests/harness/runner"
)

// CompletenessHonesty (doc-16 property 4) asserts a scan is NEVER labeled COMPLETE without a valid,
// unexpired RangeCoverageCertificate. PARTIAL/BEST_EFFORT scans are always honest. A COMPLETE label
// with no/expired certificate is a violation (design/03, design/16).
type CompletenessHonesty struct{}

// Name returns the property name.
func (CompletenessHonesty) Name() string { return "completeness-honesty" }

// Check flags scans labeled COMPLETE without a valid certificate.
func (CompletenessHonesty) Check(h *runner.History) []runner.Violation {
	var violations []runner.Violation
	for _, op := range h.Ops {
		if op.Kind != runner.OpScan || op.Completeness != "COMPLETE" {
			continue
		}
		valid := op.HasCertificate && op.CertValidUntilMs > op.StartMs
		if !valid {
			violations = append(violations, runner.Violation{
				Property: "completeness-honesty", Key: op.Key, Window: [2]int64{op.StartMs, op.EndMs},
				Detail: fmt.Sprintf("scan labeled COMPLETE with %s certificate", certState(op)),
			})
		}
	}
	return violations
}

func certState(op runner.Op) string {
	switch {
	case !op.HasCertificate:
		return "no"
	case op.CertValidUntilMs <= op.StartMs:
		return "an expired"
	default:
		return "a valid"
	}
}
