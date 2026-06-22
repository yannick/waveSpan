package runner

import (
	"os"
	"testing"
)

// divergeChecker flags any key whose post-heal reads disagree (a stand-in for the convergence
// checker, kept local to avoid the checker->runner import cycle in this package's tests).
type divergeChecker struct{}

func (divergeChecker) Name() string { return "diverge" }
func (divergeChecker) Check(h *History) []Violation {
	var vs []Violation
	for _, key := range distinctKeys(h) {
		vals := map[string]bool{}
		for _, r := range h.PostHealReads(key) {
			vals[r.Value] = true
		}
		if len(vals) > 1 {
			vs = append(vs, Violation{Property: "diverge", Key: key})
		}
	}
	return vs
}

func distinctKeys(h *History) []string {
	seen := map[string]bool{}
	var keys []string
	for _, op := range h.Ops {
		if op.Key != "" && !seen[op.Key] {
			seen[op.Key] = true
			keys = append(keys, op.Key)
		}
	}
	return keys
}

func TestEvaluateShrinksAndEmitsRepro(t *testing.T) {
	h := &History{Seed: 5}
	for i := 0; i < 8; i++ {
		h.Append(Op{Kind: OpPut, Key: "noise", Ack: true})
	}
	h.Append(Op{Kind: OpGet, Key: "k", Value: "1", Ack: true, ServedBy: "n1", StartMs: 100})
	h.Append(Op{Kind: OpGet, Key: "k", Value: "2", Ack: true, ServedBy: "n2", StartMs: 100})

	dir := t.TempDir()
	res := Evaluate(h, []Checker{divergeChecker{}}, dir)
	if len(res.Violations) == 0 {
		t.Fatal("Evaluate should surface the divergence violation")
	}
	if len(res.ReproPaths) != 1 {
		t.Fatalf("Evaluate should emit one repro, got %d", len(res.ReproPaths))
	}
	// the emitted repro is a minimized, replayable history
	data, err := os.ReadFile(res.ReproPaths[0])
	if err != nil {
		t.Fatal(err)
	}
	minH, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(minH.Ops) != 2 {
		t.Fatalf("emitted repro should be minimized to the 2 offending reads, got %d ops", len(minH.Ops))
	}
	if len(divergeChecker{}.Check(minH)) == 0 {
		t.Fatal("emitted repro must still reproduce the violation")
	}
}
