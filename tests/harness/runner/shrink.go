package runner

import (
	"fmt"
	"os"
	"path/filepath"
)

// Shrink minimizes a violating history by delete-based bisection: it repeatedly drops ops and
// faults that are not needed to reproduce the violation, keeping the smallest history for which
// fails() still returns true (design/25 "shrinker"; the testing-waves repro_test.go discipline).
func Shrink(h *History, fails func(*History) bool) *History {
	cur := h
	changed := true
	for changed {
		changed = false
		for i := range cur.Ops {
			cand := withoutOp(cur, i)
			if fails(cand) {
				cur = cand
				changed = true
				break
			}
		}
		if changed {
			continue
		}
		for i := range cur.Faults {
			cand := withoutFault(cur, i)
			if fails(cand) {
				cur = cand
				changed = true
				break
			}
		}
	}
	return cur
}

func withoutOp(h *History, i int) *History {
	ops := make([]Op, 0, len(h.Ops)-1)
	ops = append(ops, h.Ops[:i]...)
	ops = append(ops, h.Ops[i+1:]...)
	return &History{Seed: h.Seed, Ops: ops, Faults: h.Faults}
}

func withoutFault(h *History, i int) *History {
	faults := make([]Fault, 0, len(h.Faults)-1)
	faults = append(faults, h.Faults[:i]...)
	faults = append(faults, h.Faults[i+1:]...)
	return &History{Seed: h.Seed, Ops: h.Ops, Faults: faults}
}

// EmitRepro writes the (minimized) history as a standalone JSON regression case under dir. The
// build-tagged repro test (tests/harness/repro) replays every emitted case and asserts it still
// violates — a deterministic, public-client-free reproduction.
func EmitRepro(h *History, id, dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	data, err := h.Serialize()
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("repro_%s.json", id))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	return path, nil
}
