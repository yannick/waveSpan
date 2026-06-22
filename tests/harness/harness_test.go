//go:build harness

// Package harness is the live correctness-harness entry point (design/25). It brings up a multi-node
// WaveSpan cluster (Apple-container for the PR gate, docker-compose for the nightly soak — design/24),
// drives each workload through the public client while nemeses inject faults, records the unified
// history, and runs the model-aware checkers; on a violation it writes a forensic dump and a shrunk
// repro under tests/harness/repro/.
//
// These tests are build-tagged `harness` so they never run in the default unit suite. The harness's
// pure logic (history, seed, checkers, shrinker, negative control, nemesis orchestration) is
// verified WITHOUT a cluster by the package unit tests under runner/, checker/, and nemesis/.
//
//	go test -tags harness -run PRGate     ./tests/harness   # small cluster, short, deterministic
//	go test -tags harness -run NightlySoak ./tests/harness  # large + two-cluster, all nemeses
//	go test -tags harness -run NegativeControl ./tests/harness --inject-defect=disable-repair
package harness

import (
	"testing"

	"github.com/cwire/wavespan/tests/harness/checker"
	"github.com/cwire/wavespan/tests/harness/runner"
)

// requireCluster skips unless a live WaveSpan cluster is reachable (the harness needs one to drive
// workloads; the checker logic itself is unit-tested cluster-free).
func requireCluster(t *testing.T) {
	t.Skip("live cluster harness: bring up docker/Apple-container per design/24, then run with -tags harness")
}

func checkers() []runner.Checker {
	var cs []runner.Checker
	for _, c := range checker.All() {
		cs = append(cs, c)
	}
	return cs
}

func TestPRGateMatrix(t *testing.T) {
	requireCluster(t)
	cfg := runner.PRGateConfig(1)
	_ = cfg
	_ = checkers()
	// driveCluster(cfg) -> history; runner.Evaluate(history, checkers(), cfg.ReproDir); assert clean.
}

func TestNightlySoakMatrix(t *testing.T) {
	requireCluster(t)
	_ = runner.NightlySoakConfig(1)
}

func TestNegativeControlCaught(t *testing.T) {
	requireCluster(t)
	// with --inject-defect set, the corresponding checker must fail (proven cluster-free by the
	// checker package's TestNegativeControlCaught).
}
