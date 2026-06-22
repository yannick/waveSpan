//go:build chaos

// Package chaos is the nightly convergence release gate (TS-102). The chaos suite itself — the
// bank/register/set workloads and the {node-kill, partition-halves, cluster-partition, latency,
// kill-origin-after-ack} nemeses — is the model-aware correctness harness built in M14
// (design/25_correctness_harness.md, tests/harness/), seeded from the testing-waves bank test.
//
// This file is a THIN entry point: it invokes the harness's convergence configuration and fails
// the gate on any convergence/durability violation (asserted AFTER faults heal — eventual, not
// linearizable). It deliberately does NOT reimplement bank/nemeses.
//
// Run (nightly, requires the multi-node + two-cluster compose clusters):
//
//	docker compose -f docker/docker-compose.yaml up -d
//	docker compose -f docker/docker-compose.global.yaml up -d
//	go test -tags chaos -timeout 30m ./tests/chaos -run Convergence
package chaos

import "testing"

// TestConvergenceGate runs the M14 correctness harness with the nightly convergence config and
// gates on its convergence + durability checkers. Until M14 lands (tests/harness/runner), this
// skips with a clear pointer rather than silently passing.
func TestConvergenceGate(t *testing.T) {
	runner := locateHarness()
	if runner == "" {
		t.Skip("M14 correctness harness (tests/harness/runner) not present; convergence gate is wired but the harness is built in M14 (design/25)")
	}
	t.Fatalf("harness present at %s but the gate wiring expects M14's runner.Run API; implement per design/25 once M14 lands", runner)
}

// locateHarness returns the path to the M14 harness runner if it exists, else "".
func locateHarness() string {
	// M14 builds tests/harness/runner; until then there is nothing to invoke.
	return ""
}
