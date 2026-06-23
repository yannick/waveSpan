//go:build harness

// Package harness is the live correctness-harness entry point (design/25, JEPSEN.md). It brings up a
// docker WaveSpan cluster (design/24), drives the Jepsen-style workloads through the public client
// while live nemeses inject faults from the host, heals, does a final post-heal read phase, and runs
// the model-aware checkers. On a violation it dumps forensics and shrinks a repro.
//
// Build-tagged `harness` so it never runs in the default suite. The pure logic (history, seed,
// checkers, shrinker, negative control, nemesis orchestration) is verified WITHOUT a cluster by the
// runner/checker/nemesis unit tests.
//
//	docker compose -f docker/docker-compose.yaml build   # once
//	go test -tags harness -run PRGate -timeout 600s ./tests/harness
package harness

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yannick/wavespan/tests/harness/checker"
	"github.com/yannick/wavespan/tests/harness/client"
	"github.com/yannick/wavespan/tests/harness/nemesis"
	"github.com/yannick/wavespan/tests/harness/runner"
	"github.com/yannick/wavespan/tests/harness/workloads"
)

func checkers() []runner.Checker {
	var cs []runner.Checker
	for _, c := range checker.All() {
		cs = append(cs, c)
	}
	return cs
}

func dataAddrs(c *runner.Cluster) map[string]string {
	m := map[string]string{}
	for _, member := range c.Members() {
		m[member] = c.DataAddr(member)
	}
	return m
}

// setUp brings the cluster up and returns it + a recording client + a fresh history.
func setUp(t *testing.T, seed int64) (*runner.Cluster, *client.Client, *runner.History) {
	t.Helper()
	c := runner.DevCluster()
	if err := c.Up(90 * time.Second); err != nil {
		t.Fatalf("cluster up: %v", err)
	}
	t.Cleanup(func() { _ = c.Down() })
	h := &runner.History{Seed: seed}
	return c, client.New(dataAddrs(c), h), h
}

// runChecks evaluates the history and fails the test with a forensic dump on any violation.
func runChecks(t *testing.T, h *runner.History) {
	t.Helper()
	res := runner.Evaluate(h, checkers(), "tests/harness/repro")
	if len(res.Violations) > 0 {
		for _, v := range res.Violations {
			t.Errorf("VIOLATION %s: %s", v.Property, v.Detail)
			t.Log(runner.Dump(h, v))
		}
		if len(res.ReproPaths) > 0 {
			t.Logf("shrunk repros: %v", res.ReproPaths)
		}
		t.FailNow()
	}
}

// TestPRGateRegisterUnderPartition: concurrent single-key writes while a node is partitioned, then
// heal and assert HLC-LWW convergence (lww-determinism + convergence) post-heal.
func TestPRGateRegisterUnderPartition(t *testing.T) {
	c, cl, h := setUp(t, 1)
	members := c.Members()
	ctx, cancel := context.WithCancel(context.Background())

	part := nemesis.DockerPartition(c)
	go func() {
		time.Sleep(time.Second)
		part.Start(h, []string{members[2]}, time.Now().UnixMilli())
		time.Sleep(2 * time.Second)
		part.Stop(h, time.Now().UnixMilli())
	}()

	workloads.Register(ctx, cl, members, 1, 40)
	cancel()
	c.WaitConverged(20 * time.Second) // cluster re-forms (all ALIVE)
	bg := context.Background()
	if pending := workloads.WaitConverged(bg, cl, members, "default", []string{"reg"}, 30*time.Second); len(pending) > 0 {
		t.Logf("keys did not converge within the window: %v (recording the divergent state for the checker)", pending)
	}
	workloads.PostHealReadAll(bg, cl, members, "default", []string{"reg"})
	runChecks(t, h)
}

// TestPRGateSetUnderKill: a grow-only set while a node is killed+restarted; every acked add must be
// present on every replica post-heal (durability + convergence).
func TestPRGateSetUnderKill(t *testing.T) {
	c, cl, h := setUp(t, 2)
	members := c.Members()
	ctx, cancel := context.WithCancel(context.Background())

	kill := nemesis.DockerKill(c)
	go func() {
		time.Sleep(time.Second)
		kill.Start(h, []string{members[1]}, time.Now().UnixMilli())
		time.Sleep(2 * time.Second)
		kill.Stop(h, time.Now().UnixMilli())
	}()

	acked := workloads.Set(ctx, cl, members, 2, 20)
	cancel()
	c.WaitConverged(25 * time.Second) // cluster re-forms after the kill+restart
	bg := context.Background()
	if pending := workloads.WaitConverged(bg, cl, members, "default", acked, 30*time.Second); len(pending) > 0 {
		t.Logf("keys did not converge within the window: %v", pending)
	}
	workloads.PostHealReadAll(bg, cl, members, "default", acked)
	runChecks(t, h)
}

// TestPRGateIdempotencyUnderPause: a request_id retried while the origin is paused must yield exactly
// one logical mutation (idempotency).
func TestPRGateIdempotencyUnderPause(t *testing.T) {
	c, cl, h := setUp(t, 3)
	members := c.Members()
	ctx, cancel := context.WithCancel(context.Background())

	pause := nemesis.DockerPause(c)
	go func() {
		time.Sleep(500 * time.Millisecond)
		pause.Start(h, []string{members[1]}, time.Now().UnixMilli())
		time.Sleep(time.Second)
		pause.Stop(h, time.Now().UnixMilli())
	}()

	workloads.Idempotency(ctx, cl, members[0], "retry-req-1", 8)
	cancel()
	c.WaitConverged(15 * time.Second)
	runChecks(t, h)
}

// TestPRGateEverywhereBackfill: writes to a replicate-everywhere namespace land on every node, and a
// wiped node that rejoins streams them back via bootstrap — verified with a local-only ScanLocal
// count (no read warms the cache), so it proves proactive backfill, not fetch-on-miss.
func TestPRGateEverywhereBackfill(t *testing.T) {
	c, cl, _ := setUp(t, 4)
	members := c.Members()
	bg := context.Background()

	const n = 40
	for i := 0; i < n; i++ {
		if !cl.Put(bg, members[0], "ref", fmt.Sprintf("ref/%d", i), fmt.Sprintf("v%d", i), "", "") {
			t.Fatalf("put %d not acked", i)
		}
	}

	// every node holds every record locally (replicate-everywhere fanout).
	waitUntil(t, 30*time.Second, func() bool {
		for _, m := range members {
			if cl.LocalCount(bg, m, "ref") != n {
				return false
			}
		}
		return true
	}, "replicate-everywhere fanout to all nodes")

	// wipe node3 (container + data volume) and bring it back empty.
	victim := members[2]
	if err := c.WipeAndRestart(victim); err != nil {
		t.Fatalf("wipe %s: %v", victim, err)
	}
	c.WaitConverged(30 * time.Second)

	// backfill: without reading any key, the rejoined node must stream all records back.
	waitUntil(t, 60*time.Second, func() bool { return cl.LocalCount(bg, victim, "ref") == n },
		"wiped node backfilled the everywhere namespace")
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timeout waiting for: %s", what)
}
