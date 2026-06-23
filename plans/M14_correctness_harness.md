# M14 — Correctness Harness Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the model-aware correctness harness specified in `25_correctness_harness.md`:
Jepsen-style workloads reimplemented in Go and CockroachDB-style nemeses, both adapted to
assert WaveSpan's **declared** eventual-consistency model (convergence, origin+1 durability,
HLC-LWW/keep-siblings, lazy TTL, optional session read-your-writes, idempotency) — never
linearizability. The harness is the canonical chaos layer that M12/TS-102 consumes.

**Architecture:** A `runner` brings up a multi-node WaveSpan cluster (Apple-container local
path for PR, docker-compose for nightly — doc 24), drives a *workload* generator through the
public client, records a unified op+fault *history*, injects *nemeses* via the doc-24 runner
fault hooks and per-module fault hooks (doc 17), then runs model-aware *checkers* over the
history. Violations trigger a forensic dump and a shrinker that emits a standalone
deterministic regression test. It reuses the proven `testing-waves` shape
(generator -> apply -> record -> check -> dump -> minimal repro) at cluster scale.

**Tech Stack:** Pure Go (no CGO, doc 17), `github.com/yannick/wavespan` public client, the
doc-24 runner + fault hooks, `os/exec`/docker for nemeses, `tests/harness/` package tree.
Seeds the work from `/Volumes/HOME/code/storage-engines/testing-waves/` (bank.go, verify.go,
repro_test.go, main.go).

**Depends on:** M03 (KV origin+1 + mutation log) — minimal value: bank/register/durability/
idempotency/session/ttl on a single cluster. Full value after **M07** (global active-active +
anti-entropy + keep-siblings) for cross-cluster convergence/sibling checkers, and **M11**
(operator/drain) for the `rolling-drain` nemesis. Doc 24 (runner + fault hooks) must exist for
the nemeses to drive faults.

---

## Context

Specified by `25_correctness_harness.md`; verifies the model from docs 00, 03, 06, 13, 22, 23
and implements the five property tests of doc 16. This milestone **builds** the harness; M12
(TS-102) **consumes** it for the nightly convergence gate — `tests/chaos/` becomes a thin
build-tagged entry point that invokes `tests/harness/`, not a separate suite.

Hard scope constraints:

- **Model-aware only.** No linearizability/serializability checker. Every convergence/no-loss
  assertion is made *post-heal with no new writes*, or is a continuous per-op property
  (durability, idempotency, session monotonicity). Stale reads and partition-time divergence
  are **not** violations (doc 00, doc 13).
- **Acked-ops only.** Convergence/no-loss apply only to ops that returned success. Un-acked
  writes (lost ACK, origin crash before reply) may or may not survive — never assert their
  presence (doc 13).
- **Policy-aware loss.** Under `hlc-last-write-wins` a concurrent overwrite may legally erase
  another write (doc 06); `no-lost-update-per-policy` must not flag that. It flags loss only
  under keep-siblings / grow-only-set where the policy promises no loss.
- **Pure Go, no CGO** (doc 17). Nemeses use doc-24 hooks + `os/exec`, not C.
- **Deterministic.** Seed + op log + nemesis schedule reproduce any run; violations shrink to
  a standalone `go test` repro (the `repro_test.go` discipline).
- **Negative control.** The harness must catch a deliberately injected bug (disable repair,
  skip dedupe, mislabel a partial scan `COMPLETE`) — proving it is not vacuously green, the
  way `testing-waves` makes RepeatableRead a negative control.

## File Structure

```
tests/harness/runner/runner.go            # orchestrate cluster + workload + nemesis, collect history
tests/harness/runner/history.go           # op/fault history types, append, serialize, forensic dump
tests/harness/runner/seed.go              # deterministic RNG + schedule derivation
tests/harness/runner/cluster.go           # cluster handle: nodes by member ID, clusters by ID, per-replica read
tests/harness/runner/shrink.go            # minimal-repro shrinking; emit standalone repro test
tests/harness/client/client.go            # thin wrapper over the WaveSpan public client
tests/harness/workloads/bank/bank.go      # conservation-under-convergence (generalized testing-waves bank)
tests/harness/workloads/register/register.go
tests/harness/workloads/set/set.go
tests/harness/workloads/listappend/listappend.go
tests/harness/workloads/idempotency/idempotency.go
tests/harness/workloads/durability/durability.go
tests/harness/workloads/session/session.go
tests/harness/workloads/ttl/ttl.go
tests/harness/nemesis/nemesis.go          # Nemesis interface (Start/Stop/schedule), composition
tests/harness/nemesis/kill.go partition.go pause.go latency.go loss.go clockskew.go disk.go drain.go
tests/harness/checker/checker.go          # Checker interface: history -> []Violation
tests/harness/checker/durability.go convergence.go lww.go completeness.go idempotency.go
tests/harness/checker/nolostupdate.go session.go ttl.go
tests/harness/harness_test.go             # //go:build harness — wires workloads x nemeses x checkers
tests/harness/repro/                       # generated standalone regression tests
```

Tests are build-tagged `//go:build harness` so they do not run in the default unit suite.

## Tasks

### Task 1: Runner — history, seed, cluster handle

**Files:**
- Create: `tests/harness/runner/history.go`, `tests/harness/runner/seed.go`, `tests/harness/runner/cluster.go`
- Test: `tests/harness/runner/history_test.go`, `tests/harness/runner/seed_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestHistoryRecordsOpsAndFaults` — append ops `{op,key,value,request_id,session,start,end,ack,served_by,observed_version}` and faults `{fault,targets,start,end}`; serialize and re-parse on one timeline.
  - `TestSeedDeterministic` — the same `int64` seed yields the same op stream and the same nemesis schedule; different seeds diverge.
  - `TestForensicDumpShape` — `Dump(history, violation)` emits seed, offending op window, per-replica disagreement, and nemesis timeline (mirror `testing-waves` `dumpFailure`).
- [ ] **Step 2:** Run, expect FAIL (package not built).
- [ ] **Step 3:** Implement history types + append/serialize/dump, a seeded `*rand.Rand` schedule deriver, and a `Cluster` handle exposing `Nodes()`, `Clusters()`, `ReadFromReplica(member, key)`, `LiveReplicas(key)`. The cluster handle wraps the doc-24 runner.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 2: Client wrapper + checker interface

**Files:**
- Create: `tests/harness/client/client.go`, `tests/harness/checker/checker.go`
- Test: `tests/harness/checker/checker_test.go`

- [ ] **Step 1:** Write failing test `TestCheckerInterface` — a trivial checker over a synthetic history returns the expected `[]Violation`; an empty/clean history returns none.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `client.Client` (Put/Get/Delete/Scan/CAS/Append/session-token plumbing over the WaveSpan public client, recording ack state into the history) and `type Checker interface { Check(*History) []Violation }` plus `Violation{Property, Detail, Window}`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: bank workload + convergence checker (generalize testing-waves)

**Files:**
- Create: `tests/harness/workloads/bank/bank.go`, `tests/harness/checker/convergence.go`
- Test: `tests/harness/workloads/bank/bank_test.go`
- Reference: `/Volumes/HOME/code/storage-engines/testing-waves/bank.go`, `verify.go`

- [ ] **Step 1:** Write failing tests:
  - `TestBankConservesPostHeal` — N accounts at 100; concurrent transfers (keep-siblings+merge mode OR CAS-retry mode); after a partition heals and writes stop, the sum read from **every live replica and every cluster** equals `N*initialBal`.
  - `TestBankNoAssertionDuringPartition` — a mid-partition snapshot summing to a non-conserved total is **not** flagged (divergence is permitted, doc 13).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Port the `testing-waves` transfer/sum logic to the cluster client; implement both transfer modes (keep-siblings merge re-adding the conserved delta; CAS-retry honoring `cas_conflict_window`). Implement `convergence` checker: group by key, assert all live replicas/clusters agree post-heal.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: register workload + lww-determinism + no-lost-update checkers

**Files:**
- Create: `tests/harness/workloads/register/register.go`, `tests/harness/checker/lww.go`, `tests/harness/checker/nolostupdate.go`
- Test: `tests/harness/checker/lww_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestLWWWinnerIsDocMaximal` — recompute the doc-22-maximal version (HLC physical, HLC logical, writer cluster, writer member, writer sequence) over the recorded acked writes; assert every replica returns it post-heal, order-independent.
  - `TestKeepSiblingsSetExact` — sibling set = all pairwise-concurrent acked writes not dominated by a later one; no missing, no spurious sibling.
  - `TestNoLostUpdatePolicyAware` — under LWW a concurrent overwrite winning is **not** a loss; under keep-siblings a dropped acked concurrent write **is** a violation.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement register generator (single-key R/W with recorded value history) and the two checkers (recompute expected winner/sibling set from the history).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: set / grow-only + listappend workloads + their checks

**Files:**
- Create: `tests/harness/workloads/set/set.go`, `tests/harness/workloads/listappend/listappend.go`
- Test: `tests/harness/workloads/set/set_test.go`, `tests/harness/workloads/listappend/listappend_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestGrowOnlySetNoLoss` — every acked added element is present on every live replica post-heal.
  - `TestListConvergedOrder` — after heal, every replica's read of a key yields the **same** list order.
  - `TestSessionCausalAppendOrder` — with a session token, a session never observes its own appends out of order; a cycle implying that **is** flagged.
  - `TestSerializabilityCycleNotFlagged` — an inter-key dependency cycle that would be a serializability anomaly is **not** flagged (out of scope, doc 00).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement set/grow-only over keep-siblings client-merge; implement list-append + Elle-style dependency edges restricted to (a) single-key converged order and (b) session causal order — never cross-key serializability cycles.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: idempotency, durability, session, ttl workloads + checkers

**Files:**
- Create: `tests/harness/workloads/{idempotency,durability,session,ttl}/*.go`, `tests/harness/checker/{idempotency,durability,session,ttl}.go`
- Test: matching `*_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestIdempotentExactlyOnce` — a `request_id` retried across partition + origin-restart yields exactly one logical mutation (no double counter/list entry).
  - `TestDurabilityOriginPlusOne` — after `kill-origin-after-ack`, the value remains readable from the nearby replica and repair restores target-N; loss permitted only if the second node also dies first (doc 13 property 1).
  - `TestSessionMonotonic` — a session read never observes a version older than one the session already observed/wrote, including across reconnect/cache-resubscribe.
  - `TestTTLBoundAndConvergence` — an expired key disappears from all live replicas within `maxExpiredVisibility = bucketSize+sweepInterval+replicationLag` (with margin), never earlier asserted; expiry preserves convergence; remote cluster honors origin `expires_at`; `hideExpiredOnRead=true` hides pre-tombstone.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the four workloads and checkers per doc 25.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: completeness-honesty checker (property 4)

**Files:**
- Create: `tests/harness/checker/completeness.go`; extend the scan path in `client/client.go`
- Test: `tests/harness/checker/completeness_test.go`

- [ ] **Step 1:** Write failing test `TestScanNeverLabelsPartialComplete` — drive scans under partition (cache coverage necessarily incomplete); assert `ScanHeader.completeness` is `PARTIAL`/`BEST_EFFORT` unless a valid, unexpired `RangeCoverageCertificate` is attached; a `COMPLETE` label with no/expired certificate is a violation (doc 03, doc 16 property 4).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the checker over recorded scan headers + certificates.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 8: Nemeses

**Files:**
- Create: `tests/harness/nemesis/nemesis.go` + `kill.go partition.go pause.go latency.go loss.go clockskew.go disk.go drain.go`
- Test: `tests/harness/nemesis/nemesis_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestNemesisStartStopHeal` — each nemesis starts a fault, records it to the history, and `Stop` fully heals it.
  - `TestKillOriginAfterAck` — the nemesis kills the origin pod within a bounded window after a write ACK (the durability hook).
  - `TestNemesisComposition` — two nemeses compose without deadlock and both appear on the timeline.
  - `TestClockSkewBeyondRejected` — skew beyond `maxClockSkewMs` triggers the skew rejection/metric (doc 22) and does not drag local clocks past the bound.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `type Nemesis interface { Start(ctx, *Cluster, schedule); Stop() }` and each nemesis from the doc-25 table over the doc-24 runner fault hooks + per-module hooks (doc 17): node-kill/restart, kill-origin-after-ack, pause/resume, empty-volume-restart, partition halves/asymmetric, latency, packet-loss, clock-skew bounded/beyond, disk-fill/stall, gateway-restart, rolling-drain (M11 operator path), cluster-partition.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 9: Shrinker + standalone repro emission

**Files:**
- Create: `tests/harness/runner/shrink.go`, `tests/harness/repro/` (output dir)
- Test: `tests/harness/runner/shrink_test.go`
- Reference: `/Volumes/HOME/code/storage-engines/testing-waves/repro_test.go`

- [ ] **Step 1:** Write failing test `TestShrinkProducesMinimalRepro` — given a history that violates a checker, the shrinker deletes ops/faults not needed to reproduce and emits a standalone `//go:build harness` `go test` in `tests/harness/repro/` using only the public client; the emitted test reproduces the violation and a no-op variant does not.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement bisection/deletion shrinking over the recorded op+fault schedule (keep failing, drop the rest), then template a standalone repro test (mirror `repro_test.go`'s two-key/one-writer minimality).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 10: Top-level runner wiring + tiers + negative control

**Files:**
- Create: `tests/harness/runner/runner.go`, `tests/harness/harness_test.go`
- Test: `tests/harness/harness_test.go` (`//go:build harness`)

- [ ] **Step 1:** Write failing tests:
  - `TestPRGateMatrix` — small cluster (Apple-container local path), short duration, `{node-kill, partition-halves, kill-origin-after-ack, clock-skew-bounded}` x every workload's quick variant; all checkers green; deterministic for a fixed seed.
  - `TestNightlySoakMatrix` (long, tagged) — large + two-cluster clusters, all nemeses composed, all workloads; green.
  - `TestNegativeControlCaught` — with an injected defect (disable repair, OR skip receiver dedupe, OR mislabel a partial scan `COMPLETE`), the corresponding checker **fails** — proving the harness is not vacuously green (the RepeatableRead-sweep discipline of `testing-waves`).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `runner.Run(cfg)` composing workload x nemesis x checkers with seed/history/dump/shrink; define the PR-gate and nightly-soak configs (doc 24 deployment paths); add the negative-control switches behind a test-only flag.
- [ ] **Step 4:** Run `go test -tags harness ./tests/harness/...`. Expect PASS (negative control fails as designed when its defect is enabled).
- [ ] **Step 5:** Commit.

## Acceptance Criteria

From `25_correctness_harness.md` + doc 16 property tests:

- **Each workload+nemesis combo runs deterministically** — a fixed seed reproduces the same op stream, nemesis schedule, and result; the PR-gate matrix is green and stable (`TestPRGateMatrix`).
- **A known-injected bug is caught** — disabling repair (or skipping dedupe, or mislabeling a partial scan `COMPLETE`) makes the relevant checker fail (`TestNegativeControlCaught`); the harness is provably not vacuously green.
- **The five doc-16 property tests are implemented as named checkers** — `durability` (P1), `convergence` (P2), `lww-determinism` (P3), `completeness-honesty` (P4), `idempotency` (P5) — plus `no-lost-update-per-policy`, `session-monotonicity`, `ttl-bound`. No linearizability checker is present.
- **Model honesty** — convergence/no-loss assertions only post-heal on acked ops; stale reads, partition-time divergence, and LWW concurrent-overwrite loss are not flagged; serializability cross-key cycles are not flagged.
- **Nightly soak green** — large + two-cluster clusters, all nemeses composed, all workloads, run clean (`TestNightlySoakMatrix`).
- **On violation** — forensic dump (seed + op window + per-replica disagreement + nemesis timeline) and a shrunk standalone repro test land in `tests/harness/repro/`.

## Verification

1. **Unit (harness internals):** `go test ./tests/harness/runner/... ./tests/harness/checker/... ./tests/harness/nemesis/...` — history/seed determinism, each checker over synthetic histories, nemesis start/stop/heal, shrinker minimality.
2. **PR gate:** `go test -tags harness -run PRGate ./tests/harness` against the Apple-container local cluster (doc 24). Confirm green and that a fixed seed reproduces byte-identical history.
3. **Negative control:** `go test -tags harness -run NegativeControl ./tests/harness` with `--inject-defect=disable-repair` (and `=skip-dedupe`, `=mislabel-complete`); confirm the matching checker fails and names the property.
4. **Nightly soak:** `docker compose -f docker/docker-compose.global.yaml up -d` then `go test -tags harness -timeout 60m -run NightlySoak ./tests/harness`; confirm all checkers green across all workload x nemesis combinations and that any violation produced a shrunk repro under `tests/harness/repro/`.
5. **Repro replay:** run a generated `tests/harness/repro/<id>_test.go` standalone with `go test -tags harness -run <id> ./tests/harness/repro` and confirm it reproduces deterministically (the `testing-waves/repro_test.go` discipline).
