# 25. Correctness harness

## Goal

Verify WaveSpan's **declared** consistency model under faults — not a stronger one. WaveSpan
is eventually consistent (doc 00, doc 13). It promises origin+1 durability, HLC-LWW or
keep-siblings conflict resolution, lazy TTL with a bounded staleness window (doc 03),
optional session read-your-writes, and idempotent replication by mutation ID. It explicitly
does **not** promise linearizable reads/writes, serializable transactions, or globally
consistent range scans (doc 00 "does not promise by default", doc 20). The harness must
assert the right invariants for that model and treat the non-goals as non-goals.

This document is the **canonical specification of the correctness harness**. Doc 16 (testing
strategy) references it for Layer-4 chaos and the property tests; this document owns the
detail. The harness is built by milestone **M14** and consumed by **M12** (TS-102).

## Philosophy

The harness reuses the structure proven by the existing `testing-waves` bank harness
(`/Volumes/HOME/code/storage-engines/testing-waves/`), which already found a real wavesdb
skiplist MVCC bug. That structure is:

```text
generator -> apply ops against the cluster -> record a history
          -> (inject/heal faults) -> check model-aware invariants
          -> forensic dump on violation -> deterministic minimal repro
```

Principles:

1. **Model-aware, not linearizability-aware.** A naive Knossos/Elle-serializability checker
   would report *expected* violations on an eventually consistent store, because WaveSpan
   permits stale reads, concurrent versions, and partition-time divergence. We do **not** run
   a linearizability checker. We check the invariants WaveSpan actually claims. Every checker
   makes its assertion *after convergence* (post-heal, no new writes) where the model
   guarantees agreement, or asserts a per-op property (durability, idempotency, session
   monotonicity) that holds continuously.
2. **Pure Go, no CGO.** Same constraint as the rest of WaveSpan (doc 17: `wavesdb` is an
   in-process Go library, no FFI, no C build). The harness imports the WaveSpan client and
   drives a cluster; nemeses use the runner fault hooks (doc 24), `os/exec`, and the
   per-module fault-injection hooks every package exposes (doc 17 "Implementation rule").
3. **Acked-ops only.** The convergence and no-loss invariants apply only to operations that
   returned success. A write whose ACK was lost (origin crashed before reply) may or may not
   survive — that is permitted (doc 13 "Pod crash before nearby ACK"). The harness tracks ack
   state per op and never asserts presence of an un-acked write.
4. **Deterministic and reproducible.** Every run is seeded; the seed plus the op log plus the
   nemesis schedule fully reproduce a failure. On violation the harness emits a minimal repro
   (shrunk op/fault schedule) exactly as `testing-waves/repro_test.go` reduced the skiplist
   bug to two keys and one writer.

## What "the cluster" means

Unlike `testing-waves` (which drives a single in-process `wavesdb`), this harness drives a
**multi-node WaveSpan cluster** through the public client, so it exercises origin+1 fanout,
the latency graph, repair, dynamic cache, and global active-active. Two deployment targets
(doc 24):

- **Apple-container local path** — fast multi-node cluster on macOS for PR-gated subsets.
- **Docker/Linux CI path** — `docker/docker-compose.yaml` (single cluster) and
  `docker/docker-compose.global.yaml` (two clusters A/B) for the full nightly soak.

The harness talks to nodes by member ID and to clusters by cluster ID, so a checker can read
"the same key from every live replica" and "from both clusters after heal".

## Workloads

Each workload is a Go package under `tests/harness/workloads/`. A workload produces a
*generator* (a deterministic stream of ops given a seed) and a *model checker* that consumes
the recorded history. Workloads are reimplementations of the Jepsen catalogue **adapted to
eventual consistency** — the assertion is convergence/causal/durability, never
linearizability.

### bank (conservation under convergence)

Generalize the existing `testing-waves` bank to the distributed cluster.

- N accounts at balance 100; concurrent transfers move a fixed amount between accounts via
  the WaveSpan client (CAS or read-modify-write transfer, see note below).
- **Invariant:** after faults heal and writes stop, the sum of all balances read **from every
  live replica and from every cluster** equals `N * initialBal`. This is convergence of a
  conserved quantity, asserted *post-heal*, not a continuous linearizable total.
- **Not asserted:** that a mid-partition snapshot sums to `N*initialBal`. During a partition
  both sides may diverge; that is expected (doc 13 "Intra-cluster network partition"). The
  bank conservation oracle is the convergence oracle M12/TS-102 already names.
- **Conflict-policy caveat:** under `hlc-last-write-wins`, two concurrent transfers touching
  the same account can lose an update — LWW is "not semantically safe for counters" (doc 06).
  So the bank runs in one of two configured modes: (a) **keep-siblings + a deterministic
  application merge** that re-adds the conserved delta on read, asserting no acked transfer's
  delta is dropped; or (b) **CAS-guarded transfers** that re-read and retry on
  `cas_conflict_window`, accepting that CAS is best-effort (doc 03) and only asserting
  conservation post-heal. The harness records which mode and asserts accordingly. This makes
  the bank an honest eventual-consistency oracle rather than a smuggled linearizability test.

### register (per-key value history)

Single-key reads and writes, per-key value history recorded.

- **LWW determinism:** for a key under `hlc-last-write-wins`, after convergence every live
  replica returns the **same** winner, and that winner is the version that is maximal under
  the doc-22 total order (HLC physical, HLC logical, writer cluster ID, writer member ID,
  writer sequence). The checker recomputes the expected winner from the recorded write set
  and asserts each replica matches.
- **keep-siblings:** after convergence every replica returns the **same sibling set** = all
  pairwise-concurrent acked writes not dominated by a later write (doc 06). No acked
  concurrent write is missing from the sibling set; no spurious sibling is present.
- **Eventual observation:** every acked write is eventually observed by all live replicas
  (either as winner, as a sibling, or as dominated-by-a-later-version — never simply lost).

### set / grow-only set (no acked element lost)

Add elements to a set-valued key (grow-only: adds only; or OR-set semantics emulated at the
client over keep-siblings).

- **Invariant:** after convergence, the set read from every live replica contains **every
  acked added element**. No acked add is ever lost post-heal. This is the canonical
  "G-set convergence" property and is exactly what an eventually consistent store must
  guarantee even though native CRDT-set merge is deferred post-v1 (doc 06): the harness
  emulates the merge at the client over keep-siblings and asserts no acked element is dropped
  by replication/repair/anti-entropy.
- A deleted element (tombstone) is governed by the configured policy; for grow-only there are
  no deletes, isolating the no-loss property.

### list-append + cycle detection (Elle-style, eventual scope)

Per-key append-only lists; each op appends a unique value or reads the current list. We adapt
Elle's dependency-cycle reasoning to the level WaveSpan claims, **not** serializability.

- **Recovered orders:** from a read returning list `[a, b, c]` we recover write-order edges
  `a -> b -> c` (the read observed that order). Across reads we build the dependency graph.
- **What we assert (in scope):**
  - **Convergence of order:** after heal, every replica's read of a key yields the **same**
    list order — there is one converged order per key, deterministic under the conflict
    policy. Two replicas disagreeing on the final order post-convergence is a violation.
  - **Causal/session order (when session tokens are used):** a list never drops or reorders
    *this session's own* appends relative to each other — the session sees its writes in the
    order it issued them (read-your-writes, doc 00). A cycle that implies a session observed
    its own append out of order is a real violation.
  - **No lost append:** every acked append appears in the converged list (ties to the set
    no-loss property).
- **Explicitly out of scope:** full serializability cycle detection (G0/G1c/G2 anomalies
  across keys, write-skew, real-time order). WaveSpan does not promise serializable
  transactions (doc 00), so an inter-key dependency cycle that would be a serializability
  anomaly is **not** a WaveSpan violation and the checker must not flag it. We detect only
  cycles that violate (a) single-key converged order or (b) a session's own causal order.

### idempotency (one logical mutation per request ID)

Issue writes carrying a stable `request_id` / mutation ID; retry them across partitions,
reconnects, and origin restarts.

- **Invariant:** the same `request_id` produces **exactly one** logical mutation. After all
  retries and convergence, the key reflects a single application of that write — counters do
  not double-count, lists do not contain the value twice, the dedupe set absorbed the
  replays. This is doc 22 "Idempotent retry" / doc 06 receiver dedupe, end to end.
- The generator deliberately duplicates ops (same request_id) across partition boundaries and
  across the origin-crash retry path (doc 13 "Pod crash before nearby ACK") to force the
  dedupe path.

### durability (origin+1)

The doc-00/doc-13 acknowledgement rule: an acked write has at least two durable copies on
distinct nodes at the ACK instant.

- **Invariant:** for each acked write, identify the origin and **kill the origin immediately
  after ACK** (nemesis `kill-origin-after-ack`). The value must remain readable from the
  nearby replica that ACKed, and repair must restore target-N afterward (doc 13 acceptance
  "Kill origin immediately after successful origin+1 write; value remains readable").
- **Permitted exception:** if the *second* node also dies before target-N repair completes,
  loss is allowed (doc 13 "Not guaranteed in v1"). The checker asserts survival only while at
  least one of {origin replica, first nearby replica} survives, matching property 1's "unless
  the second node dies immediately after ACK".

### monotonic session reads (read-your-writes never goes backwards)

Use the optional session token (doc 00 "Session read-your-writes").

- **Invariant:** within one session, reads carrying the session token never observe a version
  **older** than a version this session has already observed or written. The session
  watermark is monotonic. A read that returns a value/version strictly behind the session's
  high-water version is a violation.
- This holds **continuously** (not just post-heal): it is a per-session property the model
  promises whenever the session token is presented, even across reconnects and cache
  resubscribes (doc 13 "Cache source failure" must downgrade/refetch, not serve behind the
  session watermark).

### TTL (lazy expiry within the staleness bound)

Write keys with a TTL; observe expiry.

- **Invariant (liveness):** an expired key **eventually disappears** from all live replicas
  within the doc-03 staleness bound `maxExpiredVisibility = bucketSize + sweepInterval +
  replicationLag`. The checker waits up to that bound (plus a margin) before asserting
  absence; it never asserts immediate expiry (TTL is approximate, doc 00).
- **Invariant (safety):** TTL expiry **never breaks convergence**. After expiry, all replicas
  agree the key is gone (tombstone converged); a key must not be alive on one replica and
  expired on another after the bound elapses with no new writes. Remote clusters honor the
  origin `expires_at` and do not extend lifetime by recomputing from apply time
  (doc 03/doc 06) — the checker writes from cluster A, reads expiry on cluster B against the
  **origin** deadline.
- For `hideExpiredOnRead=true` namespaces the read-time filter must hide an expired record
  even before the sweeper produces a tombstone (doc 03); the checker asserts that tighter
  read behavior in that mode.

## Nemeses

Nemeses are fault injectors under `tests/harness/nemesis/`, borrowed from CockroachDB
roachtest / Jepsen and executed through the **runner fault hooks (doc 24)** plus the
per-module fault-injection hooks (doc 17). Each nemesis exposes `Start`, `Stop` (heal), and a
deterministic schedule driven by the run seed. Nemeses **compose** with any workload.

| Nemesis | Fault | Maps to model fact |
|---|---|---|
| `node-kill` / `node-restart` | SIGKILL a node, later restart | spot-node disappearance (doc 13) |
| `kill-origin-after-ack` | kill the origin pod the instant after a write ACK | durability property 1 (doc 13 acceptance) |
| `pause` / `resume` | SIGSTOP then SIGCONT a node | stalled pod, stale-on-return (doc 13) |
| `empty-volume-restart` | restart a node with a fresh storage UUID | empty replacement volume = new member (doc 00, doc 13) |
| `partition-halves` | split the cluster into two halves | intra-cluster partition; both sides write (doc 13) |
| `partition-asymmetric` | one-way / overlapping (majority-minority) partition | asymmetric reachability, stale holder directory |
| `latency` | inject N ms RTT between groups | latency graph degradation (doc 04) |
| `packet-loss` | drop X% of packets between groups | timeout/loss penalty in closeness (doc 00) |
| `clock-skew-bounded` | skew a node's clock within `maxClockSkewMs` | HLC merge stays correct (doc 22) |
| `clock-skew-beyond` | skew beyond `maxClockSkewMs` (default 500ms) | skew rejection/clamp + metric (doc 22) |
| `disk-fill` / `disk-stall` | fill volume to pressure / stall I/O | disk-pressure placement penalty, out-log budget (doc 06, doc 23) |
| `gateway-restart` | restart stateless gateways | client reconnect, no data loss |
| `rolling-drain` | drain nodes one at a time (operator path) | drain/upgrade (doc 11, M11) |
| `cluster-partition` | partition cluster A from cluster B | global outage; out-log queues; AE repairs (doc 06) |

Each nemesis records its action and timing into the same history the workload writes to, so a
forensic dump shows ops and faults on one timeline.

## Invariant checkers

Checkers live under `tests/harness/checker/` and consume the recorded history. Each checker
maps onto one or more of the five **property tests in doc 16**:

| Checker | Asserts | Doc-16 property |
|---|---|---|
| `durability` | acked write has ≥2 distinct-node durable copies at ACK; survives `kill-origin-after-ack` (unless second node also dies) | **Property 1** |
| `convergence` | after heal with no new writes, all live nodes agree on winner/sibling set per key | **Property 2** |
| `lww-determinism` | LWW winner equals the doc-22-maximal version, order-independent | **Property 3** |
| `completeness-honesty` | a scan never labels a partial result `COMPLETE` without a valid range coverage certificate | **Property 4** |
| `idempotency` | same `request_id` => exactly one logical mutation across retries/partitions | **Property 5** |
| `no-lost-update-per-policy` | no acked write/element lost after convergence, evaluated *per configured policy* (LWW may drop a concurrent overwrite by design; keep-siblings/set must not drop) | supports 2 |
| `session-monotonicity` | session read-your-writes watermark never regresses | model (doc 00); continuous |
| `ttl-bound` | expired keys gone within `maxExpiredVisibility`; expiry preserves convergence | model (doc 03); supports 2 |

`completeness-honesty` deserves emphasis: it is doc-16 **property 4** and the doc-03 contract
"Do not silently return partial cache scans as complete scans." The checker drives scans
under partition (when cache coverage is necessarily incomplete) and asserts the
`ScanHeader.completeness` is `PARTIAL`/`BEST_EFFORT` unless a valid, unexpired
`RangeCoverageCertificate` is attached. A `COMPLETE` label without a certificate is a hard
failure — this is the cache's honesty invariant.

`no-lost-update-per-policy` is policy-aware on purpose: under `hlc-last-write-wins` a
concurrent blind overwrite *is allowed* to win and erase the other (doc 06 "can lose
concurrent updates"), so the checker must **not** flag that as loss; it flags loss only where
the policy promises no loss (keep-siblings retains both; the set/grow-only workload retains
every acked element).

## Methodology

Reuse the `testing-waves` mechanics, scaled to a cluster:

1. **Deterministic seeds.** One `int64` seed drives op generation, key/value selection, and
   the nemesis schedule. The same seed reproduces the same run. Recorded in every result and
   forensic dump (as `testing-waves` records `globalSeq` on failure).
2. **History recording.** Every op records `{op, key, value, request_id, session, start, end,
   ack: ok|fail|unknown, served_by, observed_version}` and every nemesis records
   `{fault, targets, start, end}`. The history is the single source of truth for every
   checker and is dumped on violation.
3. **Forensic dump on violation.** On the first violation, raise a stop flag (as
   `testing-waves` raises `stopAll`), halt generators, and dump: the seed, the offending op(s)
   and surrounding window, the per-replica reads that disagree (for convergence/LWW), the
   nemesis timeline, and the recomputed expected value. Mirror `dumpFailure`'s "expected vs
   got, which keys drifted" detail at cluster scope.
4. **Shrinking / minimal repro.** On violation, re-run with the recorded seed and op log,
   then **shrink**: bisect/delete ops and faults that are not necessary to reproduce, the way
   `repro_test.go` reduced the skiplist tear to two keys + one writer + one reader. The output
   is a standalone, deterministic Go test (`tests/harness/repro/<id>_test.go`) using only the
   public client, runnable with `go test -run`. A reproduced bug becomes a permanent
   regression test, exactly as `testing-waves` did.
5. **Configurable generators.** Concurrency, key-space size, value size, op mix
   (read/write/delete/CAS/append ratios), session usage, and conflict policy are all flags, so
   one workload can be a fast smoke or a wide soak.

## Where it runs

| Tier | Scope | Trigger |
|---|---|---|
| **PR gate (fast)** | small cluster (Apple-container local path, doc 24), short duration, a handful of nemeses (`node-kill`, `partition-halves`, `kill-origin-after-ack`, `clock-skew-bounded`), every workload's quick variant | every PR; must pass to merge |
| **Nightly soak (full)** | large clusters incl. two-cluster global (Docker/Linux CI path, doc 24), long duration, **all** nemeses composed, larger key-space, all workloads | nightly; gates release |

The PR gate is the regression net; the nightly soak is where rare, timing-dependent bugs (the
class the skiplist bug belonged to) surface. Both run pure-Go with no CGO.

The harness must also **catch a deliberately injected bug** to prove it is not vacuously
green: disabling repair, or skipping the receiver dedupe, or serving a partial scan as
`COMPLETE`, must make the relevant checker fail. This negative-control discipline is borrowed
directly from `testing-waves`'s RepeatableRead sweep (a known-weak mode the harness *must*
observe failing).

## Source layout

```text
tests/harness/
  runner/          # cluster lifecycle, seed, history recording, result + forensic dump, shrinker
    runner.go      # orchestrate: bring up cluster, run workload+nemesis, collect history
    history.go     # op/fault history types, append, serialize, dump
    seed.go        # deterministic RNG, schedule derivation
    shrink.go      # minimal-repro shrinking; emit standalone repro test
    cluster.go     # cluster handle: nodes by member ID, clusters by ID, per-replica read
  client/          # thin wrapper over the WaveSpan public client used by all workloads
  workloads/
    bank/          # conservation-under-convergence (generalized testing-waves bank)
    register/      # per-key history; LWW determinism; sibling set
    set/           # grow-only / OR-set no-loss
    listappend/    # list-append + eventual/causal cycle detection
    idempotency/   # request_id exactly-once
    durability/    # origin+1 kill-after-ack
    session/       # monotonic read-your-writes
    ttl/           # lazy expiry within staleness bound
  nemesis/
    nemesis.go     # Nemesis interface (Start/Stop/schedule), composition
    kill.go partition.go pause.go latency.go loss.go clockskew.go disk.go drain.go
  checker/
    durability.go convergence.go lww.go completeness.go idempotency.go
    nolostupdate.go session.go ttl.go
    checker.go     # Checker interface, history -> []Violation
  repro/           # generated standalone regression tests from shrunk failures
```

`tests/harness/` is the M14 deliverable. `tests/chaos/` (M12, doc 17) becomes a thin
build-tagged entry point that invokes this harness for the TS-102 nightly convergence gate
rather than a bespoke suite.
