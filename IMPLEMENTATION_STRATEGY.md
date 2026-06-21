# WaveSpan implementation strategy

This is the master strategy document for building WaveSpan, the Kubernetes-native,
eventually-consistent distributed database described under `design/`. It fixes the
language/stack decision, the milestone dependency graph, the testing and correctness
strategy, the risk register, and the map from milestones to plan files and tickets.

Read this before opening any individual milestone plan in `plans/`. The design docs under
`design/` remain the source of truth for behavior; this document is the source of truth for
*sequencing and execution*.

## 1. Stack decision

WaveSpan and every one of its processes are written in **Go**.

- **Data node** (`wavespan-node`), **gateway** (`wavespan-gateway`), and **CLI**
  (`wavespanctl`): Go.
- **Operator** (`wavespan-operator`): Go with Kubebuilder, in a separate module under
  `operator/`.
- **Local storage engine**: the `wavesdb` Go LSM library, imported **in-process**. There is
  no FFI boundary on the data path. The data node calls `wavesdb` as ordinary Go.

This supersedes the Rust/C recommendation in `design/17_source_tree.md`, which predates the
decision to standardize on Go. The design docs occasionally still show Rust trait sketches
(for example `LocalStore` in `design/02_storage_wavesdb.md`); treat those as *behavioral
specifications*, not as the implementation language. Each milestone plan restates the
boundary as an idiomatic Go interface.

### Rationale

- **One language, one toolchain.** The data node, gateway, operator, and CLI share proto
  types, config types, and test harnesses. A single `go build` graph removes the FFI/cgo
  complexity that the original Rust-data-node + C-FFI-storage split would have imposed on the
  hot path.
- **`wavesdb` is already Go.** `wavesdb` (module `wavesdb`, the Go successor to the C
  `tidesdb` engine) exposes `Open`, `CreateColumnFamily`, `Begin`/`Txn`, `Iterator`,
  `FlushMemtable`, `Compact`, and `Checkpoint` as a pure-Go API. Importing it directly is
  zero-FFI and gives us native column families and native LSM TTL.
- **Operator ecosystem.** Kubebuilder/controller-runtime is the strongest operator stack and
  is Go-native, matching `design/09_kubernetes_operator.md`.
- **Reuse `testing-waves`.** The existing `testing-waves` Jepsen-style bank-invariant harness
  is reused for distributed correctness rather than rebuilt.

### Reference-only material

- The C `tidesdb` engine is **reference-only**. It is the ancestor of `wavesdb` and is not
  part of the build, the module graph, or CI.
- `design/02_storage_wavesdb.md` is named for that ancestor but its *content* describes the
  `wavesdb`-backed storage layer. Where this strategy or a plan says "design doc
  `02_storage_wavesdb.md`", it refers to that same file.

### Module layout

```
waveSpan/
  go.mod                      module github.com/cwire/wavespan
                              replace wavesdb => ../wavesdb
  cmd/
    wavespan-node/            data pod process
    wavespan-gateway/         optional stateless gateway
    wavespanctl/              admin/client CLI
  proto/wavespan/v1/          common.proto kv.proto cypher.proto
                              replication.proto admin.proto
  internal/
    config/        version/        storage/
    membership/    latencygraph/   placement/
    replication/local/  replication/global/
    cache/         kv/             conflict/    ttl/
    graph/         cypher/parser/  cypher/planner/
    vector/        observability/  security/
  operator/                   separate go.mod, Kubebuilder
  docker/                     Dockerfile, docker-compose, scripts
  tests/integration/  tests/chaos/  fixtures/
```

The `internal/` module names map onto the crate responsibilities in
`design/17_source_tree.md` one-to-one (`replication-local` -> `internal/replication/local`,
`cache-subscriptions` -> `internal/cache`, `graph-core` -> `internal/graph`, and so on). The
dependency-direction rules from that doc are enforced: `storage` never imports `kv`,
`membership` never imports `kv`, the operator never imports data-node internals except
generated config/API types.

### Build and test platform

- **No CGO, static, scratch.** Every binary builds with `CGO_ENABLED=0`, statically linked,
  shipped `FROM scratch`, multi-arch (linux/arm64 + linux/amd64). This is a hard rule
  (`design/24_container_dev_and_testing.md`) and it forecloses any cgo dependency — notably the
  M10 vector HNSW is pure Go (no hnswlib/faiss binding); the escape hatch is an out-of-process
  ANN service, never cgo.
- **Apple `container` locally, Docker/Linux in CI.** Fast local multi-node clusters run on Apple's
  `container` + container machines (macOS/Apple Silicon); CI runs the identical OCI images on
  Linux. Apple `container` is never required in CI because the images are byte-identical.
- **buf + ConnectRPC.** `buf generate` emits Go + `connect-go` server stubs and `connect-es`
  client stubs from one proto set. The same services power internal RPC, `wavespanctl`, and the
  embedded UI. Live UI updates (gossip, data inspection) use Connect server-streaming, not a
  separate WebSocket layer.
- **Embedded UI.** `ui/` (Vite + React + TS) builds into the node binary via `go:embed`
  (`design/26_node_ui_and_observability.md`), so the scratch image is self-contained.
- **Correctness harness** (`design/25_correctness_harness.md`, built in M14) is the canonical
  chaos/property suite; M12 runs it rather than a bespoke one.

## 2. Milestone dependency graph

Milestones are dependency-ordered. The data path (M0->M4) is a strict spine; once target-N
and repair exist (M4), three independent tracks open up in parallel.

```
M0 bootstrap
   |
M1 storage (wavesdb wrapper, envelopes, UUID)
   |
M2 membership + latency graph
   |
M3 origin+1 KV (public KV, StoreReplica, placement, ACK rule)
   |
M4 target-N fanout + repair (holder directory, repair engine)
   |
   +--> M5 dynamic cache (FetchReplica, SubscribeKey, eviction)
   |       |
   +--> M6 scans + TTL (cache-fast/routed scans, coverage cert, TTL)
   |
   +--> M7 global active-active (peer streams, HLC-LWW, anti-entropy)

M1 --> M8 graph + Cypher subset
          |
          +--> M9 vector exact search
                   |
                   +--> M10 vector ANN + delta index

M0 --> M11 operator (CRDs, StatefulSet reconcile, drain)
            ^ needs M3 for a meaningful reconcile target

M2 + M3 --> M13 node UI + gossip observability (embedded Vite/React, ConnectRPC streams)

M3 --> M14 correctness harness (model-aware Jepsen workloads + CockroachDB nemeses)
          ^ full value after M7 (global) and M11 (drain/operator nemeses)

M7 + M11 + M14 --> M12 hardening (mTLS, auth, backup/restore, chaos via M14, dashboards)
```

### What runs in parallel

- **Spine (sequential):** M0 -> M1 -> M2 -> M3 -> M4. Each strictly depends on the prior.
  Do not parallelize within the spine.
- **After M4, three tracks fan out and can run concurrently:**
  - Track Cache/Scan: M5, then M6 (M6 also reuses M4 holder directory for routed scans).
  - Track Global: M7.
- **Graph/Vector track is independent of the cache/global tracks.** It forks off M1 (it only
  needs the storage envelopes and column families), so M8 -> M9 -> M10 can run in parallel
  with M5/M6/M7. M8 reuses `internal/kv` encoders but does not need the replication fanout.
- **Operator track** forks off M0 and can start as soon as the binary serves `/healthz`
  (M0). It produces a *meaningful* reconcile only once M3 exists (a node that forms a cluster
  and accepts writes), so land M11's reconcile acceptance against an M3+ binary.
- **M13 (UI + observability)** forks off M2/M3 — it needs gossip to observe and KV data to
  inspect — and can run alongside the cache/global/graph tracks. The topology view gets richer
  as M4 (repair) lands but does not block on it.
- **M14 (correctness harness)** forks off M3 and grows with the system: it starts with KV
  durability/idempotency/convergence workloads and adds nemeses as M7 (partition/conflict) and
  M11 (drain/empty-volume) land. M12 *consumes* M14 (it does not build a bespoke chaos suite).
- **M12 (hardening)** is the join point. It requires M7 (global path to secure and chaos-test
  cross-cluster), M11 (operator-deployed cluster to run nightly chaos against), and M14 (the
  harness it runs).

### Critical path

`M0 -> M1 -> M2 -> M3 -> M4 -> M7 -> M12` is the longest chain and defines the schedule. The
graph/vector track (M8->M10) and the cache/scan track (M5->M6) must finish before M12 only to
the extent M12 chaos-tests them; M12's hard prerequisites are M7 and M11.

## 3. Testing and correctness strategy

WaveSpan is eventually consistent. Tests verify **convergence, durability thresholds, and
metadata honesty** rather than pretending reads are linearizable. The strategy is layered;
each layer gates the next.

### Layer 1 - Unit tests (per module, every milestone)

Co-located `_test.go` files. Required coverage areas track
`design/16_testing_strategy.md`:

- **Versioning** (`internal/version`): HLC ordering, logical-counter tie-breaks, mutation-ID
  idempotency, tombstone ordering, proto encode/decode round-trips.
- **Conflict** (`internal/conflict`): LWW value/value, LWW delete/value, keep-siblings,
  determinism under shuffled input.
- **TTL** (`internal/ttl`): bucket assignment, best-effort hide-expired, tombstone emission,
  compaction eligibility.
- **Placement** (`internal/placement`): distinct-node filter, compliance-boundary hard fail,
  prefer-local-geo spillover, latency scoring, disk/load penalties.
- **Storage** (`internal/storage`): the same KV/scan suite runs against both the in-memory
  store and the `wavesdb`-backed store (table-driven, shared test corpus).

Every module ships unit tests, metrics hooks, and fault-injection hooks
(`design/17_source_tree.md` "Implementation rule").

### Layer 2 - docker-compose integration tests (`tests/integration/`)

The same `wavespan-node` binary runs as a 3-node (and for latency, 5-node) cluster via
`docker/docker-compose.yaml` (`design/10_docker_dev.md`). Tests drive it through
`wavespanctl` and the gRPC APIs. Required scenarios per
`design/16_testing_strategy.md` "Docker cluster tests":

- 3-node cluster forms gossip membership; 5-node cluster populates the latency graph.
- Write ACK requires one nearby durable replica (origin+1).
- Target-N fanout fills asynchronously; killing a holder triggers repair convergence.
- Read miss creates a dynamic cache replica; update propagates; source failure resyncs.
- `cache-fast` scan returns `BEST_EFFORT`; certified range cache returns `COMPLETE`.

### Layer 3 - Model-aware correctness harness (`tests/harness/`, design doc 25)

The `testing-waves` bank test is the seed for a full **model-aware** harness
(`design/25_correctness_harness.md`, built in M14): Jepsen-style workloads (bank, register, set,
list-append/Elle) reimplemented in Go to assert WaveSpan's *declared* model — convergence,
no-lost-update per policy, durability, idempotency, session monotonicity — plus CockroachDB-style
nemeses driven through the runner fault hooks (`design/24_container_dev_and_testing.md`). The
classic **bank invariant** (total balance conserved across concurrent transfers) runs against a
local Apple-container or docker-compose cluster. Because WaveSpan is eventually consistent, the invariant is checked **at
quiescence**: after a workload + fault window, stop writes, let anti-entropy/repair converge,
then assert the conserved total and per-key winner determinism. This is the primary
correctness signal for the replication and conflict layers (M3, M4, M7).

### Layer 4 - Nightly chaos (`tests/chaos/`)

Runs continuously in CI nightly (`design/16_testing_strategy.md` "Chaos tests"): kill random
container every 10-60s, delete a data dir and restart, pause a node 2 min, partition into
halves, inject 100ms latency, drop 10% packets, restart all gateways, simulate clock skew,
fill disk to pressure. Asserts the property tests below hold at quiescence.

### Property tests (must hold)

From `design/16_testing_strategy.md` "Property tests":

1. A successful write has >=2 durable copies on distinct nodes at the ACK instant (unless the
   second node dies immediately after ACK).
2. Repeated anti-entropy with no new writes converges all live nodes to the same
   winner/siblings.
3. The LWW resolver is deterministic under all message orders.
4. A dynamic cache never reports `COMPLETE` range coverage without a valid certificate.
5. Idempotent retry with the same request ID yields one logical mutation.

### CI merge gates (from `design/16_testing_strategy.md`)

Do not merge if any of these fail:

- origin+1 invariant fails;
- conflict resolver is nondeterministic;
- dynamic cache mislabels a partial scan as complete;
- global anti-entropy fails the convergence test;
- vector exact search returns the wrong top-k;
- graph index rebuild loses nodes/edges;
- operator generates an invalid StatefulSet.

Each milestone plan's Verification section states which gates it newly enables.

## 4. Risk register

| Risk | Impact | Milestone(s) most exposed | Mitigation |
|---|---|---|---|
| **Spot churn outpaces repair** — under-replication grows faster than the repair worker drains it. | Durability loss; property 1 violated. | M4, M12 | Priority queue keyed by under-replication severity; rate-limit + churn backpressure in the repair engine (`design/23_repair_engine.md`); `kv_under_replicated_keys_estimate` metric with alert; chaos test "kill every 10-60s" must converge. |
| **LWW data loss** — HLC last-write-wins silently drops a concurrent write. | Lost updates; not safe for counters/sets. | M7 | Make LWW deterministic and *observable* (siblings policy available per namespace, `conflictState` surfaced in every response); document non-safety; bank invariant uses keep-siblings or CRDT-style accounts where it matters. |
| **ANN recall drift** — delta index / background merge degrades recall over time. | Wrong vector results, silently. | M10 | Exact-rerank top candidates; nightly recall/latency benchmark with a regression gate; tombstone filter test; rebuild path validated. |
| **Go GC pauses on scan hot paths** — large range scans / vector merges allocate heavily and stall on GC. | Tail-latency spikes; subscription lag. | M6, M9, M10 | Bounded scan batches; reuse buffers and iterators (`wavesdb` `Iterator` is reusable); avoid per-row allocation in merge; load tests track p99 and `kv_cache_subscription_lag_ms`; set `GOGC`/`GOMEMLIMIT` in the node Dockerfile. |
| **Storage-UUID / identity confusion** — pod name reused, storage UUID not persisted. | Split-brain holder records. | M1, M2 | Persist storage UUID in CF `sys`; `memberId` is runtime, `storageUuid` is durable (`design/04`). |
| **Cache miss broadcast storm** — a miss fans out to all nodes. | O(N) amplification. | M5 | Resolve holders via range directory + compact holder summaries only; never broadcast (`design/README.md` hard rule 3). |

## 5. Milestone -> plan -> ticket map

| Milestone | Plan file | Tickets | Design docs |
|---|---|---|---|
| M0 bootstrap | `plans/M00_bootstrap.md` | TS-001, TS-002, TS-003 | 17, 18, 22, 14 |
| M1 storage (wavesdb) | `plans/M01_storage_wavesdb.md` | TS-010, TS-011, TS-012 | 02, 22 |
| M2 membership + latency | `plans/M02_membership_latency.md` | TS-020, TS-021, TS-022 | 04 |
| M3 KV origin+1 | `plans/M03_kv_origin_plus_one.md` | TS-030, TS-031, TS-032 | 03, 05, 11 |
| M4 target-N + repair | `plans/M04_targetn_and_repair.md` | TS-033, TS-034, TS-035 | 05, 23 |
| M5 dynamic cache | `plans/M05_dynamic_cache.md` | TS-040, TS-041, TS-042, TS-043 | 05 |
| M6 scans + TTL | `plans/M06_scans_and_ttl.md` | TS-050, TS-051, TS-052, TS-053 | 03 |
| M7 global active-active | `plans/M07_global_active_active.md` | TS-060, TS-061, TS-062, TS-063 | 06 |
| M8 graph + Cypher | `plans/M08_graph_cypher.md` | TS-070, TS-071, TS-072, TS-073 | 07 |
| M9 vector exact | `plans/M09_vector_exact.md` | TS-080, TS-081, TS-082 | 08 |
| M10 vector ANN | `plans/M10_vector_ann.md` | TS-083, TS-084 | 08 |
| M11 operator | `plans/M11_operator.md` | TS-090, TS-091, TS-092, TS-093 | 09, 12 |
| M12 hardening | `plans/M12_hardening.md` | TS-100, TS-101, TS-102 | 13, 14, 15, 16, 25 |
| M13 UI + observability | `plans/M13_ui_and_observability.md` | (new) | 26, 11, 04, 14 |
| M14 correctness harness | `plans/M14_correctness_harness.md` | (extends TS-102) | 25, 16, 13(failure model) |

Placement (TS-023) is folded into M3, where it is first needed for candidate selection. This
strategy doc and plans `M00`-`M06` are written now; `M07`-`M12` are written by parallel
agents against the same template.

Design docs `22_versioning_and_hlc.md` (HLC/version types) and `23_repair_engine.md` (repair
priority queue, rate limit, churn backpressure) are authored alongside these plans; M0/M1 and
M4 respectively treat them as canonical.

## 6. Interface and management plane

WaveSpan exposes **two data surfaces and one management surface** — see
`11_api_contracts.md` "Interface design rationale" for the full argument.

- **KV: dedicated typed gRPC** (`Put/Get/Delete/Scan/CompareAndSet/Watch`). No SQL. A point read is
  one framed protobuf round-trip; SQL would add per-call parse/plan cost and imply guarantees the
  engine refuses to make (no serializable txns, no globally consistent scans, no linearizable reads).
  Server-side `Scan` predicate pushdown (a typed `ScanFilter`, not a language) is the one declarative
  KV capability worth adding — **deferred past v1**, field reserved.
- **Graph/vector: Cypher subset** (`CypherService`) with vector procedures.
- **Management: one authoritative `AdminService` (gRPC).** HTTP `/admin/*` is a thin read-only/debug
  gateway over it; `wavespanctl` is a gRPC client of it; mutating ops (`TriggerRepair`, drain) require
  the operator/admin role. CRDs are the config source of truth in k8s; the *same* schema loads from
  YAML in Docker (generate both from one definition).
- **Transport:** uniform gRPC + mTLS everywhere in v1, including the internal replication/gossip hot
  path. This is chosen for uniformity and streaming, **not** because it is proven-optimal internally.
  If origin+1 latency misses its SLO, the first lever is a leaner intra-cluster binary transport
  behind the same Go interfaces — profile before replacing.

### Latency SLOs (v1 targets, single-region cluster)

The roadmap's acceptance criteria are behavioral; these add the missing numeric targets so "fast" is
measurable. Measured at the data pod (gateway adds its own budget), p50/p99, on the reference 3-node
docker-compose cluster under nominal load. Treated as **gates for the M12 load tests**, tunable as
hardware/benchmarks land:

| Operation | p50 | p99 | Notes |
|---|---|---|---|
| `Get` local durable hit | < 0.5 ms | < 2 ms | served from local wavesdb |
| `Get` dynamic-cache hit | < 0.5 ms | < 2 ms | second read after fetch |
| `Get` miss -> closest-holder fetch | < 3 ms | < 15 ms | one intra-cluster RPC + store |
| `Put` origin+1 ACK | < 3 ms | < 15 ms | local durable + one nearby durable replica |
| `CompareAndSet` | < 3 ms | < 15 ms | coordinator-local, best-effort |
| `Scan` cache-fast (per 1k rows) | < 5 ms | < 25 ms | streamed; watch Go GC on hot scans |
| target-N fill lag (background) | — | < 5 s | not on the ACK path |

These feed the alert thresholds in `14_observability.md` (e.g. `OriginPlusOneLatencyHigh`) and the
M12 load-test plan. If internal gRPC overhead pushes origin+1 p99 past 15 ms, apply the transport
lever above before widening the target.
