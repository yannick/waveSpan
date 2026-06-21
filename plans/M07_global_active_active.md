# M07 — Global Active-Active Replication Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let two or more WaveSpan Kubernetes clusters all accept writes and converge asynchronously, with no cross-cluster consensus on the hot path.

**Architecture:** Every committed local mutation is appended to a per-peer, partitioned outbound replication log on disk. Replicators stream those entries to peer clusters over a gRPC `PushGlobal` stream; receivers apply mutation envelopes idempotently into local WavesDB through a conflict resolver (HLC-LWW by default, keep-siblings opt-in). A periodic anti-entropy pass exchanges per-range hash summaries to repair anything lost during outages. Out-log disk is bounded per peer with backpressure that, by default, never blocks local writes.

**Tech Stack:** Go, `github.com/cwire/wavespan` (imports `wavesdb` in-process), gRPC (`proto/wavespan/v1/replication.proto`), Prometheus metrics, docker-compose for two-cluster integration tests.

**Depends on:** M01 (WavesDB wrapper, versioned record envelope), M02 (membership), M03/M04 (origin+1 + target-N fanout, mutation log), TS-060/061/062/063.

---

## Context

This milestone (roadmap M7, doc `06_global_active_active_replication.md`) adds the cross-cluster replication layer. It reuses the local mutation log produced by M03/M04 but ships those mutations to *peer clusters* instead of nearby pods, and applies inbound mutations through a pluggable conflict resolver.

Hard scope constraints for v1:

- **Only HLC-LWW and keep-siblings resolvers ship.** CRDT policies (G-counter, PN-counter, OR-set, LWW-register, append log) and the application/WASM resolver are **interface-only / deferred** — define the `ConflictResolver` interface and register the two concrete resolvers, but do not implement CRDTs.
- **Default local writes never wait for global replication.** Out-log is best-effort and bounded. The single exception is namespaces flagged `globalDurabilityRequired`, which backpressure (block the local write ACK) when the out-log is full.
- **Backpressure / log retention rule (doc 06 "Backpressure"):** retain logs up to a configured per-peer disk budget; only compact/drop the **oldest** entries **after** an anti-entropy checkpoint has confirmed the peer caught up past them. Never drop un-checkpointed entries silently.
- Mutation identity is `cluster_id + member_id + writer_sequence`; receivers must ignore already-applied IDs (replay protection).
- Graph and vector raw records flow through this same protocol (consumed by M08–M10); design the envelope to carry any record type, keyed by column family + key.

## File Structure

```
proto/wavespan/v1/replication.proto        # add GlobalReplication service: PushGlobal stream, AntiEntropy RPCs, GlobalMutation envelope
internal/config/global.go                  # ClusterPeer config struct, globalReplication block, globalDurabilityRequired flag parsing
internal/replication/global/peer.go        # ClusterPeer connection manager (dial, reconnect, health)
internal/replication/global/outlog.go      # per-peer partitioned outbound log: append, iterate, checkpoint, bounded compaction
internal/replication/global/inlog.go       # inbound log: idempotent dedupe by mutation_id, apply ordering
internal/replication/global/sender.go      # outbound stream: drain out-log -> PushGlobal, backpressure + disk budget
internal/replication/global/receiver.go    # inbound stream server: receive PushGlobal, write in-log, hand to applier
internal/replication/global/applier.go     # apply GlobalMutation via conflict resolver into local WavesDB
internal/replication/global/antientropy.go # range Merkle/hash summaries, summary exchange, divergent-range repair, checkpoint advance
internal/replication/global/metrics.go     # global_repl_* Prometheus collectors
internal/conflict/resolver.go              # ConflictResolver interface + ResolveResult; registry by policy name
internal/conflict/hlc_lww.go               # HLC last-write-wins resolver (default), tombstone-wins-if-version-wins
internal/conflict/keep_siblings.go         # keep-siblings resolver
internal/conflict/deferred.go              # interface stubs documenting deferred CRDT/app resolvers (no impl)
docker/docker-compose.global.yaml          # two clusters (A 3 nodes, B 3 nodes) wired as peers
tests/integration/global_replication_test.go
```

Tests live beside their packages as `*_test.go` (e.g. `internal/conflict/hlc_lww_test.go`).

## Tasks

### Task 1: Proto — GlobalReplication service and envelope

**Files:**
- Modify: `proto/wavespan/v1/replication.proto`
- Create: regenerated Go stubs under `proto/wavespan/v1/` (via `make proto`)

- [ ] **Step 1:** Add to `replication.proto`:
  - `message GlobalMutationId { string cluster_id = 1; string member_id = 2; uint64 writer_sequence = 3; }`
  - `message GlobalMutation { GlobalMutationId id = 1; string column_family = 2; bytes key = 3; bytes record = 4; wavespan.v1.Version version = 5; bool tombstone = 6; uint32 partition = 7; int64 origin_expires_at = 8; }` (record bytes is the M01 `StoredRecord` envelope so graph/vector reuse it)
  - `service GlobalReplication { rpc PushGlobal(stream GlobalMutation) returns (stream PushGlobalAck); rpc RangeSummary(RangeSummaryRequest) returns (RangeSummaryResponse); rpc FetchRange(FetchRangeRequest) returns (stream GlobalMutation); }`
  - `PushGlobalAck { uint64 applied_through_seq = 1; uint32 partition = 2; }`
  - anti-entropy messages: `RangeSummaryRequest{ repeated KeyRange ranges = 1; }`, `RangeSummaryResponse{ repeated RangeHash hashes = 1; }`, `RangeHash{ bytes range_start = 1; bytes range_end = 2; bytes hash = 3; }`, `FetchRangeRequest{ KeyRange range = 1; }`.
- [ ] **Step 2:** Run `make proto`. Expected: regenerated `replication.pb.go` / `replication_grpc.pb.go` compile.
- [ ] **Step 3:** Commit.

### Task 2: ClusterPeer config and connection management (TS-060)

**Files:**
- Create: `internal/config/global.go`
- Create: `internal/replication/global/peer.go`
- Test: `internal/replication/global/peer_test.go`

- [ ] **Step 1:** Write failing test `TestPeerManagerDialsAndReconnects` — start two in-process gRPC servers, configure a peer pointing at one, assert the manager reports `Connected`; stop the server, assert it transitions to `Disconnected` and retries; restart, assert it reconnects.
- [ ] **Step 2:** Run, expect FAIL (package not built).
- [ ] **Step 3:** Implement `config.ClusterPeer{ClusterId, Geo, GossipEndpoint, ReplEndpoint, TLSSecretName}` and `config.GlobalReplication{Mode, Peers []ClusterPeer, ReadPolicy, AntiEntropyIntervalSeconds, OutLogDiskBudgetBytes}` parsed from YAML (matches doc 06 + CRD `ReplicationPolicy.global`). Implement `PeerManager` that dials each peer with backoff, exposes `Conn(clusterId)` and per-peer status, and a `Watch()` for status changes.
- [ ] **Step 4:** Run test, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Per-peer outbound log with bounded retention (TS-061, backpressure)

**Files:**
- Create: `internal/replication/global/outlog.go`
- Test: `internal/replication/global/outlog_test.go`

Layout key (doc 06): `/repl/global/out/{peer_cluster}/{partition}/{seq}` in a dedicated WavesDB column family.

- [ ] **Step 1:** Write failing tests:
  - `TestOutLogAppendIterate` — append N mutations across partitions, iterate from a seq cursor in order per partition.
  - `TestOutLogCheckpointCompaction` — set a small disk budget; append past it; assert **nothing is dropped until `Checkpoint(peer, partition, seq)` advances**; after checkpoint, oldest entries below checkpoint are compactable; entries above checkpoint are retained even over budget.
  - `TestOutLogBackpressureSignal` — when over budget and no checkpoint has advanced, `Append` returns `ErrOutLogFull` for `globalDurabilityRequired` callers but a non-blocking `dropOldestAfterCheckpoint=false` path for default callers (default path keeps appending; budget is enforced only via post-checkpoint compaction).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `OutLog` over the WavesDB CF: monotonic per-(peer,partition) seq, `Append`, `IterateFrom`, `Checkpoint`, `CompactBelowCheckpoint`, byte accounting, and the dual budget behavior. Document the rule inline: *drop oldest only after AE checkpoint*.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: Outbound sender stream (TS-061)

**Files:**
- Create: `internal/replication/global/sender.go`
- Test: `internal/replication/global/sender_test.go`

- [ ] **Step 1:** Write failing test `TestSenderShipsToReceiver` — wire a sender to a fake receiver server; append mutations to the out-log; assert the receiver gets them in order and the sender advances its sent-cursor on ack. Add `TestSenderResumesAfterDisconnect` — drop the receiver mid-stream, assert sender re-dials and resumes from last acked seq (no gaps, no duplicates beyond idempotent replay).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `Sender` per peer: open `PushGlobal` stream, drain `OutLog.IterateFrom(sentCursor)`, send, advance cursor on `PushGlobalAck`, reconnect via `PeerManager`. Hook a tap so the KV/graph/vector write path appends to every peer's out-log after local commit (`globalDurabilityRequired` namespaces block on `Append` returning `ErrOutLogFull`).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: ConflictResolver interface + HLC-LWW + keep-siblings (TS-062)

**Files:**
- Create: `internal/conflict/resolver.go`, `internal/conflict/hlc_lww.go`, `internal/conflict/keep_siblings.go`, `internal/conflict/deferred.go`
- Test: `internal/conflict/hlc_lww_test.go`, `internal/conflict/keep_siblings_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestHLCLWWDeterministic` — two concurrent versions with equal HLC resolve to a deterministic winner (tie-break by `cluster_id` then `member_id`); higher HLC always wins.
  - `TestHLCLWWTombstoneWinsIfVersionWins` — tombstone with higher version wins; tombstone with lower version loses (doc 06 "Delete conflicts").
  - `TestKeepSiblingsReturnsBoth` — two concurrent non-dominating versions return `Siblings([2])`; a dominating version collapses siblings to a single `Winner`.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement:
  - `type ResolveResult` (Go union: `Kind` + payload) with `Winner | Siblings | Tombstone | Reject` mirroring doc 06's enum.
  - `type ConflictResolver interface { Resolve(existing []StoredRecord, incoming StoredRecord) ResolveResult }`.
  - `Registry` mapping policy name -> resolver; register `"hlc-last-write-wins"` and `"keep-siblings"`.
  - `deferred.go`: documented no-op/`panic("deferred: v1 ships HLC-LWW + keep-siblings only")` stubs for `crdt-*` and `application` so the policy surface is visible but unimplemented.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: Inbound receiver + idempotent applier (TS-061/062)

**Files:**
- Create: `internal/replication/global/inlog.go`, `internal/replication/global/receiver.go`, `internal/replication/global/applier.go`
- Test: `internal/replication/global/applier_test.go`

In-log key (doc 06): `/repl/global/in/{origin_cluster}/{partition}/{seq}`.

- [ ] **Step 1:** Write failing tests:
  - `TestApplyIdempotent` — applying the same `GlobalMutationId` twice applies once (dedupe set persisted).
  - `TestApplyUsesNamespaceResolver` — a namespace configured keep-siblings stores both versions; default namespace LWW keeps the winner.
  - `TestApplyTTLUsesOriginExpiry` — applied record's `expires_at == origin_expires_at` (never recomputed from apply time, doc 06 "TTL in global replication").
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `Receiver` (gRPC server side of `PushGlobal`: persist to in-log, ack `applied_through_seq`) and `Applier`: read incoming, dedupe by mutation_id, load existing local record(s), select resolver from the namespace/graph/index policy, write the resolved result + tombstones in a single `wavesdb.Txn`, preserve `origin_expires_at`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: Anti-entropy range summaries (TS-063)

**Files:**
- Create: `internal/replication/global/antientropy.go`
- Test: `internal/replication/global/antientropy_test.go`

- [ ] **Step 1:** Write failing test `TestAntiEntropyRepairsMissedMutation` — two appliers over two stores; drop one mutation from the stream into cluster B; run an AE round; assert B converges to A. Add `TestAntiEntropyAdvancesCheckpoint` — after a successful round, the out-log checkpoint for that range advances so compaction is now allowed below it.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement: divide keyspace into ranges; compute per-range hash (Merkle/rolling hash over (key,version) tuples visible locally); exchange `RangeSummary`; for divergent ranges call `FetchRange` and feed records through the `Applier`; on completion advance `OutLog.Checkpoint` so Task 3 compaction can reclaim disk. Schedule via `AntiEntropyIntervalSeconds`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 8: Global lag and conflict metrics (TS-061/063)

**Files:**
- Create: `internal/replication/global/metrics.go`
- Test: `internal/replication/global/metrics_test.go`

- [ ] **Step 1:** Write failing test asserting the registry exposes every metric from doc 06: `global_repl_out_lag_seconds`, `global_repl_in_lag_seconds`, `global_repl_bytes_sent_total`, `global_repl_bytes_received_total`, `global_repl_conflicts_total`, `global_repl_conflicts_by_policy_total`, `global_repl_anti_entropy_runs_total`, `global_repl_anti_entropy_divergent_ranges_total`, `global_repl_apply_errors_total`.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement collectors; wire counters/gauges from sender, applier, and anti-entropy. Out-lag = now − origin HLC wall time of newest un-shipped entry; in-lag = now − origin time of newest applied entry per origin cluster.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 9: Two-cluster docker-compose + end-to-end integration test

**Files:**
- Create: `docker/docker-compose.global.yaml`
- Create: `tests/integration/global_replication_test.go`

- [ ] **Step 1:** Author `docker-compose.global.yaml`: cluster A (`a1,a2,a3`, clusterId `test-a`) and cluster B (`b1,b2,b3`, clusterId `test-b`), each node configured with the other cluster as a `ClusterPeer`, insecure dev mode TLS off, distinct gossip/repl ports.
- [ ] **Step 2:** Write `global_replication_test.go` (build-tagged `//go:build integration`) covering the four acceptance criteria below: bidirectional replication, deterministic convergence under concurrent writes, keep-siblings returns siblings, outage queues + resumes.
- [ ] **Step 3:** Run `go test -tags integration ./tests/integration -run GlobalReplication`. Expected: PASS.
- [ ] **Step 4:** Commit.

## Acceptance Criteria

From roadmap M7 + TS-060/061/062/063:

- **Two Docker clusters replicate both directions** — a write in cluster A appears in cluster B and vice versa (`TestGlobalBidirectional`).
- **Concurrent writes converge deterministically** — concurrent writes to the same key in A and B converge to the same HLC-LWW winner on both sides (`TestGlobalConvergence`).
- **Keep-siblings returns siblings** — a namespace with keep-siblings policy returns both concurrent versions to the client (`TestKeepSiblingsEndToEnd`).
- **Outage queues logs and resumes** — partition B from A, write into A (out-log retains entries up to the disk budget, never blocking default local writes), heal the partition, assert B converges; if `globalDurabilityRequired` and the budget is exhausted, the A write ACK blocks until drain (`TestOutageQueueAndResume`).
- Two Docker clusters connect (TS-060). Missed mutation during an outage is repaired by anti-entropy (TS-063).
- CRDT/application resolvers are present as interface stubs only and never invoked in v1 paths.

## Verification

1. **Unit:** `go test ./internal/conflict/... ./internal/replication/global/...` — all green; resolvers, out-log retention rule, idempotent apply, anti-entropy convergence, metrics presence.
2. **Two-cluster end-to-end:** `docker compose -f docker/docker-compose.global.yaml up -d` then `go test -tags integration ./tests/integration -run GlobalReplication`. Confirm `global_repl_in_lag_seconds` on B drops toward 0 after writes to A via `/metrics`.
3. **Outage drill:** with compose up, `docker network disconnect` B's nodes from A's network; drive writes into A; confirm out-log bytes grow but A's local write latency is unaffected (default namespace); reconnect; confirm `global_repl_anti_entropy_divergent_ranges_total` increments then convergence holds.
4. **Backpressure assertion:** set a tiny `OutLogDiskBudgetBytes`, mark a namespace `globalDurabilityRequired`, keep the peer down; confirm writes to that namespace block (return a deadline/backpressure error) while default-namespace writes still succeed — and that **no un-checkpointed out-log entry is dropped**.
