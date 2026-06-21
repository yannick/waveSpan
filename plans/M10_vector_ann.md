# M10 — Vector ANN and Delta Index Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add approximate nearest-neighbor (ANN) search over an HNSW index, a mutable delta index for write-visibility, background segment merge, exact reranking, per-partition index rebuild, and integration with global raw-vector replication.

**Architecture:** An `ANNIndex` abstraction wraps an HNSW implementation. Writes go to a small mutable **delta index** for immediate visibility; a background worker merges the delta into immutable main segments. Search queries both the main segments and the delta, merges candidates, fetches raw `VectorRecord`s to filter tombstones/stale/conflicted entries, and optionally exact-reranks the candidate set (reusing M09 exact distance). Globally, only **raw vectors** replicate (M07 stream); each cluster rebuilds its ANN locally from applied raw records. `vector.searchApprox` plugs into the M08 planner's procedure hook.

**Tech Stack:** Go, `github.com/cwire/wavespan`, HNSW (decision below), M09 exact-distance kernels for reranking, `proto/wavespan/v1` vector messages, recall/latency benchmark harness.

**Depends on:** M07 (global raw-vector replication — TS-084), M09 (raw vector storage, exact distance, top-k merge, `IndexMeta`, procedure hook). TS-083/084.

---

## Context

Roadmap M10, doc `08_vector_engine.md`. Two tickets:

- **TS-083 ANN index and delta** — ANN abstraction + HNSW, mutable delta index, background segment merge, `vector.searchApprox`, exact reranking, per-partition rebuild, benchmark.
- **TS-084 vector global replication integration** — replicate raw vectors globally and update remote local indexes (vector written in cluster A becomes searchable in cluster B after apply).

### HNSW implementation decision (record this in an ADR)

**Decision: implement HNSW in pure Go. cgo is prohibited.** `CGO_ENABLED=0` is a project-wide hard
rule (`design/24_container_dev_and_testing.md`): every binary is statically linked and shipped from a
`FROM scratch` image. A cgo-backed ANN (hnswlib/faiss bindings) would break static builds, multi-arch
cross-compilation, and the scratch images — so it is **not** an available option, not merely
discouraged. This also keeps single-binary builds and race-detector coverage of the whole node.

The `ANNIndex` interface still isolates the implementation. If, during the benchmark task, pure-Go
HNSW cannot hit the recall/latency target, the escape hatch is an **out-of-process ANN service**
behind the same `ANNIndex` interface (its own container, reached over Connect/gRPC) — never an
in-process cgo binding. Do not silently switch; record the escalation in the ADR.

### Constraints

- Visibility model `write-visible-with-delta` (doc 08): write raw record -> append mutation log -> origin+1 local -> insert into delta index -> return success -> background merge -> global apply updates remote delta indexes.
- Search path: main ANN segments + delta -> merge -> fetch raw records -> filter tombstones/TTL/conflicts -> optional exact rerank -> top-k.
- ANN/exact-block indexes are **derived**; rebuild from raw vector records per partition (doc 08 "Index rebuild": scan, skip tombstones/expired, group by partition, build immutable segment, atomically publish, GC old after no queries reference it).
- Global v1 replicates raw vectors only — **never HNSW internals** as authoritative data (doc 06/08). Each cluster rebuilds/updates its own ANN.

## File Structure

```
adr/ADR-XXXX-hnsw-pure-go.md               # the pure-Go/no-cgo HNSW decision above
internal/vector/ann/ann.go                  # ANNIndex interface (Insert/Delete/Search/Snapshot/Stats)
internal/vector/ann/hnsw.go                 # pure-Go HNSW: graph layers, efConstruction/efSearch, M
internal/vector/delta.go                    # mutable delta index over recent inserts/deletes
internal/vector/segment.go                  # immutable ANN segment build/publish/GC
internal/vector/merge.go                    # background worker: merge delta -> main segment
internal/vector/rebuild.go                  # per-partition rebuild from raw records
internal/vector/rerank.go                   # exact reranking of ANN candidates (uses M09 distance)
internal/cypher/planner/proc_vector.go      # add vector.searchApprox (extends M09 file)
internal/replication/global/vector_apply.go # on applied raw vector mutation -> update local delta index
tests/bench/vector_ann_bench_test.go        # recall@k + latency benchmark, emits a report
tests/integration/vector_ann_test.go
tests/integration/vector_global_test.go     # A->B searchable-after-apply (TS-084)
```

## Tasks

### Task 1: ADR + ANNIndex interface

**Files:**
- Create: `adr/ADR-XXXX-hnsw-pure-go.md`, `internal/vector/ann/ann.go`
- Test: `internal/vector/ann/ann_test.go`

- [ ] **Step 1:** Write the ADR capturing the pure-Go HNSW decision, the `CGO_ENABLED=0` hard rule that prohibits a cgo binding, and the out-of-process swap-in escape hatch behind `ANNIndex`.
- [ ] **Step 2:** Write failing test `TestANNInterfaceContract` against a trivial in-memory brute-force `ANNIndex` implementation (used as a test double): Insert vectors, Search returns nearest by metric, Delete tombstones, Stats reports size.
- [ ] **Step 3:** Define `type ANNIndex interface { Insert(id, vec, metric); Delete(id); Search(query, k, params) []Candidate; Snapshot() Segment; Stats() }` and a brute-force reference impl in the test.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 2: Pure-Go HNSW (TS-083)

**Files:**
- Create: `internal/vector/ann/hnsw.go`
- Test: `internal/vector/ann/hnsw_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestHNSWRecallVsExact` — build an HNSW over a few thousand vectors; for sample queries, recall@10 vs the M09 exact oracle is above a threshold (e.g. ≥0.95 with reasonable `efSearch`).
  - `TestHNSWParamsHonored` — `m`, `efConstruction`, `efSearchDefault` from `IndexMeta` are applied; higher `efSearch` raises recall.
  - `TestHNSWDelete` — deleted ids are not returned.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement HNSW in pure Go: hierarchical layers, greedy search with `ef` candidate lists, neighbor selection with `m`/`mMax`, using M09 distance kernels. No cgo. The `ANNIndex` interface is the only seam; any future out-of-process backend implements it without a build-tagged cgo file.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Mutable delta index (TS-083)

**Files:**
- Create: `internal/vector/delta.go`
- Test: `internal/vector/delta_test.go`

Delta key (doc 08): `/vector/{collection}/delta/{index_id}/{seq}`.

- [ ] **Step 1:** Write failing tests:
  - `TestDeltaImmediateVisibility` — insert a vector into the delta; a search over (main ∅ + delta) finds it immediately (acceptance: "new vector visible through delta index").
  - `TestDeltaTombstone` — a delete in the delta hides a vector present in a main segment (acceptance: "tombstoned vectors filtered").
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement a small in-memory delta (brute-force searchable) backed by the persisted delta mutation log for crash recovery; `Insert`, `Delete`, `Search`, `Drain()` (for merge).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: Immutable segments + background merge (TS-083)

**Files:**
- Create: `internal/vector/segment.go`, `internal/vector/merge.go`
- Test: `internal/vector/merge_test.go`

Segment key (doc 08): `/vector/{collection}/ann/{index_id}/{segment_id}/...`.

- [ ] **Step 1:** Write failing tests:
  - `TestMergeDrainsDeltaIntoSegment` — after inserts into the delta and a merge cycle, the vectors live in a published main segment and the delta is empty; search results unchanged across the merge.
  - `TestOldSegmentGCAfterNoReaders` — after a new segment is published and in-flight queries drain, the old segment is GC'd.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `segment.go` (build immutable segment from a vector set, persist, atomically publish metadata, refcount readers, GC) and `merge.go` (background worker: drain delta, build/merge segment, publish, GC old).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: Per-partition rebuild + exact reranking (TS-083)

**Files:**
- Create: `internal/vector/rebuild.go`, `internal/vector/rerank.go`
- Test: `internal/vector/rebuild_test.go`, `internal/vector/rerank_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestRebuildFromRawRecords` — wipe all ANN segments/delta, run `RebuildPartition`, scan raw records (skipping tombstones/expired), assert search returns correct results — index is fully derived.
  - `TestExactRerank` — ANN returns approximate candidates; `Rerank(query, candidates, k)` reorders them by exact distance and matches the exact top-k for that candidate set.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `RebuildPartition` (scan raw -> filter -> group by partition -> build segment -> publish) and `Rerank` reusing M09 exact distance over fetched raw vectors.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: `vector.searchApprox` Cypher procedure (TS-083)

**Files:**
- Modify: `internal/cypher/planner/proc_vector.go`
- Test: `internal/cypher/planner/proc_vector_approx_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestVectorSearchApprox` — `CALL vector.searchApprox('doc-embedding', $q, 50, {efSearch:200}) YIELD node, score RETURN node, score` returns k candidates via main+delta search, tombstone/stale filtered.
  - `TestVectorSearchApproxRerank` — with rerank requested, results are exact-reranked; ordering matches the exact top-k over the ANN candidate set.
  - `TestVectorSearchApproxHybrid` — chains into a graph MATCH/WHERE filter (doc 08 published-documents example).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Register `vector.searchApprox` in the procedure hook: resolve `IndexMeta`, search main segments + delta across partitions with `efSearch`, merge candidates, fetch raw records, filter tombstones/TTL/conflicts (winner-only), optional exact rerank, merge fragment top-k, emit `QueryMeta`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: Global raw-vector apply -> local index update (TS-084)

**Files:**
- Create: `internal/replication/global/vector_apply.go`
- Test: `internal/replication/global/vector_apply_test.go`

- [ ] **Step 1:** Write failing test `TestAppliedVectorEntersDeltaIndex` — feed a raw `VectorRecord` mutation through the M07 applier; assert the local delta index now contains it and `vector.searchApprox` finds it (the cluster-B-after-apply path), without any HNSW internals crossing the wire.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the apply hook: when the M07 `Applier` writes a raw vector record (winning version), insert/delete it in the local delta index for the matching `index_id`; background merge folds it in later. Tombstone applies as a delta delete.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 8: Recall/latency benchmark + integration suites

**Files:**
- Create: `tests/bench/vector_ann_bench_test.go`, `tests/integration/vector_ann_test.go`, `tests/integration/vector_global_test.go`

- [ ] **Step 1:** Write `vector_ann_bench_test.go`: build an index over a fixture set, sweep `efSearch`, measure recall@k vs the M09 exact oracle and query latency, and emit a report (printed table / written artifact). This is the "ANN recall/latency benchmark produced" deliverable.
- [ ] **Step 2:** Write `vector_ann_test.go` (`//go:build integration`): delta visibility end-to-end, tombstone filtering, rebuild equivalence.
- [ ] **Step 3:** Write `vector_global_test.go` (`//go:build integration`): two clusters (reuse `docker/docker-compose.global.yaml` from M07); write a vector in A; assert it becomes searchable via `vector.searchApprox` in B after apply (TS-084 acceptance), and confirm no ANN segment bytes crossed the replication stream (only raw records).
- [ ] **Step 4:** Run `go test -tags integration ./tests/integration -run 'VectorANN|VectorGlobal'` and `go test -bench . ./tests/bench`. Expect PASS + a benchmark report.
- [ ] **Step 5:** Commit.

## Acceptance Criteria

From roadmap M10 + TS-083/084:

- **New vector visible through the delta index** — a freshly written vector is returned by `vector.searchApprox` immediately via the delta, before background merge (`TestDeltaImmediateVisibility`).
- **ANN recall/latency benchmark produced** — the benchmark emits recall@k and latency across `efSearch`, demonstrating the pure-Go HNSW meets the target (`vector_ann_bench_test.go`).
- **Tombstoned vectors filtered** — deleted/tombstoned vectors never appear in approximate results (`TestDeltaTombstone`, `TestVectorSearchApprox`).
- **Vector written in A becomes searchable in B after apply** — global replication ships the raw vector; B rebuilds/updates its local index and finds it; no HNSW internals replicate (`TestAppliedVectorEntersDeltaIndex`, `vector_global_test.go`) (TS-084).
- ANN abstraction + HNSW, mutable delta, background merge, exact rerank, and per-partition rebuild all implemented and derived from raw records (TS-083).
- The HNSW decision is recorded in an ADR; pure-Go HNSW is the backend (cgo prohibited by `CGO_ENABLED=0`), with the out-of-process escape hatch documented behind `ANNIndex`.

## Verification

1. **Unit:** `go test ./internal/vector/... ./internal/vector/ann/...` — HNSW recall vs exact, delta visibility/tombstone, segment merge + GC, rebuild equivalence, exact rerank.
2. **Procedure unit:** `go test ./internal/cypher/planner -run VectorSearchApprox`.
3. **Benchmark:** `go test -bench . -benchmem ./tests/bench` — inspect the emitted recall@k/latency table; confirm recall meets the ADR target at the default `efSearch`.
4. **Global integration:** with `docker compose -f docker/docker-compose.global.yaml up -d`, run `go test -tags integration ./tests/integration -run VectorGlobal`; confirm a vector written in A is found by `searchApprox` in B and that `global_repl_bytes_*` reflects only raw-record sizes (no segment payloads).
