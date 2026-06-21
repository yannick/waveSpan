# M09 — Vector Exact Search Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store raw vectors in WavesDB, compute exact distances (cosine/dot/L2), and expose exact distributed top-k search through the Cypher `vector.searchExact` procedure.

**Architecture:** Vectors are `VectorRecord`s in WavesDB, optionally attached to a graph node. Exact search scans candidate raw vectors per partition, computes distance (SIMD where the platform allows), keeps a local top-k heap, and the coordinator merges per-fragment top-k into a global top-k. Vector partitioning is `hash(graph_id+node_id)` for graph-attached vectors and `hash(collection_id+vector_id)` for bare vectors. `VectorIndex` CRD spec parsing feeds index metadata (dimensions, metric, label/property). The `vector.searchExact` Cypher procedure plugs into the M08 planner's procedure-call hook.

**Tech Stack:** Go, `github.com/cwire/wavespan`, `proto/wavespan/v1` vector messages, optional SIMD via `golang.org/x/sys/cpu` feature detection with a portable scalar fallback, fixture-based oracle tests.

**Depends on:** M01 (record envelope), M07 (vector raw records replicate through the global stream — TS-084 is the M10 integration), M08 (Cypher planner + procedure-call hook, graph attachment). TS-080/081/082.

---

## Context

Roadmap M9, doc `08_vector_engine.md`. This milestone ships **exact** vector search only; ANN/HNSW and the delta index are M10 (TS-083/084). Three tickets:

- **TS-080 raw vector storage** — `VectorRecord` format, keys, graph attachment, persistence across restart.
- **TS-081 exact search** — distance functions + per-partition scan + distributed top-k merge matching a test oracle.
- **TS-082 vector Cypher procedures** — `vector.searchExact(indexName, queryVector, k)` wired into the M08 planner.

Key constraints:

- Distance metrics required: **cosine, dot product, L2** (doc 08). Normalize at ingest when the index policy is cosine-optimized.
- **Exact search correctness is over records visible to the local node under eventual consistency** — it is not globally fresh (doc 08 "Exact search algorithm"). The oracle in tests must compare against the *locally visible* set, not a hypothetical global truth.
- Conflict read policy default `winner-only` (doc 08 "Conflict handling"): exact search reads the winning sibling only; tombstoned/expired records are filtered.
- Partitioning: graph-attached `hash(graph_id + node_id)`; bare `hash(collection_id + vector_id)`.
- This milestone does **not** build any approximate index. The `VectorIndex` CRD's `approximate` block is parsed and stored but unused until M10.

## File Structure

```
proto/wavespan/v1/common.proto             # add VectorRecord, VectorMeta (if not present)
internal/vector/record.go                   # VectorRecord encode/decode over StoredRecord; keys
internal/vector/store.go                    # Put/Get/Delete raw vectors, graph attachment, scan candidates
internal/vector/distance.go                 # cosine/dot/L2 scalar implementations
internal/vector/distance_simd.go            # SIMD-accelerated paths (build-tagged) + cpu feature dispatch
internal/vector/exact.go                    # per-partition exact top-k scan + heap
internal/vector/topk.go                     # bounded top-k heap + fragment merge
internal/vector/partition.go                # partition key: graph-attached vs bare
internal/vector/indexmeta.go                # VectorIndex CRD spec parsing -> in-process index metadata
internal/cypher/planner/proc_vector.go      # vector.searchExact procedure bound into the planner proc-call hook
fixtures/vector/embeddings.jsonl            # vectors + queries + exact top-k oracle (computed offline)
tests/integration/vector_exact_test.go
```

## Tasks

### Task 1: Proto — VectorRecord / VectorMeta

**Files:**
- Modify: `proto/wavespan/v1/common.proto`

- [ ] **Step 1:** Add `VectorRecord` exactly as doc 08 (`collection`, `vector_id`, `repeated float values`, `dtype`, `dimensions`, `metadata map<string,Value>`, optional `graph_node_id`, `version`, `tombstone`) and a `VectorMeta` for the `/meta/` key.
- [ ] **Step 2:** `make proto`; expect compile.
- [ ] **Step 3:** Commit.

### Task 2: Raw vector storage with graph attachment (TS-080)

**Files:**
- Create: `internal/vector/record.go`, `internal/vector/store.go`, `internal/vector/partition.go`
- Test: `internal/vector/store_test.go`, `internal/vector/partition_test.go`

Keys (doc 08): `/vector/{collection}/raw/{vector_id}`, `/vector/{collection}/meta/{vector_id}`.

- [ ] **Step 1:** Write failing tests:
  - `TestVectorPutGet` — put a `VectorRecord`, get it back equal.
  - `TestVectorPersistsAcrossRestart` — put, close+reopen the wavesdb-backed store, get the same vector (acceptance for TS-080).
  - `TestGraphAttachment` — a vector with `graph_node_id` set is retrievable both by `(collection,vector_id)` and via a graph-node->vector lookup.
  - `TestPartitionRouting` — graph-attached uses `hash(graph_id+node_id)`; bare uses `hash(collection_id+vector_id)`; both deterministic.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement record codec, `Store` over `wavesdb` (Put/Get/Delete, ScanCollection candidate iterator, tombstone + winner-only filtering), and `partition.go`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Distance functions (cosine / dot / L2) with SIMD fallback (TS-081)

**Files:**
- Create: `internal/vector/distance.go`, `internal/vector/distance_simd.go`
- Test: `internal/vector/distance_test.go`

- [ ] **Step 1:** Write failing tests with hand-checked values:
  - `TestCosine`, `TestDot`, `TestL2` against known vectors (e.g. orthogonal vectors -> cosine 0; identical -> cosine 1).
  - `TestSimdMatchesScalar` — for random vectors, the SIMD path (when available) produces results equal to the scalar path within float tolerance.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement scalar `Cosine/Dot/L2` in `distance.go`. In `distance_simd.go` add a dispatcher selecting an accelerated path when `cpu.X86.HasAVX2` (or ARM equivalent) is set, falling back to scalar otherwise. Keep the public API metric-agnostic: `Distance(metric, a, b)`.
- [ ] **Step 4:** Run, expect PASS (SIMD test skips gracefully where unsupported).
- [ ] **Step 5:** Commit.

### Task 4: Exact top-k scan + distributed merge (TS-081)

**Files:**
- Create: `internal/vector/topk.go`, `internal/vector/exact.go`
- Test: `internal/vector/exact_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestTopKHeap` — bounded heap of size k keeps the k best by score.
  - `TestExactSearchSinglePartition` — over a small in-memory set, exact top-k matches a brute-force oracle for cosine/dot/L2.
  - `TestExactDistributedMerge` — split the set across 3 fragments, each returns local top-k, coordinator merge equals the global brute-force top-k.
  - `TestExactFiltersTombstonesAndSiblings` — tombstoned vectors and losing siblings are excluded (winner-only).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `topk.go` (max-size min-heap on distance), `exact.go` `SearchPartition(query, k, metric, filter)` scanning candidates and computing distance via Task 3, plus `MergeTopK(fragmentResults, k)` for the coordinator. Apply graph/property filters if provided before scoring (doc 08 step 1).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: VectorIndex CRD spec parsing -> index metadata (TS-082 prerequisite)

**Files:**
- Create: `internal/vector/indexmeta.go`
- Test: `internal/vector/indexmeta_test.go`

- [ ] **Step 1:** Write failing test `TestParseVectorIndexSpec` — given the `VectorIndex` spec (doc 12: `label`, `property`, `dimensions`, `dtype`, `metric`, `exact.enabled`, `approximate.*`, `visibility`, `replicationPolicyRef`), produce an in-process `IndexMeta` with metric/dimensions/exact-enabled resolved; `approximate` fields are captured but inert in M09. Add `TestRejectMissingDimensions` — dimensions 0/absent is an error (CRD validation rule, doc 12).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `IndexMeta` and `ParseVectorIndexSpec`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: `vector.searchExact` Cypher procedure (TS-082)

**Files:**
- Create: `internal/cypher/planner/proc_vector.go`
- Test: `internal/cypher/planner/proc_vector_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestVectorSearchExactProcedure` — `CALL vector.searchExact('doc-embedding', $query, 5) YIELD node, score RETURN node, score` returns the 5 nearest by exact distance, in score order.
  - `TestVectorSearchExactHybrid` — combine with a graph match: `CALL vector.searchExact(...) YIELD node, score MATCH (node)-[:PART_OF]->(d:Document) RETURN d.title, score` joins vector hits to graph nodes (doc 08 hybrid example).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the procedure binding: register `vector.searchExact` in the planner's procedure-call hook (added in M08). It resolves the index by name -> `IndexMeta`, routes exact search fragments across partitions, merges top-k, maps `vector_id`/`graph_node_id` to graph nodes for `YIELD node, score`, and produces `QueryMeta` (consistency=eventual, partial_graph_possible per routing).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: Fixture oracle + integration suite

**Files:**
- Create: `fixtures/vector/embeddings.jsonl`
- Create: `tests/integration/vector_exact_test.go`

- [ ] **Step 1:** Generate `embeddings.jsonl`: a few hundred small (e.g. 8–16 dim) vectors with ids, a handful of query vectors, and the precomputed exact top-k oracle per metric (computed by an offline brute-force step recorded in the file or a sibling `.expected` file).
- [ ] **Step 2:** Write `vector_exact_test.go` (`//go:build integration`): load fixtures, run `vector.searchExact` for each query/metric, assert results equal the oracle; attach a subset to graph nodes and assert a hybrid graph+vector query works; restart the store mid-test and assert the replicated/persisted vector is searchable after apply.
- [ ] **Step 3:** Run `go test -tags integration ./tests/integration -run VectorExact`. Expect PASS.
- [ ] **Step 4:** Commit.

## Acceptance Criteria

From roadmap M9 + TS-080/081/082:

- **Exact top-k matches the test oracle** — per metric (cosine/dot/L2), exact search returns exactly the brute-force nearest neighbors over locally visible records (`TestExactDistributedMerge`, integration oracle).
- **Graph + exact vector query works** — a hybrid Cypher query joining `vector.searchExact` hits to graph nodes returns the expected rows (`TestVectorSearchExactHybrid`).
- **Replicated vector is searchable after apply** — a vector record persisted (or applied from the global stream) is found by exact search once visible locally (`TestVectorPersistsAcrossRestart` + integration apply case).
- Vector put/get persists across restart (TS-080). Exact top-k matches oracle (TS-081). Cypher exact vector query works (TS-082).
- Tombstoned/losing-sibling vectors are filtered (winner-only); CRD `dimensions` validation rejects 0/missing.

## Verification

1. **Unit:** `go test ./internal/vector/...` — record persistence, partitioning, distance correctness, SIMD==scalar, top-k heap + distributed merge, tombstone/sibling filtering, CRD spec parsing.
2. **Procedure unit:** `go test ./internal/cypher/planner -run VectorSearchExact` — procedure binding + hybrid query.
3. **Fixture oracle integration:** `go test -tags integration ./tests/integration -run VectorExact` — exact results equal the offline brute-force oracle for every metric.
4. **Restart/visibility drill:** put a vector, restart the store, confirm `vector.searchExact` returns it; tombstone it and confirm it disappears from results.
