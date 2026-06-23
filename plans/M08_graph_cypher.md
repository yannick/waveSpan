# M08 — Graph Storage and Cypher Subset Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a property-graph database over a production subset of Cypher, stored in WavesDB, with distributed fragment execution bounded by guardrails and eventual-consistency-aware response metadata.

**Architecture:** Graph nodes/edges are encoded as records in dedicated WavesDB column families with adjacency and index keys (doc 07 key layout). A Cypher parser produces an AST for the v1 subset; a planner builds logical then physical plans and routes scan/expand fragments to the pods owning the relevant `hash(graph_id+node_id)` ranges, bounded by `maxRemoteFragments=128`. Graph mutations are a single `wavesdb.Txn` across graph CFs on the coordinator pod (atomic locally, eventually consistent cross-pod). Every response carries `QueryMeta` including `partial_graph_possible`.

**Tech Stack:** Go, `github.com/yannick/wavespan`, hand-written recursive-descent Cypher parser, `proto/wavespan/v1/cypher.proto`, fixture-based query tests.

**Depends on:** M01 (record envelope, WavesDB CFs), M02–M04 (membership, placement, holder directory, routed scans), M07 (graph records replicate through the global mutation stream — reuse, not required to run). TS-070/071/072/073.

---

## Context

Roadmap M8, doc `07_graph_cypher.md`. This is the graph layer. Four ticket boundaries:

- **TS-070 graph record encoding** — `NodeRecord`/`EdgeRecord` protos, adjacency keys (out/in), by-id edge lookup.
- **TS-071 indexes** — label index and property index, both *derived* from authoritative records and rebuildable.
- **TS-072 parser** — recursive-descent parser for the required v1 subset: `MATCH`, `OPTIONAL MATCH`, `WHERE`, `CREATE`, `SET`, `DELETE`, `RETURN`, `WITH`, `UNWIND`, `ORDER BY`, `LIMIT`, `SKIP`. (`MERGE`/`REMOVE`/`DETACH DELETE` are recommended-after-core; stub them as parse-recognized-but-unsupported errors, not silent.)
- **TS-073 planner/executor** — logical + physical plans, distributed fragment routing, guardrails, `QueryMeta`.

Key product constraints:

- **Graph mutation atomicity is local-batch on the coordinator** (doc 07 "Graph mutation atomicity"). A `CREATE`/`SET`/`DELETE` writes the node/edge records, label/property index entries, and adjacency entries in **one `wavesdb.Txn`**. Distributed global atomicity is NOT guaranteed; if related records map to different pods, queries may observe partial graph state — this must surface via `QueryMeta.partial_graph_possible = true`.
- **Fragment routing is bounded by `maxRemoteFragments: 128`** and all guardrails in doc 07 (`maxRowsReturned`, `maxIntermediateRows`, `maxTraversalDepth: 8`, `queryTimeoutMs`, `maxMemoryBytes`). Enforce from day one — unbounded traversal is a hard failure, not a warning.
- Indexes are derived: on mutation write the authoritative record, append the index mutation, update local indexes (sync if cheap else async), and **filter index hits against current record state** at query time (an index entry may point at a stale/tombstoned record).
- Conflict policy defaults per doc 07 table (node properties record-level LWW for v1 — labels-as-OR-set is deferred to the CRDT work in M07's deferred set; use record-level LWW for labels in v1 and note it).

## File Structure

```
proto/wavespan/v1/cypher.proto             # add NodeRecord, EdgeRecord, Value, QueryMeta, Cypher service (Query stream), QueryConsistency, Completeness
internal/graph/keys.go                      # key builders: node, label, edge out/in/by_id, prop index
internal/graph/record.go                    # NodeRecord/EdgeRecord encode/decode over StoredRecord envelope
internal/graph/store.go                     # CreateNode/Edge, GetNode/Edge, adjacency scans, single-Txn mutation batch
internal/graph/index.go                     # label + property index maintenance, rebuild-from-records, stale filtering
internal/graph/partition.go                 # hash(graph_id+node_id) -> range/pod; affinity helpers for fragment routing
internal/cypher/parser/lexer.go             # tokenizer
internal/cypher/parser/parser.go            # recursive-descent parser -> AST
internal/cypher/parser/ast.go               # AST node types
internal/cypher/planner/logical.go          # logical operators (label scan, prop seek, expand, filter, project, sort, limit, unwind)
internal/cypher/planner/physical.go         # physical plan: local scan, holder fetch, routed scan, adjacency expand, remote fragment
internal/cypher/planner/router.go           # fragment routing by range-directory affinity, maxRemoteFragments bound
internal/cypher/planner/executor.go         # execute plan, merge rows, build QueryMeta + guardrail enforcement
internal/cypher/planner/guardrails.go       # limits config + enforcement (rows, depth, memory, timeout, fragments)
fixtures/graph/social.cypher                # social graph fixture (nodes/edges) + expected query results
tests/integration/graph_query_test.go
```

## Tasks

### Task 1: Proto — graph records, Value, QueryMeta, Cypher service

**Files:**
- Modify: `proto/wavespan/v1/cypher.proto`, `proto/wavespan/v1/common.proto` (Value if not already present)

- [ ] **Step 1:** Add `NodeRecord` and `EdgeRecord` exactly as doc 07 (node_id, labels, properties map<string,Value>, version, tombstone; edge adds start/end/type). Add `Value` (oneof: null/bool/int64/double/string/bytes/list/map) if not in common.proto. Add `QueryMeta` per doc 07 (`served_by_cluster_id`, `participating_members`, `consistency`, `completeness`, `used_cache`, `partial_graph_possible`, `warnings`). Add enums `QueryConsistency`, `Completeness`. Add `service Cypher { rpc Query(CypherRequest) returns (stream CypherResult); }` where `CypherResult` carries rows + a terminal `QueryMeta`.
- [ ] **Step 2:** `make proto`; expect compile.
- [ ] **Step 3:** Commit.

### Task 2: Graph key encoding and records (TS-070)

**Files:**
- Create: `internal/graph/keys.go`, `internal/graph/record.go`
- Test: `internal/graph/keys_test.go`, `internal/graph/record_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestNodeEdgeKeyRoundTrip` — key builders produce the doc-07 layouts (`/graph/{graph}/node/{id}`, `/graph/{graph}/edge/out/{src}/{type}/{dst}/{edge}`, in, by_id, `/graph/{graph}/label/{label}/{node}`, `/graph/{graph}/prop/{label}/{prop}/{encval}/{node}`) and sort so adjacency prefix scans return edges grouped by (src,type).
  - `TestRecordEncodeDecode` — NodeRecord/EdgeRecord encode into the M01 `StoredRecord` envelope and decode back equal.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement key builders (order-preserving value encoding for the property index so range seeks work) and record codecs.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Graph store — create/get + single-Txn mutation batch (TS-070)

**Files:**
- Create: `internal/graph/store.go`, `internal/graph/partition.go`
- Test: `internal/graph/store_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestCreateAndGetNodeEdge` — create node a, node b, edge a-[:FOLLOWS]->b; get each by id; assert outgoing/incoming adjacency scans return the edge.
  - `TestMutationBatchIsSingleTxn` — a create touching node+label+prop+edge+out-adj+in-adj+by_id commits atomically: inject a failure mid-batch and assert nothing is written (no partial node without its label entry).
  - `TestPartitionKeyStable` — `Partition(graph, nodeId)` is deterministic `hash(graph_id+node_id)`.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `Store` over `wavesdb`: `CreateNode`, `CreateEdge`, `GetNode`, `GetEdge`, `ScanOutgoing/ScanIncoming`, and `ApplyMutationBatch(ops)` that opens one `wavesdb.Txn`, writes all record + index + adjacency + mutation-log entries, and commits atomically. Implement `partition.go` hashing.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: Label and property indexes (TS-071)

**Files:**
- Create: `internal/graph/index.go`
- Test: `internal/graph/index_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestLabelScan` — create nodes with labels; `ScanLabel("User")` returns exactly the User node ids.
  - `TestPropertySeek` — `SeekProperty("User","age",30)` returns nodes with age==30; range seek `age >= 30` returns the sorted suffix.
  - `TestIndexRebuildFromRecords` — wipe index CF, run `RebuildIndexes(graph)`, assert label/property scans return the same results (indexes are derived).
  - `TestStaleIndexEntryFiltered` — tombstone a node but leave its label entry; `ScanLabel` filtered against current record state excludes it.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement index maintenance hooked into `ApplyMutationBatch`, a `RebuildIndexes` full scan, and a `filterAgainstRecords` step every scan passes through.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: Cypher lexer + parser for the v1 subset (TS-072)

**Files:**
- Create: `internal/cypher/parser/lexer.go`, `internal/cypher/parser/ast.go`, `internal/cypher/parser/parser.go`
- Test: `internal/cypher/parser/parser_test.go`

- [ ] **Step 1:** Write failing tests parsing each required clause into the expected AST: `MATCH (n:User)-[:FOLLOWS]->(m) WHERE n.age > 30 RETURN m.name ORDER BY m.name SKIP 2 LIMIT 10`; `OPTIONAL MATCH`; `CREATE (a:User {id:'1'})-[:FOLLOWS]->(b:User {id:'2'})`; `MATCH (n) SET n.x = 1`; `MATCH (n) DELETE n`; `WITH n.x AS x WHERE x > 0 RETURN x`; `UNWIND [1,2,3] AS i RETURN i`. Add `TestUnsupportedClauseIsExplicitError` — `MERGE`/`LOAD CSV`/arbitrary `CALL` (other than `vector.*`, added in M09) return a clear "unsupported in v1" parse error, not a silent no-op.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the lexer (keywords, identifiers, literals, operators, parameters `$x`) and a recursive-descent parser producing `ast.Query` with reading/updating clauses. Recognize `MERGE`/`REMOVE`/`DETACH DELETE` tokens and reject with "recommended-after-core, unsupported in v1".
- [ ] **Step 4:** Run, expect PASS (the fixture queries in `fixtures/graph/social.cypher` parse).
- [ ] **Step 5:** Commit.

### Task 6: Logical + physical planner with fragment routing (TS-073)

**Files:**
- Create: `internal/cypher/planner/logical.go`, `internal/cypher/planner/physical.go`, `internal/cypher/planner/router.go`, `internal/cypher/planner/guardrails.go`
- Test: `internal/cypher/planner/planner_test.go`, `internal/cypher/planner/router_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestLogicalPlanShape` — `MATCH (n:User) WHERE n.age>30 RETURN n` plans to LabelScan -> PropertyFilter -> Projection.
  - `TestExpandUsesAdjacency` — a relationship pattern plans an ExpandOutgoing over the adjacency scan, not a full scan.
  - `TestRouterRespectsMaxRemoteFragments` — a plan that would touch >128 partitions is capped at `maxRemoteFragments=128` and the result is marked `partial_graph_possible`/warned (doc 07).
  - `TestGuardrailsEnforced` — exceeding `maxIntermediateRows` / `maxTraversalDepth` aborts with a guardrail error.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement logical operators (label scan, prop seek, expand out/in, type filter, prop filter, projection, sort, limit, skip, unwind), lower to physical (local range scan, holder fetch, routed scan, adjacency expansion, remote fragment, row merge), and `router.go` that maps target partitions to pods via the range directory affinity (M02/M04), capping at 128. `guardrails.go` enforces the doc-07 `cypher:` limits.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: Executor, row merge, QueryMeta (TS-073)

**Files:**
- Create: `internal/cypher/planner/executor.go`
- Test: `internal/cypher/planner/executor_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestExecuteMatchReturn` — against an in-memory store loaded with the social fixture, `MATCH (n:User)-[:FOLLOWS]->(m) RETURN m.name` returns the expected rows.
  - `TestExecuteCreateSetDelete` — CREATE then MATCH sees it; SET updates a property; DELETE tombstones and subsequent MATCH excludes it. All via `ApplyMutationBatch` (single Txn).
  - `TestQueryMetaPartialGraphPossible` — a query routed across >1 pod sets `partial_graph_possible=true`; a fully-local query sets it false.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the executor: drive physical operators, merge fragment rows (sorted merge for ORDER BY, apply SKIP/LIMIT after merge), and assemble `QueryMeta` (served_by_cluster_id, participating_members, consistency=eventual, completeness, used_cache, partial_graph_possible, warnings). Mutations go through `graph.Store.ApplyMutationBatch`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 8: Social graph fixture + integration query suite

**Files:**
- Create: `fixtures/graph/social.cypher`
- Create: `tests/integration/graph_query_test.go`

- [ ] **Step 1:** Author `social.cypher`: ~20 `User` nodes, `FOLLOWS` edges, a few properties (name, age, city), plus a table of canonical queries and their expected result sets (the oracle).
- [ ] **Step 2:** Write `graph_query_test.go` (`//go:build integration`): load fixture into a single-node store, run each canonical query, assert results match the oracle; assert `QueryMeta` declares consistency/freshness; wipe and `RebuildIndexes`, re-run, assert identical results.
- [ ] **Step 3:** Run `go test -tags integration ./tests/integration -run GraphQuery`. Expect PASS.
- [ ] **Step 4:** Commit.

## Acceptance Criteria

From roadmap M8 + TS-070/071/072/073:

- **Social graph fixture queries pass** — every canonical query in `fixtures/graph/social.cypher` returns the expected rows (`TestExecuteMatchReturn`, integration suite).
- **Graph indexes rebuild from records** — `RebuildIndexes` reconstructs label and property indexes from authoritative records; queries return identical results before and after (`TestIndexRebuildFromRecords`).
- **Query metadata declares cache/freshness** — responses include `QueryMeta` with consistency, completeness, used_cache, and `partial_graph_possible` (`TestQueryMetaPartialGraphPossible`).
- Create/read node and edge by ID works (TS-070). Label scan and property seek return expected nodes (TS-071). Fixture queries parse (TS-072). `MATCH`/`WHERE`/`RETURN` and `CREATE`/`SET`/`DELETE` execute correctly (TS-073).
- Guardrails (`maxRowsReturned`, `maxIntermediateRows`, `maxTraversalDepth=8`, `maxRemoteFragments=128`, `queryTimeoutMs`, `maxMemoryBytes`) are enforced and exceeding them errors clearly.
- Graph mutations are atomic within a single coordinator `wavesdb.Txn`; cross-pod partial state is surfaced, never hidden.

## Verification

1. **Unit:** `go test ./internal/graph/... ./internal/cypher/...` — keys, records, single-Txn batch, indexes + rebuild + stale filtering, parser subset + explicit unsupported errors, planner shapes, fragment cap, guardrails, executor.
2. **Fixture integration:** `go test -tags integration ./tests/integration -run GraphQuery` — fixture oracle match + rebuild equivalence + QueryMeta assertions.
3. **Guardrail drill:** craft a query that would touch >128 partitions or exceed `maxIntermediateRows`; confirm it is capped/aborted with a guardrail error and (for the fragment cap) `partial_graph_possible=true` with a warning.
4. **Atomicity drill:** inject a mid-batch failure into `ApplyMutationBatch`; confirm a subsequent MATCH sees neither the node nor its index entries (no torn graph write on the coordinator).
