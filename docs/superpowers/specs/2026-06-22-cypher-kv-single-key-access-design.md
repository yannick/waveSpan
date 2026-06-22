# Cypher `kv.*` single-key read/write — Design

**Date:** 2026-06-22
**Status:** Approved (pending spec review)
**Component:** `internal/cypher`, `internal/kv`, `cmd/wavespan-node`

## Goal

Let a Cypher query read and write **single KV keys** against the *same* namespaced,
versioned, replicated key-value store that the gRPC KV API exposes. A key written by
Cypher must be readable by the KV API and vice-versa — one coherent KV, not a second
side-channel store.

This makes graph queries able to join against KV values (e.g. fetch a node's external
profile blob during a `MATCH`) and to mutate KV state as part of graph workflows.

## Non-goals (YAGNI)

Deliberately excluded; each is an additive follow-up that does not change this design:

- Binary/typed values beyond UTF-8 string (`kv.getBytes` / base64 variant).
- Range/scan (`kv.scan`) or batch (`kv.multiGet`).
- Atomic graph+KV transactions (a `kv.put` in a statement that also mutates the graph
  commits independently of the graph `wavesdb.Txn`).
- Default-namespace shorthand (namespace is always explicit).

## Decisions (settled during brainstorming)

| Decision | Choice |
|---|---|
| KV data path | **Same KV as the gRPC API** — routed reads via the existing `Reader`, replicated writes via the existing `Coordinator`. No raw side-channel keyspace. |
| Surface | **Reads are pure scalar functions** usable inline in any expression; **writes are `CALL` procedures**. |
| Namespace | **Explicit** `(namespace, key)` on every call — matches the KV API's namespaced addressing. No hidden default. |
| Write atomicity | **Per-op, independent.** Each `kv.put`/`kv.delete` is its own KV write (origin+1 + replication fanout). It does **not** join a graph mutation's `wavesdb.Txn`. |
| Value typing | **String** (stored as UTF-8 bytes). Missing key → Cypher `null`. |
| Bad arity / non-string args | **Hard error** (the query fails with a clear message). `null` is reserved for "key genuinely absent". |
| `kv.put` result | **Yields `version`** — the committed HLC version rendered as `version.Version.MutationID()` (a stable, injective string; `internal/version/version.go:59`). `YIELD` is optional. |

## Surface

```cypher
-- READ: pure scalar function, valid anywhere an expression is allowed
kv.get(namespace, key)        -- → string value, or null if absent / tombstoned / expired

-- WRITE: CALL procedures
CALL kv.put(namespace, key, value)              -- yields: version (string)
CALL kv.put(namespace, key, value, {ttlMs: N})  -- optional opts map (native per-key TTL)
CALL kv.delete(namespace, key)                  -- yields: version (tombstone version)
```

Examples:

```cypher
-- read inline, joined against a graph match
MATCH (u:User {id:'u1'})
RETURN u.name, kv.get('profile', u.id) AS profile;

-- filter graph rows on KV state
MATCH (u:User) WHERE kv.get('flags', u.id) = 'banned' RETURN u;

-- write, consuming the yielded version
CALL kv.put('profile', 'u1', '{"v":2}') YIELD version RETURN version;

-- write as a terminal clause (YIELD optional)
CALL kv.put('profile', 'u1', '{"v":2}');

-- delete
CALL kv.delete('profile', 'u1');
```

## Components & wiring

### a. Injected capability on the executor — `internal/cypher/planner/executor.go`

Mirrors how `VectorStore` / `VectorScatter` are injected today (`executor.go:38,46`). Add a
field and an interface. The interface is shaped for the *consumer* (the procedures); the
adapter (§b) is the translation layer to the real backend types.

```go
// KVAccess is the same KV the gRPC API exposes: routed reads + replicated writes.
type KVAccess interface {
    Get(ctx context.Context, namespace string, key []byte) (value []byte, found bool, err error)
    Put(ctx context.Context, namespace string, key, value []byte, ttlMs *int64) (version string, err error)
    Delete(ctx context.Context, namespace string, key []byte) (version string, err error)
}
```

Add `KV KVAccess` to `Executor`. When nil, `kv.*` returns `"kv.<op>: KV backend not
configured"`, exactly like the vector procs guard on `e.VectorStore == nil`.

### b. Adapter — `internal/kv/cypher_access.go` (new)

A thin struct that enforces "same KV as the API" by calling the *same* objects
`kv.Service` calls (`service.go:61,76,89`). It is the **translation layer** — the real
backend signatures differ from `KVAccess`, and that is exactly its job:

- **`Get`** → `reader.Get(ctx, namespace, key, /*hideExpired*/ true)`. Real signature is
  `func (r *Reader) Get(ctx, namespace string, key []byte, hideExpired bool) (*wavespanv1.GetResult, error)`
  (`read.go:91`). The adapter unpacks `GetResult.GetFound()` / `GetResult.GetValue()` into
  `(value, found)`. `GetResult.Found` is already false for tombstoned records (propagated
  from the store as `out.Found`) and is force-set false for expired records
  (`read.go:140-141`), so the "→ null" behavior falls out naturally. This routed read is local-first with a
  closest-holder cache fetch, so `kv.get` is correct cluster-wide.
- **`Put`** → `Coordinator.Put(ctx, namespace, key, value, ttlMs, /*idemKey*/ "")`. Real
  signature returns `(PutOutcome, error)` where `PutOutcome.Version` is a `version.Version`
  (`coordinator.go:85`, struct at `coordinator.go:76-80`). The adapter returns
  `outcome.Version.MutationID()` as the version string. (`version.Version` has **no**
  `String()` method — `MutationID()` is the chosen canonical form.)
- **`Delete`** → `Coordinator.Delete(ctx, namespace, key, /*idemKey*/ "")` →
  `outcome.Version.MutationID()` (`coordinator.go:90`).

`idemKey` is empty for now (Cypher writes are not yet idempotency-keyed; a follow-up can
derive one from the statement's mutation id).

### c. Construction & injection — `cmd/wavespan-node/main.go` + `internal/cypher/service.go`

The `Executor` is **not** built in `main.go`; it is built inside `cypher.Service.Query`
(`service.go:64`, where `VectorStore` etc. are set from service fields). So injection
follows the existing vector pattern exactly:

1. `internal/cypher/service.go`: add a builder `func (s *Service) WithKV(kv planner.KVAccess) *Service`
   mirroring `WithVector` (`service.go:39`); store it on a new `s.kv` field; set `KV: s.kv`
   in the `Executor{...}` literal at `service.go:64`.
2. `cmd/wavespan-node/main.go`: construct the adapter from `reader` + `coord` (both in scope
   around `main.go:255-261`) and pass it via `cypherSvc.WithKV(adapter)` where the cypher
   service is configured (alongside the existing `WithVector*` calls).

### d. Scalar-function registry — `internal/cypher/planner/executor.go` (new, symmetric with `RegisterProcedure`)

```go
type ScalarFunc func(e *Executor, args []*wavespanv1.Value, row bindingRow) (*wavespanv1.Value, error)
var funcs = map[string]ScalarFunc{}
func RegisterFunction(name string, fn ScalarFunc) { funcs[name] = fn }
```

### e. The ops — `internal/cypher/planner/proc_kv.go` (new), following `proc_vector.go`

```go
func init() {
    RegisterFunction("kv.get", kvGet)
    RegisterProcedure("kv.put", kvPut)
    RegisterProcedure("kv.delete", kvDelete)
}
```

- `kvGet(e, args, row)`: require exactly 2 string args (else error); guard `e.KV != nil`;
  `value, found, err := e.KV.Get(...)`; return `vStr(string(value))` when found, else
  `vNull()`; non-nil `err` is returned.
- `kvPut(e, args, yieldCols, row)`: require 3 string args + optional `{ttlMs}` map; call
  `e.KV.Put(...)`; return one `bindingRow` binding `version` → `vStr(version)`.
- `kvDelete(e, args, yieldCols, row)`: require 2 string args; call `e.KV.Delete(...)`;
  return one `bindingRow` binding `version` → `vStr(version)`.

### f. Parser / AST changes (`internal/cypher/parser/`)

**f1. New function-call expression node.** Today the AST has **no** function-call expr
(exprs are only `Variable`, `PropertyAccess`, `Literal`, `Parameter`, `BinaryExpr`,
`UnaryExpr` — `ast.go:164-169`), and `evalScalar` has no such case (`eval.go:25-44`). Add:

- `ast.go`: `type FunctionCall struct { Name string; Args []Expr }` with `isExpr()`.
- `parser.go` `primary()` (`parser.go:604-612`): after consuming a dotted name
  `ident . ident`, **peek for `(`**. If present → `FunctionCall{Name:"kv.get", Args:...}`;
  otherwise the existing `PropertyAccess` path is unchanged. One token of lookahead — the
  sole disambiguation point.

**f2. Relax the procedure allowlist.** The `CALL` parser currently hard-rejects every
non-`vector.` procedure:

```go
// parser.go:182-184
if !strings.HasPrefix(name, "vector.") {
    return nil, fmt.Errorf("cypher: procedure %s is unsupported in v1 (only vector.* procedures)", name)
}
```

This must be widened to also accept `kv.` (e.g. an allowlist of supported prefixes), or
`CALL kv.put(...)` fails at parse time and the registration in §e is unreachable. The
`CallClause`/`ProcCall` logical plumbing (`ast.go:76`, `logical.go:62-65,97-98`) is
otherwise reused unchanged.

**f3. Error propagation out of `evalScalar`.** `evalScalar` returns `*wavespanv1.Value`
with **no error** (`eval.go:25`) and has ~6 infallible call sites (project, sort, unwind,
skipLimit, procCall args). Rather than re-thread an error through all of them, the
`*FunctionCall` case calls `funcs[name]`, and on error (unknown function, bad arity,
backend failure) records the **first** error on the executor — `e.evalErr` (new field) —
and returns `vNull()`. `Execute`/`apply` checks `e.evalErr` after each operator and aborts
the query with that error. This keeps `evalScalar`'s signature stable while making
`kv.get` failures hard query errors, per the Decisions table.

## Data flow

**Read** `kv.get('profile', u.id)`:
`evalScalar(*FunctionCall)` → `funcs["kv.get"]` → `e.KV.Get(ctx, "profile", "u1")` →
adapter → `reader.Get(...)` (local memtable/SSTs, else closest-holder cache fetch) → unpack
`GetResult` → bytes → `vStr`. Pure read; no write path touched. On error, `e.evalErr` is set
and the query aborts.

**Write** `CALL kv.put('profile','u1','{"v":2}')`:
`procCall` → `kvPut` → `e.KV.Put(ctx, ...)` → adapter → `Coordinator.Put` →
`NextVersion → BuildRecord → Apply (origin local durable) → fanout/replication`
(`coordinator.go:101-142`). Same bytes a gRPC `Put` would write; immediately visible to the
KV API and replicated to holders. Committed independently of any graph mutation in the same
statement. Yields one row binding `version` = `outcome.Version.MutationID()`.

## Errors & edge cases

- **Backend not configured** (`e.KV == nil`): `"kv.get: KV backend not configured"` — mirrors vector guards.
- **Wrong arity / non-string namespace or key**: sets `e.evalErr` (read) or returns an error
  from the procedure (write) → hard query error, never silent null.
- **Unknown `kv.*` name**: `CALL` typos are rejected at parse time by the (widened)
  allowlist; an unknown scalar function name sets `e.evalErr`.
- **Missing / expired / tombstoned key**: `kv.get` → `null` (driven by `GetResult.Found=false`);
  `kv.delete` of an absent key → success (tombstone version).
- **Remote-key correctness**: reads go through the *routed* `Reader`, so `kv.get` does not
  silently miss keys whose holder is another pod. If a holder fetch is unreachable, the
  adapter surfaces it and the executor records it via the existing `MarkPartial` →
  `partial_graph_possible` / `warnings` in `QueryMeta`.
- **Bare `CALL kv.put` output shape**: with no `YIELD`/`RETURN`, a terminal `ProcCall`'s
  rows are surfaced by `toOutput` (`executor.go:345-362`) under the implicit column
  `version`. This is acceptable but is implicit, not a designed projection.
- **Guardrails**: `kv.get` is a single-key point read (no fan-out). TTL on `kv.put` reuses
  the engine's native per-key TTL via the optional `{ttlMs}` arg.

## Testing

- **Parser** (`parser_test.go`): `kv.get(a,b)` → `FunctionCall`; `a.b` still → `PropertyAccess`;
  nested args; `CALL kv.put(...)` parses (after allowlist widening); a non-allowlisted
  procedure name still rejected.
- **Executor** (new `proc_kv_test.go`): the harness is **heavier** than the vector fixture
  (`proc_vector_test.go:12-34`, which needs only a `vector.Store`). `kv.*` tests need a live
  `Coordinator` (`NewCoordinator`, `coordinator.go:48`, requires `membership.Member`,
  `Cluster`, `latencygraph.Graph`, `placement.Policy`, a `Replicator`, etc.) plus a
  `Reader`. A single-node local-only fixture with `MinAckNearbyReplicas=0`
  (`coordinator.go:122`) is achievable; factor it into a shared test helper. Cases:
  write-then-read round-trip; `kv.get` of absent key → null; `kv.delete` then `kv.get` →
  null; `kv.get` inline in `WHERE` filters graph rows; a `kv.put` is visible through
  `recordstore.Get` (proves "same KV"); TTL expiry hides the value; bad arity → error.
- **Integration** (`tests/integration/`): a key written by the gRPC KV `Put` is readable
  via `kv.get` in Cypher and vice-versa, across a 2-node cluster to exercise the routed read.

## Files touched

| File | Change |
|---|---|
| `internal/cypher/parser/ast.go` | add `FunctionCall` expr node |
| `internal/cypher/parser/parser.go` | parse dotted-name-then-`(` as `FunctionCall`; widen procedure allowlist to include `kv.` |
| `internal/cypher/planner/eval.go` | eval `*FunctionCall` via function registry; set `e.evalErr` on failure |
| `internal/cypher/planner/executor.go` | `KVAccess` interface, `KV` field, `evalErr` field + post-op check, `RegisterFunction`/`funcs`/`ScalarFunc` |
| `internal/cypher/planner/proc_kv.go` | **new** — `kv.get` / `kv.put` / `kv.delete` |
| `internal/kv/cypher_access.go` | **new** — adapter over `Reader` + `Coordinator` (`GetResult` unpack, `Version.MutationID()`) |
| `internal/cypher/service.go` | `WithKV` builder + `s.kv` field; set `KV` in the `Executor` literal |
| `cmd/wavespan-node/main.go` | construct adapter from `reader`+`coord`, pass via `cypherSvc.WithKV(...)` |
| `internal/cypher/parser/parser_test.go`, `internal/cypher/planner/proc_kv_test.go`, `tests/integration/...` | tests |
| `design/07_graph_cypher.md` | document the `kv.*` built-ins |
