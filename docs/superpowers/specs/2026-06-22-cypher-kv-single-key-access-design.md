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
| KV data path | **Same KV as the gRPC API** — routed reads via the existing `reader`, replicated writes via the existing `Coordinator`. No raw side-channel keyspace. |
| Surface | **Reads are pure scalar functions** usable inline in any expression; **writes are `CALL` procedures**. |
| Namespace | **Explicit** `(namespace, key)` on every call — matches the KV API's namespaced addressing. No hidden default. |
| Write atomicity | **Per-op, independent.** Each `kv.put`/`kv.delete` is its own KV write (origin+1 + replication fanout). It does **not** join a graph mutation's `wavesdb.Txn`. |
| Value typing | **String** (stored as UTF-8 bytes). Missing key → Cypher `null`. |
| Bad arity / non-string args | **Hard error** (the query fails with a clear message). `null` is reserved for "key genuinely absent". |
| `kv.put` result | **Yields `version`** (the committed HLC version, as a string). `YIELD` is optional for callers who don't need it. |

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

Mirrors how `VectorStore` / `VectorScatter` are injected today. Add a field and an interface:

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
`kv.Service` calls:

- `Get` → the existing `reader.Get(ctx, namespace, key, hideExpired)` (local-first read
  with a closest-holder cache fetch on a miss — `service.go:88`). This is what makes
  `kv.get` correct cluster-wide instead of silently missing keys whose holder is another
  pod.
- `Put` → `Coordinator.Put(ctx, namespace, key, value, ttlMs, idemKey)` →
  `NextVersion → BuildRecord → Apply (origin local durable) → fanout` (`coordinator.go:85`).
- `Delete` → `Coordinator.Delete(ctx, namespace, key, idemKey)` (`coordinator.go:90`).

`idemKey` is empty for now (Cypher writes are not yet idempotency-keyed; a follow-up can
derive one from the statement's mutation id).

### c. Construction — `cmd/wavespan-node/main.go` (~line 255)

Where `coord := kv.NewCoordinator(...)` and the executor factory are built: construct the
adapter from `reader` + `coord`, and set it on every executor the node creates
(`exec.KV = adapter`).

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

- `kvGet(e, args, row)`: require exactly 2 string args (else hard error); guard `e.KV != nil`;
  `value, found, err := e.KV.Get(...)`; return `vStr(string(value))` when found, else
  `vNull()`. `err` propagates as a query error.
- `kvPut(e, args, yieldCols, row)`: require 3 string args + optional `{ttlMs}` map; call
  `e.KV.Put(...)`; return one `bindingRow` binding `version` → `vStr(version)`.
- `kvDelete(e, args, yieldCols, row)`: require 2 string args; call `e.KV.Delete(...)`;
  return one `bindingRow` binding `version` → `vStr(version)`.

### f. Parser / AST — the one genuinely new language construct (`internal/cypher/parser/`)

Today the AST has **no function-call expression node** (exprs are only `Variable`,
`PropertyAccess`, `Literal`, `Parameter`, `BinaryExpr`, `UnaryExpr` — `ast.go:164-169`).
Add:

- `ast.go`: `type FunctionCall struct { Name string; Args []Expr }` with `isExpr()`.
- `parser.go` primary-expression parsing: a dotted name (`kv` `.` `get`) **followed by
  `(`** parses as `FunctionCall{Name:"kv.get", Args:...}`. Without a following `(` the same
  token sequence stays a `PropertyAccess` (`variable.property`). This is **one token of
  lookahead** after the dotted name — the sole disambiguation point.
- `eval.go`: add `case *parser.FunctionCall:` in `evalScalar` → look up `funcs[x.Name]`,
  evaluate each arg via `evalScalar`, call the function. Unknown function name → **hard
  error** (`"unknown function <name>"`), not a silent null.

`CALL kv.put` / `CALL kv.delete` need **no** parser change — `CallClause` (`parser.go`) and
`ProcCall` (`logical.go:62`) already exist; only the `init()` registration is new.

## Data flow

**Read** `kv.get('profile', u.id)`:
`evalScalar(*FunctionCall)` → `funcs["kv.get"]` → `e.KV.Get(ctx, "profile", "u1")` →
adapter → `reader.Get` (local memtable/SSTs, else closest-holder cache fetch) → decode
`StoredRecord`, drop tombstoned/expired → bytes → `vStr`. Pure read; no write path touched.

**Write** `CALL kv.put('profile','u1','{"v":2}')`:
`procCall` → `kvPut` → `e.KV.Put(ctx, ...)` → adapter → `Coordinator.Put` →
`NextVersion → BuildRecord → Apply → fanout/replication`. Same bytes a gRPC `Put` would
write; immediately visible to the KV API and replicated to holders. Committed
independently of any graph `CREATE`/`SET`/`DELETE` in the same statement.

## Errors & edge cases

- **Backend not configured** (`e.KV == nil`): `"kv.get: KV backend not configured"` — mirrors vector guards.
- **Wrong arity / non-string namespace or key**: hard query error with a clear message (never silent null).
- **Unknown `kv.*` name** (typo `kv.gett`): hard parse/plan error.
- **Missing / expired / tombstoned key**: `kv.get` → `null`; `kv.delete` of an absent key → success (tombstone version).
- **Remote-key correctness**: reads go through the *routed* `reader`, so `kv.get` does not
  silently miss keys whose holder is another pod. If a holder fetch is unreachable, the
  adapter surfaces it and the executor records it via the existing `MarkPartial` →
  `partial_graph_possible` / `warnings` in `QueryMeta`.
- **Guardrails**: `kv.get` is a single-key point read (no fan-out). TTL on `kv.put` reuses
  the engine's native per-key TTL via the optional `{ttlMs}` arg.

## Testing

- **Parser** (`parser_test.go`): `kv.get(a,b)` → `FunctionCall`; `a.b` still → `PropertyAccess`;
  nested args; `CALL kv.put(...)` parses; `kv.gett(...)` rejected.
- **Executor** (new `proc_kv_test.go`, fixture-backed like `proc_vector_test.go`):
  write-then-read round-trip; `kv.get` of absent key → null; `kv.delete` then `kv.get` →
  null; `kv.get` inline in `WHERE` filters graph rows; a `kv.put` is visible through
  `recordstore.Get` (proves "same KV"); TTL expiry hides the value; bad arity → error.
- **Integration** (`tests/integration/`): a key written by the gRPC KV `Put` is readable
  via `kv.get` in Cypher and vice-versa, across a 2-node cluster to exercise the routed read.

## Files touched

| File | Change |
|---|---|
| `internal/cypher/parser/ast.go` | add `FunctionCall` expr node |
| `internal/cypher/parser/parser.go` | parse dotted-name-then-`(` as `FunctionCall` |
| `internal/cypher/planner/eval.go` | eval `*FunctionCall` via function registry |
| `internal/cypher/planner/executor.go` | `KVAccess` interface, `KV` field, `RegisterFunction`/`funcs` |
| `internal/cypher/planner/proc_kv.go` | **new** — `kv.get` / `kv.put` / `kv.delete` |
| `internal/kv/cypher_access.go` | **new** — adapter over `reader` + `Coordinator` |
| `cmd/wavespan-node/main.go` | construct adapter, set `exec.KV` |
| `internal/cypher/parser/parser_test.go`, `internal/cypher/planner/proc_kv_test.go`, `tests/integration/...` | tests |
| `design/07_graph_cypher.md` | document the `kv.*` built-ins |
