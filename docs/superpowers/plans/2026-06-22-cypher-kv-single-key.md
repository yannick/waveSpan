# Cypher `kv.*` single-key read/write — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `kv.get(namespace, key)` as a Cypher scalar function and `CALL kv.put/kv.delete` as procedures, reading/writing the same namespaced, replicated KV the gRPC API exposes.

**Architecture:** A new `FunctionCall` AST node + scalar-function registry let `kv.get` be used inline in any expression; `kv.put`/`kv.delete` reuse the existing `CALL`/`ProcCall` plumbing. All three call a `planner.KVAccess` interface, implemented by an adapter (`internal/kv/cypher_access.go`) that routes reads through the existing `Reader` (local-first + closest-holder fetch) and writes through the `Coordinator` (origin+1 + replication). Injection mirrors the vector path: `cypher.Service.WithKV` → `Executor.KV`.

**Tech Stack:** Go 1.26, the in-tree `wavesdb` LSM engine, Connect RPC, the existing `internal/cypher` parser/planner.

**Spec:** `docs/superpowers/specs/2026-06-22-cypher-kv-single-key-access-design.md`

---

## Security & robustness requirements (apply throughout)

- **Strict argument validation.** Every `kv.*` op validates arity and that namespace/key/value args are *strings* (and `ttlMs` an int). A bad call is a hard query error — never a silent null, never a panic on a type assertion. `null` is reserved for "key genuinely absent."
- **No trust-boundary bypass.** Reads/writes go through the *same* `Reader`/`Coordinator` the gRPC API uses, so any namespace placement policy, value-size limits, and durability the KV layer enforces apply unchanged. `kv.*` adds no new privileged path.
- **Bounded resource use.** `kv.get` is a single-key point read; per-row fan-out (one read per result row) is bounded by the existing `maxRowsReturned` / `maxIntermediateRows` guardrails. All KV calls pass the request context (`e.Ctx`) so query timeout / client cancellation propagates and a slow/unreachable holder cannot hang the query.
- **No secret leakage in errors.** Error messages include namespace and key (not value); values are never echoed into error strings or logs.

## Test strategy

- **Parser tests** (`parser_test.go`): pure, no backend.
- **Planner tests** (`proc_kv_test.go`): drive whole queries through `Executor.Execute` with a **fake `KVAccess`** (in-package, deterministic). This is the right seam — fast, focused, exercises arg-validation and inline use without standing up a coordinator.
- **Adapter test** (`internal/kv/cypher_access_test.go`): real `Reader`+`Coordinator` over a mem store, reusing the existing single-node helpers in `coordinator_test.go`. Proves the translation layer (`GetResult` unpack, `MutationID()` versioning, same-store coherence).
- **Integration test** (`tests/integration/`): 2-node cluster, gRPC `Put` ↔ Cypher `kv.get` round-trip both directions.

---

## Task 1: `FunctionCall` AST node + parser support

**Files:**
- Modify: `internal/cypher/parser/ast.go`
- Modify: `internal/cypher/parser/parser.go` (`primary()`, ~line 604; add `callArgs` helper)
- Test: `internal/cypher/parser/parser_test.go`

- [ ] **Step 1: Write failing tests**

In `parser_test.go`:

```go
func TestParseFunctionCall(t *testing.T) {
	q, err := Parse("RETURN kv.get('users', 'u1')")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ret := q.Clauses[len(q.Clauses)-1].(*ReturnClause)
	fc, ok := ret.Items[0].Expr.(*FunctionCall)
	if !ok {
		t.Fatalf("expected *FunctionCall, got %T", ret.Items[0].Expr)
	}
	if fc.Name != "kv.get" || len(fc.Args) != 2 {
		t.Fatalf("got name=%q args=%d", fc.Name, len(fc.Args))
	}
}

func TestParsePropertyAccessStillWorks(t *testing.T) {
	q, err := Parse("MATCH (n) RETURN n.name")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ret := q.Clauses[len(q.Clauses)-1].(*ReturnClause)
	if _, ok := ret.Items[0].Expr.(*PropertyAccess); !ok {
		t.Fatalf("expected *PropertyAccess, got %T", ret.Items[0].Expr)
	}
}
```

> Note: confirm the `ReturnClause`/return-item field names against `ast.go` before running (the projection item type may be e.g. `ReturnItem{Expr, Alias}`). Adjust the test accessors to match — do not change the AST.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cypher/parser/ -run 'TestParseFunctionCall|TestParsePropertyAccessStillWorks' -v`
Expected: FAIL — `FunctionCall` undefined.

- [ ] **Step 3: Add the AST node** in `ast.go` (next to the other `isExpr` types, ~line 164):

```go
// FunctionCall is a scalar function invocation in an expression, e.g. kv.get('ns','k').
type FunctionCall struct {
	Name string
	Args []Expr
}

func (*FunctionCall) isExpr() {}
```

- [ ] **Step 4: Parse it** in `parser.go`. Replace the `TokIdent` dotted branch in `primary()`:

```go
	case TokIdent:
		p.next()
		if p.acceptPunct(".") {
			if p.peek().Type != TokIdent {
				return nil, fmt.Errorf("cypher: expected property name after '.'")
			}
			name := p.next().Val
			// A dotted name followed by '(' is a function call (kv.get(...)); otherwise property access.
			if p.isPunct("(") {
				args, err := p.callArgs()
				if err != nil {
					return nil, err
				}
				return &FunctionCall{Name: t.Val + "." + name, Args: args}, nil
			}
			return &PropertyAccess{Variable: t.Val, Property: name}, nil
		}
		return &Variable{Name: t.Val}, nil
```

Add the helper (near the other parse helpers):

```go
// callArgs parses a parenthesized, comma-separated expression list: '(' [expr {',' expr}] ')'.
func (p *parser) callArgs() ([]Expr, error) {
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	var args []Expr
	if !p.isPunct(")") {
		for {
			a, err := p.expr()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if !p.acceptPunct(",") {
				break
			}
		}
	}
	return args, p.expectPunct(")")
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/cypher/parser/ -v`
Expected: PASS (all existing parser tests still green).

- [ ] **Step 6: Commit**

```bash
git add internal/cypher/parser/ast.go internal/cypher/parser/parser.go internal/cypher/parser/parser_test.go
git commit -m "feat(cypher): parse FunctionCall expressions (kv.get(...))"
```

---

## Task 2: Allow `kv.` procedures in the `CALL` parser

**Files:**
- Modify: `internal/cypher/parser/parser.go:182-184` (the `vector.`-only guard)
- Test: `internal/cypher/parser/parser_test.go`

- [ ] **Step 1: Write failing tests**

```go
func TestParseCallKVPut(t *testing.T) {
	if _, err := Parse("CALL kv.put('users','u1','v')"); err != nil {
		t.Fatalf("CALL kv.put should parse: %v", err)
	}
}

func TestParseCallUnknownNamespaceRejected(t *testing.T) {
	if _, err := Parse("CALL foo.bar('x')"); err == nil {
		t.Fatal("CALL foo.bar should be rejected")
	}
}
```

- [ ] **Step 2: Run to verify the first fails**

Run: `go test ./internal/cypher/parser/ -run TestParseCall -v`
Expected: `TestParseCallKVPut` FAILS ("only vector.* procedures"); `TestParseCallUnknownNamespaceRejected` already passes.

- [ ] **Step 3: Widen the allowlist** at `parser.go:182`:

```go
	if !strings.HasPrefix(name, "vector.") && !strings.HasPrefix(name, "kv.") {
		return nil, fmt.Errorf("cypher: procedure %s is unsupported in v1 (only vector.* and kv.* procedures)", name)
	}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/cypher/parser/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cypher/parser/parser.go internal/cypher/parser/parser_test.go
git commit -m "feat(cypher): allow kv.* procedures in CALL parser"
```

---

## Task 3: `KVAccess` interface, scalar-function registry, error propagation, `FunctionCall` eval

**Files:**
- Modify: `internal/cypher/planner/executor.go` (Executor struct, registry, `evalErr`, Execute loop)
- Modify: `internal/cypher/planner/eval.go` (`evalScalar` case + `evalFunc`; add `fmt` import)
- Test: `internal/cypher/planner/proc_kv_test.go` (registry/eval-only cases; KV ops land in Task 4)

- [ ] **Step 1: Write a failing test** (`proc_kv_test.go`) for the registry + error propagation, using a throwaway registered function:

```go
package planner

import (
	"testing"

	"github.com/cwire/wavespan/internal/cypher/parser"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func init() {
	RegisterFunction("test.echo", func(_ *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
		return args[0], nil
	})
}

func runQuery(t *testing.T, e *Executor, q string) *Result {
	t.Helper()
	ast, err := parser.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	res, err := e.Execute(ast)
	if err != nil {
		t.Fatalf("execute %q: %v", q, err)
	}
	return res
}

func TestScalarFunctionEvaluated(t *testing.T) {
	e := &Executor{}
	res := runQuery(t, e, "RETURN test.echo('hi') AS v")
	if got := res.Rows[0]["v"].GetStringValue(); got != "hi" {
		t.Fatalf("got %q", got)
	}
}

func TestUnknownFunctionIsHardError(t *testing.T) {
	e := &Executor{}
	ast, _ := parser.Parse("RETURN nope.fn() AS v")
	if _, err := e.Execute(ast); err == nil {
		t.Fatal("unknown function must be a hard error")
	}
}
```

> Confirm the `RETURN ... AS v` alias syntax parses and that the output column is `v` (check `ast.go`/`project`). If aliasing isn't supported, return the bare expression and read the implicit column.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cypher/planner/ -run 'TestScalarFunction|TestUnknownFunction' -v`
Expected: FAIL — `RegisterFunction` / `Executor.evalErr` undefined.

- [ ] **Step 3: Add the interface, registry, fields** in `executor.go`. Near the `Procedure` type (~line 62):

```go
// KVAccess is the KV read/write surface the cypher kv.* built-ins use. It is satisfied by
// internal/kv.CypherKV, which routes to the same Reader/Coordinator the gRPC KV API uses.
type KVAccess interface {
	Get(ctx context.Context, namespace string, key []byte) (value []byte, found bool, err error)
	Put(ctx context.Context, namespace string, key, value []byte, ttlMs *int64) (version string, err error)
	Delete(ctx context.Context, namespace string, key []byte) (version string, err error)
}

// ScalarFunc is a CALL-free function usable inline in expressions, e.g. kv.get(ns, key).
type ScalarFunc func(e *Executor, args []*wavespanv1.Value, row bindingRow) (*wavespanv1.Value, error)

var funcs = map[string]ScalarFunc{}

// RegisterFunction registers a scalar function by name (e.g. "kv.get").
func RegisterFunction(name string, fn ScalarFunc) { funcs[name] = fn }
```

Add fields to the `Executor` struct:

```go
	// KV, when set, backs the kv.* built-ins. Nil ⇒ kv.* returns "backend not configured".
	KV KVAccess
	// evalErr captures the first error raised inside expression evaluation (evalScalar cannot
	// return an error); Execute aborts the query with it after the current operator.
	evalErr error
```

In `Execute`, check `evalErr` inside the op loop:

```go
	for _, op := range ops {
		if rows, err = e.apply(op, rows); err != nil {
			return nil, err
		}
		if e.evalErr != nil {
			return nil, e.evalErr
		}
		if err := e.Limits.checkIntermediate(len(rows)); err != nil {
			return nil, err
		}
	}
```

- [ ] **Step 4: Evaluate `FunctionCall`** in `eval.go`. Add `"fmt"` to the import block, add a case in `evalScalar`'s switch:

```go
	case *parser.FunctionCall:
		return e.evalFunc(x, row)
```

Add the helpers:

```go
// evalFunc evaluates a scalar function call. Failures (unknown name, bad args, backend error)
// are recorded on e.evalErr (first wins) and surface as a hard query error after the current
// operator; the expression itself yields null meanwhile.
func (e *Executor) evalFunc(x *parser.FunctionCall, row bindingRow) *wavespanv1.Value {
	fn, ok := funcs[x.Name]
	if !ok {
		e.setEvalErr(fmt.Errorf("cypher: unknown function %s", x.Name))
		return vNull()
	}
	args := make([]*wavespanv1.Value, len(x.Args))
	for i, a := range x.Args {
		args[i] = e.evalScalar(a, row)
	}
	v, err := fn(e, args, row)
	if err != nil {
		e.setEvalErr(err)
		return vNull()
	}
	return v
}

func (e *Executor) setEvalErr(err error) {
	if e.evalErr == nil {
		e.evalErr = err
	}
}
```

- [ ] **Step 5: Run to verify pass**

Run: `go test ./internal/cypher/planner/ -run 'TestScalarFunction|TestUnknownFunction' -v`
Expected: PASS. Then `go build ./...` to confirm nothing else broke.

- [ ] **Step 6: Commit**

```bash
git add internal/cypher/planner/executor.go internal/cypher/planner/eval.go internal/cypher/planner/proc_kv_test.go
git commit -m "feat(cypher): scalar-function registry, KVAccess interface, eval error propagation"
```

---

## Task 4: `kv.get` / `kv.put` / `kv.delete` built-ins

**Files:**
- Create: `internal/cypher/planner/proc_kv.go`
- Test: `internal/cypher/planner/proc_kv_test.go` (extend)

- [ ] **Step 1: Write failing tests** — add a fake backend + behavior tests to `proc_kv_test.go`:

```go
import "context"

type fakeKV struct {
	data    map[string]string
	ver     int
	getErr  error
}

func nsKey(ns string, key []byte) string { return ns + "\x00" + string(key) }

func (f *fakeKV) Get(_ context.Context, ns string, key []byte) ([]byte, bool, error) {
	if f.getErr != nil {
		return nil, false, f.getErr
	}
	v, ok := f.data[nsKey(ns, key)]
	if !ok {
		return nil, false, nil
	}
	return []byte(v), true, nil
}
func (f *fakeKV) Put(_ context.Context, ns string, key, value []byte, _ *int64) (string, error) {
	if f.data == nil {
		f.data = map[string]string{}
	}
	f.data[nsKey(ns, key)] = string(value)
	f.ver++
	return "v" + string(rune('0'+f.ver)), nil
}
func (f *fakeKV) Delete(_ context.Context, ns string, key []byte) (string, error) {
	delete(f.data, nsKey(ns, key))
	f.ver++
	return "v" + string(rune('0'+f.ver)), nil
}

func TestKVPutThenGet(t *testing.T) {
	kv := &fakeKV{data: map[string]string{}}
	e := &Executor{KV: kv}
	runQuery(t, e, "CALL kv.put('profile','u1','hello')")
	res := runQuery(t, &Executor{KV: kv}, "RETURN kv.get('profile','u1') AS v")
	if got := res.Rows[0]["v"].GetStringValue(); got != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestKVGetMissingIsNull(t *testing.T) {
	res := runQuery(t, &Executor{KV: &fakeKV{data: map[string]string{}}}, "RETURN kv.get('x','nope') AS v")
	if _, isNull := res.Rows[0]["v"].GetValue().(*wavespanv1.Value_Null); !isNull {
		t.Fatalf("expected null, got %v", res.Rows[0]["v"])
	}
}

func TestKVPutYieldsVersion(t *testing.T) {
	res := runQuery(t, &Executor{KV: &fakeKV{data: map[string]string{}}}, "CALL kv.put('p','k','v') YIELD version RETURN version")
	if res.Rows[0]["version"].GetStringValue() == "" {
		t.Fatal("expected a non-empty version")
	}
}

func TestKVGetBadArityErrors(t *testing.T) {
	ast, _ := parser.Parse("RETURN kv.get('only-one') AS v")
	if _, err := (&Executor{KV: &fakeKV{}}).Execute(ast); err == nil {
		t.Fatal("kv.get with 1 arg must error")
	}
}

func TestKVGetNonStringArgErrors(t *testing.T) {
	ast, _ := parser.Parse("RETURN kv.get('ns', 42) AS v")
	if _, err := (&Executor{KV: &fakeKV{}}).Execute(ast); err == nil {
		t.Fatal("kv.get with non-string key must error")
	}
}

func TestKVNoBackendErrors(t *testing.T) {
	ast, _ := parser.Parse("RETURN kv.get('ns','k') AS v")
	if _, err := (&Executor{}).Execute(ast); err == nil {
		t.Fatal("kv.get without backend must error")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cypher/planner/ -run TestKV -v`
Expected: FAIL — `kv.get`/`kv.put`/`kv.delete` not registered.

- [ ] **Step 3: Implement** `proc_kv.go`:

```go
package planner

import (
	"context"
	"fmt"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func init() {
	RegisterFunction("kv.get", kvGet)
	RegisterProcedure("kv.put", kvPut)
	RegisterProcedure("kv.delete", kvDelete)
}

func (e *Executor) kvCtx() context.Context {
	if e.Ctx != nil {
		return e.Ctx
	}
	return context.Background()
}

// stringArg returns the string payload of args[i], or an error if missing or not a string.
func stringArg(fn string, args []*wavespanv1.Value, i int) (string, error) {
	if i >= len(args) {
		return "", fmt.Errorf("cypher: %s: missing argument %d", fn, i+1)
	}
	s, ok := args[i].GetValue().(*wavespanv1.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("cypher: %s: argument %d must be a string", fn, i+1)
	}
	return s.StringValue, nil
}

// kvGet implements kv.get(namespace, key) -> string|null.
func kvGet(e *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("cypher: kv.get(namespace, key) requires 2 arguments")
	}
	if e.KV == nil {
		return nil, fmt.Errorf("cypher: kv.get: KV backend not configured")
	}
	ns, err := stringArg("kv.get", args, 0)
	if err != nil {
		return nil, err
	}
	key, err := stringArg("kv.get", args, 1)
	if err != nil {
		return nil, err
	}
	val, found, err := e.KV.Get(e.kvCtx(), ns, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("cypher: kv.get(%q, %q): %w", ns, key, err)
	}
	if !found {
		return vNull(), nil
	}
	return vStr(string(val)), nil
}

// kvPut implements CALL kv.put(namespace, key, value [, {ttlMs}]) YIELD version.
func kvPut(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	if len(args) < 3 || len(args) > 4 {
		return nil, fmt.Errorf("cypher: kv.put(namespace, key, value [, opts]) requires 3 or 4 arguments")
	}
	if e.KV == nil {
		return nil, fmt.Errorf("cypher: kv.put: KV backend not configured")
	}
	ns, err := stringArg("kv.put", args, 0)
	if err != nil {
		return nil, err
	}
	key, err := stringArg("kv.put", args, 1)
	if err != nil {
		return nil, err
	}
	value, err := stringArg("kv.put", args, 2)
	if err != nil {
		return nil, err
	}
	ttlMs, err := kvPutTTL(args)
	if err != nil {
		return nil, err
	}
	ver, err := e.KV.Put(e.kvCtx(), ns, []byte(key), []byte(value), ttlMs)
	if err != nil {
		return nil, fmt.Errorf("cypher: kv.put(%q, %q): %w", ns, key, err)
	}
	nr := cloneRow(row)
	nr["version"] = vStr(ver)
	return []bindingRow{nr}, nil
}

// kvPutTTL reads the optional 4th map arg {ttlMs: int}; returns nil when absent.
func kvPutTTL(args []*wavespanv1.Value) (*int64, error) {
	if len(args) < 4 {
		return nil, nil
	}
	m := args[3].GetMapValue()
	if m == nil {
		return nil, fmt.Errorf("cypher: kv.put: 4th argument must be a map like {ttlMs: 1000}")
	}
	ent, ok := m.GetEntries()["ttlMs"]
	if !ok {
		return nil, nil
	}
	iv, ok := ent.GetValue().(*wavespanv1.Value_IntValue)
	if !ok {
		return nil, fmt.Errorf("cypher: kv.put: ttlMs must be an integer")
	}
	ms := iv.IntValue
	return &ms, nil
}

// kvDelete implements CALL kv.delete(namespace, key) YIELD version.
func kvDelete(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("cypher: kv.delete(namespace, key) requires 2 arguments")
	}
	if e.KV == nil {
		return nil, fmt.Errorf("cypher: kv.delete: KV backend not configured")
	}
	ns, err := stringArg("kv.delete", args, 0)
	if err != nil {
		return nil, err
	}
	key, err := stringArg("kv.delete", args, 1)
	if err != nil {
		return nil, err
	}
	ver, err := e.KV.Delete(e.kvCtx(), ns, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("cypher: kv.delete(%q, %q): %w", ns, key, err)
	}
	nr := cloneRow(row)
	nr["version"] = vStr(ver)
	return []bindingRow{nr}, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/cypher/planner/ -run TestKV -v`
Expected: PASS.

- [ ] **Step 5: Add an inline-use test** (kv.get inside WHERE, graph-backed) using the existing planner fixture. Inspect `fixture_test.go` for the helper that builds an `Executor` with a populated graph `Store` (e.g. `newFixture(t)`); set `.KV = &fakeKV{...}` on it and assert:

```go
func TestKVGetInWhereFiltersRows(t *testing.T) {
	// ... build graph fixture with two :User nodes u1,u2 ...
	// kv 'flags'/'u1' = "banned"
	// MATCH (u:User) WHERE kv.get('flags', u.id) = 'banned' RETURN u.id
	// assert exactly one row, u1
}
```

> Use the fixture's real constructor; do not hand-roll graph records. If the fixture API differs, adapt the test to it.

Run: `go test ./internal/cypher/planner/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/cypher/planner/proc_kv.go internal/cypher/planner/proc_kv_test.go
git commit -m "feat(cypher): kv.get/kv.put/kv.delete built-ins with strict arg validation"
```

---

## Task 5: KV adapter (`Reader` + `Coordinator` → `KVAccess`)

**Files:**
- Create: `internal/kv/cypher_access.go`
- Test: `internal/kv/cypher_access_test.go`

- [ ] **Step 1: Write a failing test** reusing the single-node helpers in `coordinator_test.go` (`member`, `cluster`, `defaultPolicy`, the `n1` node helper, a `repl`). Inspect that file first for exact helper names/shapes.

```go
package kv

import (
	"context"
	"testing"
)

func TestCypherKVRoundTrip(t *testing.T) {
	// Build a single-node coordinator + reader over one store (mirror coordinator_test.go setup).
	n1 := /* node fixture from coordinator_test.go */
	coord := NewCoordinator(n1.store, member("node1"), cluster, latencygraphNew(), repl, defaultPolicy(), local.NewIdempotency(0), nil, nil, time.Second)
	reader := NewReader(n1.store, member("node1"))
	kv := NewCypherKV(reader, coord)

	ctx := context.Background()
	ver, err := kv.Put(ctx, "profile", []byte("u1"), []byte("hello"), nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if ver == "" {
		t.Fatal("expected non-empty version")
	}
	got, found, err := kv.Get(ctx, "profile", []byte("u1"))
	if err != nil || !found || string(got) != "hello" {
		t.Fatalf("get: got=%q found=%v err=%v", got, found, err)
	}

	if _, _, _ = kv.Get(ctx, "profile", []byte("absent")); false {
	}
	_, found, _ = kv.Get(ctx, "profile", []byte("absent"))
	if found {
		t.Fatal("absent key should not be found")
	}

	if _, err := kv.Delete(ctx, "profile", []byte("u1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, found, _ := kv.Get(ctx, "profile", []byte("u1")); found {
		t.Fatal("deleted key should not be found")
	}
}
```

> The exact fixture wiring (`defaultPolicy` may need `MinAckNearbyReplicas: 0` for single-node ack) must match `coordinator_test.go`. Copy its working setup rather than guessing.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/kv/ -run TestCypherKVRoundTrip -v`
Expected: FAIL — `NewCypherKV` undefined.

- [ ] **Step 3: Implement** `cypher_access.go`:

```go
// Package kv: CypherKV adapts the KV Reader+Coordinator to the cypher planner.KVAccess interface,
// so Cypher kv.* built-ins read and write the exact same namespaced, replicated KV as the gRPC API.
package kv

import "context"

// CypherKV satisfies planner.KVAccess (structural — this package does not import the planner).
type CypherKV struct {
	reader *Reader
	coord  *Coordinator
}

// NewCypherKV wires the adapter from the same Reader and Coordinator the gRPC KV Service uses.
func NewCypherKV(reader *Reader, coord *Coordinator) *CypherKV {
	return &CypherKV{reader: reader, coord: coord}
}

// Get routes through the Reader: local-first with a closest-holder cache fetch on a miss.
// hideExpired=true so expired/tombstoned records read as absent (→ Cypher null).
func (k *CypherKV) Get(ctx context.Context, namespace string, key []byte) ([]byte, bool, error) {
	res, err := k.reader.Get(ctx, namespace, key, true)
	if err != nil {
		return nil, false, err
	}
	if !res.GetFound() {
		return nil, false, nil
	}
	return res.GetValue(), true, nil
}

// Put routes through the Coordinator (origin+1 durable + replication fanout). The returned version
// is the committed HLC version's stable MutationID string.
func (k *CypherKV) Put(ctx context.Context, namespace string, key, value []byte, ttlMs *int64) (string, error) {
	out, err := k.coord.Put(ctx, namespace, key, value, ttlMs, "")
	if err != nil {
		return "", err
	}
	return out.Version.MutationID(), nil
}

// Delete tombstones the key through the Coordinator and returns the tombstone version's MutationID.
func (k *CypherKV) Delete(ctx context.Context, namespace string, key []byte) (string, error) {
	out, err := k.coord.Delete(ctx, namespace, key, "")
	if err != nil {
		return "", err
	}
	return out.Version.MutationID(), nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./internal/kv/ -run TestCypherKVRoundTrip -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/kv/cypher_access.go internal/kv/cypher_access_test.go
git commit -m "feat(kv): CypherKV adapter over Reader+Coordinator for cypher kv.*"
```

---

## Task 6: Wire the adapter into the Cypher service and node

**Files:**
- Modify: `internal/cypher/service.go` (`Service` struct, `WithKV`, `Executor` literal at ~line 64)
- Modify: `cmd/wavespan-node/main.go` (construct adapter; add `.WithKV(...)` to the cypher service builder)
- Test: `go build ./...` + existing service tests

- [ ] **Step 1: Add `WithKV` + field** to `service.go`:

In the `Service` struct, add:

```go
	kv planner.KVAccess
```

Add the builder (next to `WithVector`):

```go
// WithKV enables the kv.* built-ins (kv.get / CALL kv.put / CALL kv.delete) over the same KV the
// gRPC API exposes.
func (s *Service) WithKV(kv planner.KVAccess) *Service {
	s.kv = kv
	return s
}
```

Set it on the executor literal in `Query` (~line 64):

```go
		Ctx: ctx, VectorScatter: s.vectorScatter,
		KV: s.kv,
```

- [ ] **Step 2: Construct + inject in `main.go`.** After `reader` is built (`main.go:261`) and where `cypherSvc` is configured with `.WithVector(...)`, build and pass the adapter:

```go
	cypherKV := kv.NewCypherKV(reader, coord)
	// ... cypherSvc := cypher.NewService(...).WithVector(...).WithVectorScatter(...).WithKV(cypherKV)
```

> Find the existing `cypher.NewService(...)` chain in main.go and append `.WithKV(cypherKV)`. `reader` and `coord` are both in scope there.

- [ ] **Step 3: Build + run the full suite**

Run: `go build ./... && go test ./internal/cypher/... ./internal/kv/...`
Expected: PASS, clean build.

- [ ] **Step 4: Commit**

```bash
git add internal/cypher/service.go cmd/wavespan-node/main.go
git commit -m "feat(cypher): wire CypherKV into Service.WithKV and the node"
```

---

## Task 7: Integration test — KV API ↔ Cypher coherence

**Files:**
- Create/extend: `tests/integration/cypher_kv_test.go`

- [ ] **Step 1: Study an existing 2-node integration test** (e.g. `tests/integration/membership_test.go`, `vector_distributed_test.go`) for the cluster-bootstrap + Connect-client helpers. Reuse them.

- [ ] **Step 2: Write the test** asserting both directions:
  - gRPC KV `Put(namespace="profile", key="u1", value="hello")` → Cypher `RETURN kv.get('profile','u1')` returns `"hello"`.
  - Cypher `CALL kv.put('profile','u2','world')` → gRPC KV `Get(namespace="profile", key="u2")` returns `"world"`.
  - Run across 2 nodes so the routed `Reader` path (closest-holder fetch) is exercised; allow for eventual-consistency settling (poll/retry with a bounded deadline, as the other distributed tests do).

- [ ] **Step 3: Run**

Run: `go test ./tests/integration/ -run Cypher_KV -v` (match the actual test name)
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/cypher_kv_test.go
git commit -m "test(integration): KV API <-> Cypher kv.* coherence across 2 nodes"
```

---

## Task 8: Document the `kv.*` built-ins

**Files:**
- Modify: `design/07_graph_cypher.md`

- [ ] **Step 1: Add a "KV built-ins" subsection** documenting `kv.get(namespace, key)` (scalar, → string|null), `CALL kv.put(namespace, key, value [, {ttlMs}]) YIELD version`, `CALL kv.delete(namespace, key) YIELD version`; note they hit the same KV as the gRPC API, are per-op (not atomic with graph mutations), take explicit namespaces, and hard-error on bad args.

- [ ] **Step 2: Commit**

```bash
git add design/07_graph_cypher.md
git commit -m "docs: document cypher kv.* built-ins in design/07"
```

---

## Final verification

- [ ] `go build ./...` clean.
- [ ] `go test ./internal/cypher/... ./internal/kv/... ./tests/integration/... ` green.
- [ ] `go vet ./...` and `gofmt -l` clean on touched files.
- [ ] Manual smoke (optional): start a node, run `RETURN kv.get('x','y')`, `CALL kv.put('x','y','z')`, confirm the KV API sees `z`.
