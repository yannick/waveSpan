package planner

import (
	"context"
	"fmt"
	"testing"

	"github.com/yannick/wavespan/internal/cypher/parser"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
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

type fakeKV struct {
	data    map[string]string
	ver     int
	getErr  error
	partial bool // when true, reads report a possibly-incomplete result (unreachable holder)
}

func nsKey(ns string, key []byte) string { return ns + "\x00" + string(key) }

func (f *fakeKV) Get(_ context.Context, ns string, key []byte) ([]byte, bool, bool, error) {
	if f.getErr != nil {
		return nil, false, f.partial, f.getErr
	}
	v, ok := f.data[nsKey(ns, key)]
	if !ok {
		return nil, false, f.partial, nil
	}
	return []byte(v), true, f.partial, nil
}
func (f *fakeKV) Put(_ context.Context, ns string, key, value []byte, _ *int64) (string, error) {
	if f.data == nil {
		f.data = map[string]string{}
	}
	f.data[nsKey(ns, key)] = string(value)
	f.ver++
	return fmt.Sprintf("v%d", f.ver), nil
}
func (f *fakeKV) Delete(_ context.Context, ns string, key []byte) (string, error) {
	delete(f.data, nsKey(ns, key))
	f.ver++
	return fmt.Sprintf("v%d", f.ver), nil
}

func TestKVPutThenGet(t *testing.T) {
	kv := &fakeKV{data: map[string]string{}}
	runQuery(t, &Executor{KV: kv}, "CALL kv.put('profile','u1','hello')")
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
	if res.Meta.GetPartialGraphPossible() {
		t.Fatal("a complete miss must not be marked partial")
	}
}

// When the KV read is incomplete (e.g. a holder was unreachable), a null kv.get result must surface
// as QueryMeta.partial_graph_possible with a warning — not a silent definite absence.
func TestKVGetPartialReadMarksQueryPartial(t *testing.T) {
	res := runQuery(t, &Executor{KV: &fakeKV{data: map[string]string{}, partial: true}}, "RETURN kv.get('x','nope') AS v")
	if !res.Meta.GetPartialGraphPossible() {
		t.Fatal("an unreachable-holder read must set QueryMeta.partial_graph_possible")
	}
	if len(res.Meta.GetWarnings()) == 0 {
		t.Fatal("expected a warning describing the partial read")
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

// TestKVGetInWhereFiltersRows is an end-to-end check: scan the social fixture's User nodes and
// filter them by an inline kv.get over the 'flags' namespace keyed on the node's id property.
// The fixture defines users alice..heidi (with property id); only 'alice' is flagged 'banned'.
func TestKVGetInWhereFiltersRows(t *testing.T) {
	e := newExec(t)
	e.KV = &fakeKV{data: map[string]string{
		nsKey("flags", []byte("alice")): "banned",
		nsKey("flags", []byte("bob")):   "active",
	}}
	loadFixture(t, e)
	res := run(t, e, "MATCH (n:User) WHERE kv.get('flags', n.id) = 'banned' RETURN n.name")
	got := colVals(res, "n.name")
	if len(got) != 1 || got[0] != "Alice" {
		t.Fatalf("expected [Alice], got %v", got)
	}
}

// A non-UTF8 stored value (which the gRPC KV API may legitimately write as bytes) must read back as
// a Cypher bytes value — not a hard error and not an opaque proto-marshal stream corruption.
func TestKVGetNonUTF8ValueReturnsBytes(t *testing.T) {
	bin := "\xff\xfe\xfd"
	kv := &fakeKV{data: map[string]string{nsKey("x", []byte("bin")): bin}}
	res := runQuery(t, &Executor{KV: kv}, "RETURN kv.get('x','bin') AS v")
	bv, ok := res.Rows[0]["v"].GetValue().(*wavespanv1.Value_BytesValue)
	if !ok {
		t.Fatalf("expected a bytes value, got %T", res.Rows[0]["v"].GetValue())
	}
	if string(bv.BytesValue) != bin {
		t.Fatalf("bytes round-trip mismatch: got %x", bv.BytesValue)
	}
	// The original bug: a non-UTF8 value forced into a proto string field corrupts the result
	// stream with an opaque marshal error. A bytes field has no UTF-8 constraint, so marshal must
	// now succeed — this is the regression guard for that stream-corruption failure.
	if _, err := proto.Marshal(res.Rows[0]["v"]); err != nil {
		t.Fatalf("non-UTF8 kv.get value must marshal cleanly, got: %v", err)
	}
}
