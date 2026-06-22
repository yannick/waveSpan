package planner

import (
	"context"
	"fmt"
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

type fakeKV struct {
	data   map[string]string
	ver    int
	getErr error
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

// A non-UTF8 stored value (which the gRPC KV API may legitimately write as bytes) must surface as a
// clean query error from kv.get, not corrupt the result stream with an opaque proto marshal failure.
func TestKVGetNonUTF8ValueErrors(t *testing.T) {
	kv := &fakeKV{data: map[string]string{nsKey("x", []byte("bin")): "\xff\xfe\xfd"}}
	ast, _ := parser.Parse("RETURN kv.get('x','bin') AS v")
	if _, err := (&Executor{KV: kv}).Execute(ast); err == nil {
		t.Fatal("kv.get on a non-UTF8 value must be a hard error")
	}
}
