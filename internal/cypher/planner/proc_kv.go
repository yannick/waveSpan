package planner

import (
	"context"
	"fmt"
	"unicode/utf8"

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
	val, found, partial, err := e.KV.Get(e.kvCtx(), ns, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("cypher: kv.get(%q, %q): %w", ns, key, err)
	}
	if partial {
		// A holder was unreachable, so a null here may be a false negative. Surface it via
		// QueryMeta.partial_graph_possible + a warning rather than presenting a definite absence.
		e.MarkPartial(fmt.Sprintf("kv.get(%q, %q): a holder was unreachable; result may be incomplete", ns, key))
	}
	if !found {
		return vNull(), nil
	}
	// The gRPC KV API stores values as arbitrary bytes. A non-UTF8 value cannot go into a proto
	// string field — doing so would corrupt the result stream with an opaque marshal error
	// mid-query. Return UTF-8 values as a Cypher string (the common case) and any other bytes as a
	// bytes value, which has no UTF-8 constraint. This keeps kv.get readable for every value the KV
	// API can write, with no stream corruption.
	if utf8.Valid(val) {
		return vStr(string(val)), nil
	}
	return vBytes(val), nil
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
