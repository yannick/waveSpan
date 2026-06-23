package planner

import (
	"fmt"
	"unicode/utf8"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Replicated-collections built-ins (design/30 §13.9): set.* / hash.* / zset.* over the CP consensus
// tier, backed by Executor.Collections. Scalar functions (set.contains, set.card, hash.get,
// zset.score) read inline; procedures mutate or enumerate. Reads are bounded-stale.
func init() {
	RegisterFunction("set.contains", setContains)
	RegisterFunction("set.card", setCard)
	RegisterFunction("hash.get", hashGet)
	RegisterFunction("zset.score", zsetScore)

	RegisterProcedure("set.add", setAdd)
	RegisterProcedure("set.remove", setRemove)
	RegisterProcedure("set.members", setMembers)
	RegisterProcedure("hash.set", hashSet)
	RegisterProcedure("hash.getAll", hashGetAll)
	RegisterProcedure("zset.add", zsetAdd)
	RegisterProcedure("zset.range", zsetRange)
}

// vAuto returns a UTF-8 payload as a Cypher string, otherwise as bytes (no UTF-8 constraint).
func vAuto(b []byte) *wavespanv1.Value {
	if utf8.Valid(b) {
		return vStr(string(b))
	}
	return vBytes(b)
}

func numberArg(fn string, args []*wavespanv1.Value, i int) (float64, error) {
	if i >= len(args) {
		return 0, fmt.Errorf("cypher: %s: missing argument %d", fn, i+1)
	}
	switch v := args[i].GetValue().(type) {
	case *wavespanv1.Value_IntValue:
		return float64(v.IntValue), nil
	case *wavespanv1.Value_DoubleValue:
		return v.DoubleValue, nil
	}
	return 0, fmt.Errorf("cypher: %s: argument %d must be a number", fn, i+1)
}

// strArgs pulls n string args after validating arity and the configured backend.
func (e *Executor) collStrArgs(fn string, args []*wavespanv1.Value, n int) ([]string, error) {
	if len(args) != n {
		return nil, fmt.Errorf("cypher: %s requires %d arguments", fn, n)
	}
	if e.Collections == nil {
		return nil, fmt.Errorf("cypher: %s: collections backend not configured", fn)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		s, err := stringArg(fn, args, i)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}

// --- scalar functions ---

func setContains(e *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
	a, err := e.collStrArgs("set.contains(ns, coll, member)", args, 3)
	if err != nil {
		return nil, err
	}
	ok, err := e.Collections.SIsMember(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]))
	if err != nil {
		return nil, fmt.Errorf("cypher: set.contains(%q, %q): %w", a[0], a[1], err)
	}
	return vBool(ok), nil
}

func setCard(e *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
	a, err := e.collStrArgs("set.card(ns, coll)", args, 2)
	if err != nil {
		return nil, err
	}
	n, err := e.Collections.SCard(e.kvCtx(), a[0], []byte(a[1]))
	if err != nil {
		return nil, fmt.Errorf("cypher: set.card(%q, %q): %w", a[0], a[1], err)
	}
	return vInt(int64(n)), nil
}

func hashGet(e *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
	a, err := e.collStrArgs("hash.get(ns, coll, field)", args, 3)
	if err != nil {
		return nil, err
	}
	v, found, err := e.Collections.HGet(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]))
	if err != nil {
		return nil, fmt.Errorf("cypher: hash.get(%q, %q): %w", a[0], a[1], err)
	}
	if !found {
		return vNull(), nil
	}
	return vAuto(v), nil
}

func zsetScore(e *Executor, args []*wavespanv1.Value, _ bindingRow) (*wavespanv1.Value, error) {
	a, err := e.collStrArgs("zset.score(ns, coll, member)", args, 3)
	if err != nil {
		return nil, err
	}
	sc, found, err := e.Collections.ZScore(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]))
	if err != nil {
		return nil, fmt.Errorf("cypher: zset.score(%q, %q): %w", a[0], a[1], err)
	}
	if !found {
		return vNull(), nil
	}
	return vFloat(sc), nil
}

// --- procedures ---

func setAdd(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("set.add(ns, coll, member)", args, 3)
	if err != nil {
		return nil, err
	}
	n, err := e.Collections.SAdd(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]))
	if err != nil {
		return nil, fmt.Errorf("cypher: set.add(%q, %q): %w", a[0], a[1], err)
	}
	nr := cloneRow(row)
	nr["added"] = vInt(int64(n))
	return []bindingRow{nr}, nil
}

func setRemove(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("set.remove(ns, coll, member)", args, 3)
	if err != nil {
		return nil, err
	}
	n, err := e.Collections.SRem(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]))
	if err != nil {
		return nil, fmt.Errorf("cypher: set.remove(%q, %q): %w", a[0], a[1], err)
	}
	nr := cloneRow(row)
	nr["removed"] = vInt(int64(n))
	return []bindingRow{nr}, nil
}

func setMembers(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("set.members(ns, coll)", args, 2)
	if err != nil {
		return nil, err
	}
	members, err := e.Collections.SMembers(e.kvCtx(), a[0], []byte(a[1]), 0)
	if err != nil {
		return nil, fmt.Errorf("cypher: set.members(%q, %q): %w", a[0], a[1], err)
	}
	out := make([]bindingRow, 0, len(members))
	for _, m := range members {
		nr := cloneRow(row)
		nr["member"] = vAuto(m)
		out = append(out, nr)
	}
	return out, nil
}

func hashSet(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("hash.set(ns, coll, field, value)", args, 4)
	if err != nil {
		return nil, err
	}
	n, err := e.Collections.HSet(e.kvCtx(), a[0], []byte(a[1]), []byte(a[2]), []byte(a[3]))
	if err != nil {
		return nil, fmt.Errorf("cypher: hash.set(%q, %q): %w", a[0], a[1], err)
	}
	nr := cloneRow(row)
	nr["added"] = vInt(int64(n))
	return []bindingRow{nr}, nil
}

func hashGetAll(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("hash.getAll(ns, coll)", args, 2)
	if err != nil {
		return nil, err
	}
	fields, values, err := e.Collections.HGetAll(e.kvCtx(), a[0], []byte(a[1]), 0)
	if err != nil {
		return nil, fmt.Errorf("cypher: hash.getAll(%q, %q): %w", a[0], a[1], err)
	}
	out := make([]bindingRow, 0, len(fields))
	for i := range fields {
		nr := cloneRow(row)
		nr["field"] = vAuto(fields[i])
		nr["value"] = vAuto(values[i])
		out = append(out, nr)
	}
	return out, nil
}

func zsetAdd(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	if len(args) != 4 {
		return nil, fmt.Errorf("cypher: zset.add(ns, coll, member, score) requires 4 arguments")
	}
	if e.Collections == nil {
		return nil, fmt.Errorf("cypher: zset.add: collections backend not configured")
	}
	ns, err := stringArg("zset.add", args, 0)
	if err != nil {
		return nil, err
	}
	coll, err := stringArg("zset.add", args, 1)
	if err != nil {
		return nil, err
	}
	member, err := stringArg("zset.add", args, 2)
	if err != nil {
		return nil, err
	}
	score, err := numberArg("zset.add", args, 3)
	if err != nil {
		return nil, err
	}
	n, err := e.Collections.ZAdd(e.kvCtx(), ns, []byte(coll), []byte(member), score)
	if err != nil {
		return nil, fmt.Errorf("cypher: zset.add(%q, %q): %w", ns, coll, err)
	}
	nr := cloneRow(row)
	nr["added"] = vInt(int64(n))
	return []bindingRow{nr}, nil
}

func zsetRange(e *Executor, args []*wavespanv1.Value, _ []string, row bindingRow) ([]bindingRow, error) {
	a, err := e.collStrArgs("zset.range(ns, coll)", args, 2)
	if err != nil {
		return nil, err
	}
	members, scores, err := e.Collections.ZRange(e.kvCtx(), a[0], []byte(a[1]), 0)
	if err != nil {
		return nil, fmt.Errorf("cypher: zset.range(%q, %q): %w", a[0], a[1], err)
	}
	out := make([]bindingRow, 0, len(members))
	for i := range members {
		nr := cloneRow(row)
		nr["member"] = vAuto(members[i])
		nr["score"] = vFloat(scores[i])
		out = append(out, nr)
	}
	return out, nil
}
