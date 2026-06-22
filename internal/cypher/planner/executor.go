package planner

import (
	"fmt"
	"sort"

	"github.com/cwire/wavespan/internal/cypher/parser"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/vector"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// bindingRow binds variables to nodes (*NodeRecord), edges (*EdgeRecord), or scalars (*Value).
type bindingRow map[string]any

// Result is the outcome of executing a query.
type Result struct {
	Columns []string
	Rows    []map[string]*wavespanv1.Value
	Meta    *wavespanv1.QueryMeta
}

// Executor runs a logical plan against a local graph store with guardrail enforcement and honest
// QueryMeta (design/07). Cross-pod fan-out is reflected via partial_graph_possible.
type Executor struct {
	Store       *graph.Store
	GraphID     string
	Limits      Limits
	Router      PartitionRouter
	SelfCluster string
	SelfMember  string
	Params      map[string]*wavespanv1.Value
	NewVersion  func() *wavespanv1.Version

	// Vector search support (M9): the vector store and an index-name resolver enable the
	// vector.searchExact procedure.
	VectorStore *vector.Store
	VectorIndex func(name string) (*vector.IndexMeta, bool)

	pods    map[string]bool
	warns   []string
	columns []string
}

// Procedure is a CALL-able procedure: it extends a binding row with YIELD bindings.
type Procedure func(e *Executor, args []*wavespanv1.Value, yields []string, row bindingRow) ([]bindingRow, error)

var procedures = map[string]Procedure{}

// RegisterProcedure registers a CALL-able procedure by name (e.g. "vector.searchExact").
func RegisterProcedure(name string, fn Procedure) { procedures[name] = fn }

// Execute plans and runs a parsed query.
func (e *Executor) Execute(q *parser.Query) (*Result, error) {
	ops, err := Plan(q)
	if err != nil {
		return nil, err
	}
	if e.Router == nil {
		e.Router = LocalRouter{Self: e.SelfMember}
	}
	if e.Limits == (Limits{}) {
		e.Limits = DefaultLimits()
	}
	e.pods = map[string]bool{}
	rows := []bindingRow{{}}
	for _, op := range ops {
		if rows, err = e.apply(op, rows); err != nil {
			return nil, err
		}
		if err := e.Limits.checkIntermediate(len(rows)); err != nil {
			return nil, err
		}
	}
	out := e.toOutput(rows)
	if len(out) > e.Limits.MaxRowsReturned {
		return nil, &GuardrailError{Limit: "maxRowsReturned", Value: len(out), Max: e.Limits.MaxRowsReturned}
	}
	return &Result{Columns: e.columns, Rows: out, Meta: e.meta()}, nil
}

func (e *Executor) meta() *wavespanv1.QueryMeta {
	pods := make([]string, 0, len(e.pods))
	for p := range e.pods {
		pods = append(pods, p)
	}
	sort.Strings(pods)
	partial := len(e.pods) > 1
	completeness := wavespanv1.Completeness_COMPLETE
	if partial {
		completeness = wavespanv1.Completeness_PARTIAL
	}
	return &wavespanv1.QueryMeta{
		ServedByClusterId:    e.SelfCluster,
		ParticipatingMembers: pods,
		Consistency:          wavespanv1.QueryConsistency_QUERY_CONSISTENCY_EVENTUAL,
		Completeness:         completeness,
		UsedCache:            false,
		PartialGraphPossible: partial,
		Warnings:             e.warns,
	}
}

func (e *Executor) touch(nodeID string) {
	e.pods[e.Router.PodFor(graph.Partition(e.GraphID, nodeID))] = true
}

func (e *Executor) apply(op LogicalOp, rows []bindingRow) ([]bindingRow, error) {
	switch o := op.(type) {
	case *LabelScan:
		return e.scan(rows, o.Variable, func() ([]*wavespanv1.NodeRecord, error) {
			ids, err := e.Store.ScanLabel(e.GraphID, o.Label)
			if err != nil {
				return nil, err
			}
			return e.nodesByID(ids)
		})
	case *AllNodesScan:
		return e.scan(rows, o.Variable, func() ([]*wavespanv1.NodeRecord, error) {
			return e.Store.AllNodes(e.GraphID)
		})
	case *PropertyFilter:
		var kept []bindingRow
		for _, r := range rows {
			if e.evalBool(o.Predicate, r) {
				kept = append(kept, r)
			}
		}
		return kept, nil
	case *ExpandOutgoing:
		return e.expand(rows, o.From, o.To, o.RelVar, o.Type, true)
	case *ExpandIncoming:
		return e.expand(rows, o.From, o.To, o.RelVar, o.Type, false)
	case *Unwind:
		return e.unwind(rows, o)
	case *Project:
		return e.project(rows, o)
	case *Sort:
		e.sortRows(rows, o.Keys)
		return rows, nil
	case *SkipLimit:
		return e.skipLimit(rows, o), nil
	case *CreatePatterns:
		return rows, e.create(o, rows)
	case *SetItems:
		return rows, e.set(o, rows)
	case *DeleteVars:
		return rows, e.del(o, rows)
	case *ProcCall:
		return e.procCall(o, rows)
	}
	return rows, fmt.Errorf("cypher: unsupported operator %s", op.opName())
}

func (e *Executor) procCall(o *ProcCall, rows []bindingRow) ([]bindingRow, error) {
	proc, ok := procedures[o.Procedure]
	if !ok {
		return nil, fmt.Errorf("cypher: unknown procedure %s", o.Procedure)
	}
	var out []bindingRow
	for _, r := range rows {
		args := make([]*wavespanv1.Value, len(o.Args))
		for i, a := range o.Args {
			args[i] = e.evalScalar(a, r)
		}
		prows, err := proc(e, args, o.Yields, r)
		if err != nil {
			return nil, err
		}
		out = append(out, prows...)
	}
	return out, nil
}

func (e *Executor) nodesByID(ids []string) ([]*wavespanv1.NodeRecord, error) {
	out := make([]*wavespanv1.NodeRecord, 0, len(ids))
	for _, id := range ids {
		if n, found, err := e.Store.GetNode(e.GraphID, id); err == nil && found {
			out = append(out, n)
		}
	}
	return out, nil
}

func (e *Executor) scan(rows []bindingRow, variable string, fetch func() ([]*wavespanv1.NodeRecord, error)) ([]bindingRow, error) {
	var nodes []*wavespanv1.NodeRecord
	var fetched bool
	var out []bindingRow
	for _, r := range rows {
		// a variable already bound by a prior CALL/MATCH is a constraint, not a fresh scan.
		if _, ok := r[variable].(*wavespanv1.NodeRecord); ok {
			out = append(out, r)
			continue
		}
		if !fetched {
			n, err := fetch()
			if err != nil {
				return nil, err
			}
			nodes, fetched = n, true
		}
		for _, n := range nodes {
			nr := cloneRow(r)
			nr[variable] = n
			e.touch(n.GetNodeId())
			out = append(out, nr)
		}
	}
	return out, nil
}

func (e *Executor) expand(rows []bindingRow, from, to, relVar, typ string, outgoing bool) ([]bindingRow, error) {
	if err := e.Limits.checkDepth(1); err != nil { // single hop; chained expands accumulate naturally
		return nil, err
	}
	var out []bindingRow
	for _, r := range rows {
		node, ok := r[from].(*wavespanv1.NodeRecord)
		if !ok {
			continue
		}
		var edges []*wavespanv1.EdgeRecord
		var err error
		if outgoing {
			edges, err = e.Store.ScanOutgoing(e.GraphID, node.GetNodeId(), typ)
		} else {
			edges, err = e.Store.ScanIncoming(e.GraphID, node.GetNodeId(), typ)
		}
		if err != nil {
			return nil, err
		}
		for _, edge := range edges {
			otherID := edge.GetEndNode()
			if !outgoing {
				otherID = edge.GetStartNode()
			}
			other, found, _ := e.Store.GetNode(e.GraphID, otherID)
			if !found {
				continue
			}
			nr := cloneRow(r)
			nr[to] = other
			if relVar != "" {
				nr[relVar] = edge
			}
			e.touch(otherID)
			out = append(out, nr)
		}
	}
	return out, nil
}

func (e *Executor) unwind(rows []bindingRow, o *Unwind) ([]bindingRow, error) {
	var out []bindingRow
	for _, r := range rows {
		v := e.evalScalar(o.Expr, r)
		if v.GetListValue() == nil {
			continue
		}
		for _, elem := range v.GetListValue().GetValues() {
			nr := cloneRow(r)
			nr[o.Alias] = elem
			out = append(out, nr)
		}
	}
	return out, nil
}

func (e *Executor) project(rows []bindingRow, o *Project) ([]bindingRow, error) {
	e.columns = projectColumns(o.Items)
	var out []bindingRow
	seen := map[string]bool{}
	for _, r := range rows {
		nr := bindingRow{}
		var keyparts string
		for i, item := range o.Items {
			val := e.evalScalar(item.Expr, r)
			nr[e.columns[i]] = val
			keyparts += valueKey(val) + "\x1f"
		}
		if o.Distinct {
			if seen[keyparts] {
				continue
			}
			seen[keyparts] = true
		}
		out = append(out, nr)
	}
	return out, nil
}

func (e *Executor) sortRows(rows []bindingRow, keys []parser.SortItem) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, k := range keys {
			a, b := e.evalScalar(k.Expr, rows[i]), e.evalScalar(k.Expr, rows[j])
			c := compareValues(a, b)
			if c == 0 {
				continue
			}
			if k.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
}

func (e *Executor) skipLimit(rows []bindingRow, o *SkipLimit) []bindingRow {
	skip := 0
	if o.Skip != nil {
		skip = int(e.evalScalar(o.Skip, bindingRow{}).GetIntValue())
	}
	if skip > len(rows) {
		skip = len(rows)
	}
	rows = rows[skip:]
	if o.Limit != nil {
		limit := int(e.evalScalar(o.Limit, bindingRow{}).GetIntValue())
		if limit < len(rows) {
			rows = rows[:limit]
		}
	}
	return rows
}

// toOutput converts the final binding rows to scalar output rows. If the last clause was not a
// projection, bare variables are surfaced by id.
func (e *Executor) toOutput(rows []bindingRow) []map[string]*wavespanv1.Value {
	out := make([]map[string]*wavespanv1.Value, 0, len(rows))
	for _, r := range rows {
		m := map[string]*wavespanv1.Value{}
		if len(e.columns) > 0 {
			for _, col := range e.columns {
				if v, ok := r[col].(*wavespanv1.Value); ok {
					m[col] = v
				}
			}
		} else {
			for k, v := range r {
				m[k] = bindingToValue(v)
			}
		}
		out = append(out, m)
	}
	return out
}

func cloneRow(r bindingRow) bindingRow {
	nr := make(bindingRow, len(r)+1)
	for k, v := range r {
		nr[k] = v
	}
	return nr
}

func projectColumns(items []parser.ReturnItem) []string {
	cols := make([]string, len(items))
	for i, item := range items {
		cols[i] = itemName(item)
	}
	return cols
}

func itemName(item parser.ReturnItem) string {
	if item.Alias != "" {
		return item.Alias
	}
	switch ex := item.Expr.(type) {
	case *parser.Variable:
		return ex.Name
	case *parser.PropertyAccess:
		return ex.Variable + "." + ex.Property
	default:
		return "expr"
	}
}
