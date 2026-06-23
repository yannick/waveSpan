package planner

import "github.com/yannick/wavespan/internal/cypher/parser"

// LogicalOp is a node in the logical plan tree (design/07 "Planner"). The plan is a linear pipeline
// of operators for the v1 subset.
type LogicalOp interface{ opName() string }

// LabelScan scans all nodes carrying a label into the binding `Variable`.
type LabelScan struct {
	Variable string
	Label    string
}

// AllNodesScan scans every node (no label) into `Variable`.
type AllNodesScan struct{ Variable string }

// PropertyFilter keeps rows whose predicate holds (WHERE / inline property match).
type PropertyFilter struct{ Predicate parser.Expr }

// ExpandOutgoing / ExpandIncoming expand a relationship from `From` to `To`, optionally typed.
type ExpandOutgoing struct {
	From, To, RelVar, Type string
}

// ExpandIncoming expands an incoming relationship.
type ExpandIncoming struct {
	From, To, RelVar, Type string
}

// ExpandBoth expands an undirected relationship (`-[]-`): neighbours via both outgoing and incoming
// adjacency.
type ExpandBoth struct {
	From, To, RelVar, Type string
}

// Unwind expands a list expression into one row per element bound to Alias.
type Unwind struct {
	Expr  parser.Expr
	Alias string
}

// Project evaluates the RETURN/WITH items into output columns.
type Project struct {
	Items    []parser.ReturnItem
	Distinct bool
}

// Sort orders rows by the ORDER BY keys.
type Sort struct{ Keys []parser.SortItem }

// SkipLimit slices the result.
type SkipLimit struct {
	Skip  parser.Expr
	Limit parser.Expr
}

// CreatePatterns is the CREATE updating operator.
type CreatePatterns struct{ Patterns []parser.PatternPart }

// SetItems is the SET updating operator.
type SetItems struct{ Items []parser.SetItem }

// DeleteVars is the DELETE updating operator.
type DeleteVars struct{ Variables []string }

// ProcCall invokes a registered procedure (CALL ... YIELD ...).
type ProcCall struct {
	Procedure string
	Args      []parser.Expr
	Yields    []string
}

func (*LabelScan) opName() string      { return "LabelScan" }
func (*AllNodesScan) opName() string   { return "AllNodesScan" }
func (*PropertyFilter) opName() string { return "PropertyFilter" }
func (*ExpandOutgoing) opName() string { return "ExpandOutgoing" }
func (*ExpandIncoming) opName() string { return "ExpandIncoming" }
func (*ExpandBoth) opName() string     { return "ExpandBoth" }
func (*Unwind) opName() string         { return "Unwind" }
func (*Project) opName() string        { return "Project" }
func (*Sort) opName() string           { return "Sort" }
func (*SkipLimit) opName() string      { return "SkipLimit" }
func (*CreatePatterns) opName() string { return "CreatePatterns" }
func (*SetItems) opName() string       { return "SetItems" }
func (*DeleteVars) opName() string     { return "DeleteVars" }
func (*ProcCall) opName() string       { return "ProcCall" }

// Plan lowers a parsed query into a linear logical plan.
func Plan(q *parser.Query) ([]LogicalOp, error) {
	var ops []LogicalOp
	for _, c := range q.Clauses {
		switch cl := c.(type) {
		case *parser.MatchClause:
			ops = append(ops, planMatch(cl)...)
		case *parser.CreateClause:
			ops = append(ops, &CreatePatterns{Patterns: cl.Patterns})
		case *parser.SetClause:
			ops = append(ops, &SetItems{Items: cl.Items})
		case *parser.DeleteClause:
			ops = append(ops, &DeleteVars{Variables: cl.Variables})
		case *parser.UnwindClause:
			ops = append(ops, &Unwind{Expr: cl.Expr, Alias: cl.Alias})
		case *parser.CallClause:
			ops = append(ops, &ProcCall{Procedure: cl.Procedure, Args: cl.Args, Yields: cl.Yields})
		case *parser.WithClause:
			ops = append(ops, &Project{Items: cl.Items, Distinct: cl.Distinct})
			if cl.Where != nil {
				ops = append(ops, &PropertyFilter{Predicate: cl.Where})
			}
			ops = appendOrderSkipLimit(ops, cl.OrderBy, cl.Skip, cl.Limit)
		case *parser.ReturnClause:
			ops = append(ops, &Project{Items: cl.Items, Distinct: cl.Distinct})
			ops = appendOrderSkipLimit(ops, cl.OrderBy, cl.Skip, cl.Limit)
		}
	}
	return ops, nil
}

func planMatch(m *parser.MatchClause) []LogicalOp {
	var ops []LogicalOp
	for _, part := range m.Patterns {
		// the anchor node: label scan if labelled, else all-nodes scan
		if len(part.Node.Labels) > 0 {
			ops = append(ops, &LabelScan{Variable: part.Node.Variable, Label: part.Node.Labels[0]})
		} else {
			ops = append(ops, &AllNodesScan{Variable: part.Node.Variable})
		}
		if pf := inlinePropFilter(part.Node); pf != nil {
			ops = append(ops, pf)
		}
		from := part.Node.Variable
		for i, rel := range part.Rels {
			to := part.Nodes[i].Variable
			typ := ""
			if len(rel.Types) > 0 {
				typ = rel.Types[0]
			}
			switch rel.Direction {
			case parser.DirIn:
				ops = append(ops, &ExpandIncoming{From: from, To: to, RelVar: rel.Variable, Type: typ})
			case parser.DirBoth:
				ops = append(ops, &ExpandBoth{From: from, To: to, RelVar: rel.Variable, Type: typ})
			default:
				ops = append(ops, &ExpandOutgoing{From: from, To: to, RelVar: rel.Variable, Type: typ})
			}
			if pf := inlinePropFilter(part.Nodes[i]); pf != nil {
				ops = append(ops, pf)
			}
			from = to
		}
	}
	if m.Where != nil {
		ops = append(ops, &PropertyFilter{Predicate: m.Where})
	}
	return ops
}

// inlinePropFilter turns a node pattern's inline {prop: value} into an equality predicate.
func inlinePropFilter(n parser.NodePattern) *PropertyFilter {
	if len(n.Properties) == 0 {
		return nil
	}
	var pred parser.Expr
	for prop, val := range n.Properties {
		eq := &parser.BinaryExpr{Op: "=", Left: &parser.PropertyAccess{Variable: n.Variable, Property: prop}, Right: val}
		if pred == nil {
			pred = eq
		} else {
			pred = &parser.BinaryExpr{Op: "AND", Left: pred, Right: eq}
		}
	}
	return &PropertyFilter{Predicate: pred}
}

func appendOrderSkipLimit(ops []LogicalOp, order []parser.SortItem, skip, limit parser.Expr) []LogicalOp {
	if len(order) > 0 {
		ops = append(ops, &Sort{Keys: order})
	}
	if skip != nil || limit != nil {
		ops = append(ops, &SkipLimit{Skip: skip, Limit: limit})
	}
	return ops
}
