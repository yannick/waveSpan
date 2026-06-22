package planner

import (
	"testing"

	"github.com/cwire/wavespan/internal/cypher/parser"
)

func planOf(t *testing.T, q string) []LogicalOp {
	t.Helper()
	ast, err := parser.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	ops, err := Plan(ast)
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func TestLogicalPlanShape(t *testing.T) {
	ops := planOf(t, "MATCH (n:User) WHERE n.age > 30 RETURN n")
	want := []string{"LabelScan", "PropertyFilter", "Project"}
	if len(ops) != len(want) {
		t.Fatalf("plan length = %d, want %d (%v)", len(ops), len(want), opNames(ops))
	}
	for i, w := range want {
		if ops[i].opName() != w {
			t.Fatalf("op[%d] = %s, want %s", i, ops[i].opName(), w)
		}
	}
}

func TestExpandUsesAdjacency(t *testing.T) {
	ops := planOf(t, "MATCH (n:User)-[:FOLLOWS]->(m) RETURN m")
	found := false
	for _, o := range ops {
		if eo, ok := o.(*ExpandOutgoing); ok {
			found = true
			if eo.Type != "FOLLOWS" || eo.From != "n" || eo.To != "m" {
				t.Fatalf("expand wrong: %#v", eo)
			}
		}
		if _, ok := o.(*AllNodesScan); ok {
			t.Fatal("a label-anchored expand must not full-scan")
		}
	}
	if !found {
		t.Fatalf("relationship pattern did not plan an ExpandOutgoing: %v", opNames(ops))
	}
}

func TestRouterRespectsMaxRemoteFragments(t *testing.T) {
	r := RouteFragments(300, DefaultLimits())
	if r.Fragments != 128 || !r.Capped || !r.PartialGraphPossible {
		t.Fatalf("router should cap at 128 and mark partial: %#v", r)
	}
	if r2 := RouteFragments(1, DefaultLimits()); r2.Capped || r2.PartialGraphPossible {
		t.Fatalf("single-partition route should not be capped/partial: %#v", r2)
	}
}

func opNames(ops []LogicalOp) []string {
	out := make([]string, len(ops))
	for i, o := range ops {
		out[i] = o.opName()
	}
	return out
}
