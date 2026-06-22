package parser

import (
	"strings"
	"testing"
)

func mustParse(t *testing.T, q string) *Query {
	t.Helper()
	ast, err := Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return ast
}

func TestParseMatchWhereReturnOrderSkipLimit(t *testing.T) {
	q := mustParse(t, "MATCH (n:User)-[:FOLLOWS]->(m) WHERE n.age > 30 RETURN m.name ORDER BY m.name SKIP 2 LIMIT 10")
	if len(q.Clauses) != 2 {
		t.Fatalf("want 2 clauses, got %d", len(q.Clauses))
	}
	mc, ok := q.Clauses[0].(*MatchClause)
	if !ok || len(mc.Patterns) != 1 {
		t.Fatalf("first clause not a single-pattern MATCH: %#v", q.Clauses[0])
	}
	part := mc.Patterns[0]
	if part.Node.Variable != "n" || part.Node.Labels[0] != "User" {
		t.Fatalf("node pattern wrong: %#v", part.Node)
	}
	if len(part.Rels) != 1 || part.Rels[0].Direction != DirOut || part.Rels[0].Types[0] != "FOLLOWS" {
		t.Fatalf("relationship wrong: %#v", part.Rels)
	}
	if part.Nodes[0].Variable != "m" {
		t.Fatalf("target node wrong: %#v", part.Nodes[0])
	}
	where, ok := mc.Where.(*BinaryExpr)
	if !ok || where.Op != ">" {
		t.Fatalf("WHERE not a > comparison: %#v", mc.Where)
	}
	rc := q.Clauses[1].(*ReturnClause)
	if len(rc.Items) != 1 || len(rc.OrderBy) != 1 || rc.Skip == nil || rc.Limit == nil {
		t.Fatalf("RETURN order/skip/limit wrong: %#v", rc)
	}
}

func TestParseOptionalMatch(t *testing.T) {
	q := mustParse(t, "OPTIONAL MATCH (n) RETURN n")
	if mc, ok := q.Clauses[0].(*MatchClause); !ok || !mc.Optional {
		t.Fatalf("expected OPTIONAL MATCH: %#v", q.Clauses[0])
	}
}

func TestParseCreate(t *testing.T) {
	q := mustParse(t, "CREATE (a:User {id:'1'})-[:FOLLOWS]->(b:User {id:'2'})")
	cc, ok := q.Clauses[0].(*CreateClause)
	if !ok || len(cc.Patterns) != 1 {
		t.Fatalf("expected CREATE: %#v", q.Clauses[0])
	}
	part := cc.Patterns[0]
	if part.Node.Properties["id"] == nil || len(part.Rels) != 1 {
		t.Fatalf("create pattern wrong: %#v", part)
	}
}

func TestParseSetAndDelete(t *testing.T) {
	q := mustParse(t, "MATCH (n) SET n.x = 1")
	if sc, ok := q.Clauses[1].(*SetClause); !ok || sc.Items[0].Property != "x" {
		t.Fatalf("expected SET n.x: %#v", q.Clauses[1])
	}
	q2 := mustParse(t, "MATCH (n) DELETE n")
	if dc, ok := q2.Clauses[1].(*DeleteClause); !ok || dc.Variables[0] != "n" {
		t.Fatalf("expected DELETE n: %#v", q2.Clauses[1])
	}
}

func TestParseWithAndUnwind(t *testing.T) {
	q := mustParse(t, "WITH n.x AS x WHERE x > 0 RETURN x")
	wc, ok := q.Clauses[0].(*WithClause)
	if !ok || wc.Items[0].Alias != "x" || wc.Where == nil {
		t.Fatalf("expected WITH ... AS x WHERE: %#v", q.Clauses[0])
	}
	q2 := mustParse(t, "UNWIND [1,2,3] AS i RETURN i")
	uc, ok := q2.Clauses[0].(*UnwindClause)
	if !ok || uc.Alias != "i" {
		t.Fatalf("expected UNWIND ... AS i: %#v", q2.Clauses[0])
	}
	if lit, ok := uc.Expr.(*Literal); !ok || len(lit.List) != 3 {
		t.Fatalf("UNWIND list wrong: %#v", uc.Expr)
	}
}

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

func TestUnsupportedClauseIsExplicitError(t *testing.T) {
	for _, q := range []string{
		"MERGE (n:User {id:'1'})",
		"MATCH (n) DETACH DELETE n",
		"LOAD CSV FROM 'x' AS row RETURN row",
		"CALL db.labels()",
		"MATCH (n) REMOVE n.x",
	} {
		_, err := Parse(q)
		if err == nil {
			t.Fatalf("expected explicit unsupported error for %q", q)
		}
		if !strings.Contains(err.Error(), "unsupported in v1") {
			t.Fatalf("error for %q should say unsupported in v1, got: %v", q, err)
		}
	}
}
