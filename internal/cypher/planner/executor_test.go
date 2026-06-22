package planner

import (
	"sort"
	"testing"

	"github.com/cwire/wavespan/internal/cypher/parser"
	"github.com/cwire/wavespan/internal/graph"
	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func newExec(t *testing.T) *Executor {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	seq := uint64(0)
	return &Executor{
		Store: graph.NewStore(mem), GraphID: "g", Limits: DefaultLimits(),
		Router: LocalRouter{Self: "self"}, SelfCluster: "dev", SelfMember: "self",
		NewVersion: func() *wavespanv1.Version {
			seq++
			return &wavespanv1.Version{HlcPhysicalMs: seq, WriterClusterId: "dev", WriterMemberId: "self", WriterSequence: seq}
		},
	}
}

func run(t *testing.T, e *Executor, q string) *Result {
	t.Helper()
	ast, err := parser.Parse(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	res, err := e.Execute(ast)
	if err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
	return res
}

func colVals(res *Result, col string) []string {
	var out []string
	for _, r := range res.Rows {
		out = append(out, r[col].GetStringValue())
	}
	sort.Strings(out)
	return out
}

func seedSocial(t *testing.T, e *Executor) {
	run(t, e, "CREATE (a:User {id:'a', name:'alice', age:30})")
	run(t, e, "CREATE (b:User {id:'b', name:'bob', age:40})")
	run(t, e, "CREATE (c:User {id:'c', name:'carol', age:25})")
	// alice follows bob and carol; bob follows carol
	st := e.Store
	mk := func(s, d string) {
		if err := st.CreateEdge(&wavespanv1.EdgeRecord{GraphId: "g", EdgeId: s + "|FOLLOWS|" + d, StartNode: s, EndNode: d, Type: "FOLLOWS", Version: &wavespanv1.Version{HlcPhysicalMs: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	mk("a", "b")
	mk("a", "c")
	mk("b", "c")
}

func TestExecuteMatchReturn(t *testing.T) {
	e := newExec(t)
	seedSocial(t, e)

	res := run(t, e, "MATCH (n:User) RETURN n.name")
	if got := colVals(res, "n.name"); !streq(got, []string{"alice", "bob", "carol"}) {
		t.Fatalf("label scan names = %v", got)
	}
	res = run(t, e, "MATCH (n:User)-[:FOLLOWS]->(m) RETURN m.name")
	if got := colVals(res, "m.name"); !streq(got, []string{"bob", "carol", "carol"}) {
		t.Fatalf("FOLLOWS targets = %v", got)
	}
	res = run(t, e, "MATCH (n:User) WHERE n.age > 30 RETURN n.name")
	if got := colVals(res, "n.name"); !streq(got, []string{"bob"}) {
		t.Fatalf("age>30 = %v", got)
	}
}

func TestExecuteCreateSetDelete(t *testing.T) {
	e := newExec(t)
	run(t, e, "CREATE (a:User {id:'1', name:'alice'})")
	if got := colVals(run(t, e, "MATCH (n:User {id:'1'}) RETURN n.name"), "n.name"); !streq(got, []string{"alice"}) {
		t.Fatalf("created node not found: %v", got)
	}
	run(t, e, "MATCH (n:User {id:'1'}) SET n.name = 'alice2'")
	if got := colVals(run(t, e, "MATCH (n:User {id:'1'}) RETURN n.name"), "n.name"); !streq(got, []string{"alice2"}) {
		t.Fatalf("SET did not update: %v", got)
	}
	run(t, e, "MATCH (n:User {id:'1'}) DELETE n")
	if got := run(t, e, "MATCH (n:User {id:'1'}) RETURN n.name").Rows; len(got) != 0 {
		t.Fatalf("DELETE did not remove node: %v", got)
	}
}

func TestQueryMetaPartialGraphPossible(t *testing.T) {
	e := newExec(t)
	seedSocial(t, e)
	// local router -> single pod -> not partial
	res := run(t, e, "MATCH (n:User) RETURN n.name")
	if res.Meta.GetPartialGraphPossible() {
		t.Fatal("a fully-local query must not be partial_graph_possible")
	}
	if res.Meta.GetConsistency() != wavespanv1.QueryConsistency_QUERY_CONSISTENCY_EVENTUAL {
		t.Fatal("graph queries are eventual")
	}
	// a router that spreads partitions across pods -> partial
	e.Router = spreadRouter{}
	e.pods = nil
	res = run(t, e, "MATCH (n:User) RETURN n.name")
	if !res.Meta.GetPartialGraphPossible() {
		t.Fatal("a multi-pod query should set partial_graph_possible")
	}
}

// spreadRouter maps each partition to a distinct pod.
type spreadRouter struct{}

func (spreadRouter) PodFor(p uint32) string { return "pod-" + string(rune('a'+int(p%8))) }

func TestGuardrailsEnforced(t *testing.T) {
	e := newExec(t)
	seedSocial(t, e)
	e.Limits.MaxIntermediateRows = 1 // 3 User nodes exceed this
	if _, err := e.Execute(mustAst(t, "MATCH (n:User) RETURN n.name")); err == nil {
		t.Fatal("expected maxIntermediateRows guardrail error")
	}
}

func mustAst(t *testing.T, q string) *parser.Query {
	t.Helper()
	a, err := parser.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func streq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
