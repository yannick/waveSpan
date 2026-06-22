package planner

import (
	"os"
	"strings"
	"testing"

	"github.com/cwire/wavespan/internal/cypher/parser"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// loadFixture parses and executes the social fixture's statements into the executor's store.
func loadFixture(t *testing.T, e *Executor) {
	t.Helper()
	data, err := os.ReadFile("../../../fixtures/graph/social.cypher")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(stripComments(string(data)), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		ast, err := parser.Parse(stmt)
		if err != nil {
			t.Fatalf("fixture parse %q: %v", stmt, err)
		}
		if _, err := e.Execute(ast); err != nil {
			t.Fatalf("fixture exec %q: %v", stmt, err)
		}
	}
}

func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

var fixtureOracle = []struct {
	query string
	col   string
	want  []string
}{
	{"MATCH (n:User) RETURN n.name", "n.name", []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"}},
	{"MATCH (n:User) WHERE n.age >= 35 RETURN n.name", "n.name", []string{"Bob", "Dave", "Eve", "Grace"}},
	{"MATCH (n:User) WHERE n.city = 'NYC' RETURN n.name", "n.name", []string{"Alice", "Bob", "Eve", "Heidi"}},
	{"MATCH (a:User {id:'alice'})-[:FOLLOWS]->(m) RETURN m.name", "m.name", []string{"Bob", "Carol"}},
}

func TestFixtureOracle(t *testing.T) {
	e := newExec(t)
	loadFixture(t, e)
	for _, tc := range fixtureOracle {
		got := colVals(run(t, e, tc.query), tc.col)
		if !streq(got, tc.want) {
			t.Errorf("query %q = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestFixtureSurvivesIndexRebuild(t *testing.T) {
	e := newExec(t)
	loadFixture(t, e)
	if err := e.Store.RebuildIndexes("g"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range fixtureOracle {
		got := colVals(run(t, e, tc.query), tc.col)
		if !streq(got, tc.want) {
			t.Errorf("after rebuild, query %q = %v, want %v", tc.query, got, tc.want)
		}
	}
}

func TestFixtureQueryMeta(t *testing.T) {
	e := newExec(t)
	loadFixture(t, e)
	meta := run(t, e, "MATCH (n:User) RETURN n.name").Meta
	if meta.GetConsistency() != wavespanv1.QueryConsistency_QUERY_CONSISTENCY_EVENTUAL {
		t.Fatal("graph query must declare eventual consistency")
	}
	if meta.GetServedByClusterId() != "dev" {
		t.Fatalf("served_by_cluster_id = %q", meta.GetServedByClusterId())
	}
}
