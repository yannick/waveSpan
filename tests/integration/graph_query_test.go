//go:build integration

package integration

import (
	"context"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func cypherClient(dataPort string) wavespanv1connect.CypherClient {
	return wavespanv1connect.NewCypherClient(http.DefaultClient, "http://localhost:"+dataPort)
}

// cypherQuery runs a query against the cluster and returns the string values of one column.
func cypherQuery(t *testing.T, port, query, col string) []string {
	t.Helper()
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{GraphId: "g", Query: query}))
	if err != nil {
		t.Fatalf("cypher %q: %v", query, err)
	}
	var out []string
	for stream.Receive() {
		if row := stream.Msg().GetRow(); row != nil {
			out = append(out, row.GetColumns()[col].GetStringValue())
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("cypher stream %q: %v", query, err)
	}
	sort.Strings(out)
	return out
}

func cypherExec(t *testing.T, port, stmt string) {
	t.Helper()
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{GraphId: "g", Query: stmt}))
	if err != nil {
		t.Fatalf("cypher exec %q: %v", stmt, err)
	}
	for stream.Receive() { //nolint:revive // drain the stream
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("cypher exec stream %q: %v", stmt, err)
	}
}

func TestGraphQueryOverCluster(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const port = "7811" // node1 data port; graph writes/reads are local to the serving node
	data, err := os.ReadFile("../../fixtures/graph/social.cypher")
	if err != nil {
		t.Fatal(err)
	}
	var clean strings.Builder
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		clean.WriteString(line + "\n")
	}
	for _, stmt := range strings.Split(clean.String(), ";") {
		if strings.TrimSpace(stmt) != "" {
			cypherExec(t, port, stmt)
		}
	}

	cases := []struct {
		query, col string
		want       []string
	}{
		{"MATCH (n:User) RETURN n.name", "n.name", []string{"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi"}},
		{"MATCH (n:User) WHERE n.age >= 35 RETURN n.name", "n.name", []string{"Bob", "Dave", "Eve", "Grace"}},
		{"MATCH (a:User {id:'alice'})-[:FOLLOWS]->(m) RETURN m.name", "m.name", []string{"Bob", "Carol"}},
	}
	for _, tc := range cases {
		got := cypherQuery(t, port, tc.query, tc.col)
		if !streq(got, tc.want) {
			t.Errorf("over cluster, query %q = %v, want %v", tc.query, got, tc.want)
		}
	}
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
