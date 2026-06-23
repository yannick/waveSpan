//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// ingestVectorOn stores a vector via a SPECIFIC node's data port. Intra-cluster, a raw vector lives
// only on the node that ingested it (no origin+1 fanout for vectors), so this pins where it lives.
func ingestVectorOn(t *testing.T, port, id string, values []float32) {
	t.Helper()
	if _, err := vectorClient(port).Put(context.Background(), connect.NewRequest(&wavespanv1.PutVectorRequest{
		Record: &wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: values, Dtype: "float32"},
	})); err != nil {
		t.Fatalf("ingest %s on :%s: %v", id, port, err)
	}
}

// searchApproxIDs runs vector.searchApprox via the given coordinator node and returns the hit ids.
func searchApproxIDs(t *testing.T, port string, query []float32, k int) []string {
	t.Helper()
	ql := &wavespanv1.ValueList{}
	for _, v := range query {
		ql.Values = append(ql.Values, &wavespanv1.Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: float64(v)}})
	}
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{
		GraphId: "g",
		Query:   "CALL vector.searchApprox('docs', $q, $k, {efSearch: 64}) YIELD node, score RETURN node, score",
		Parameters: map[string]*wavespanv1.Value{
			"q": {Value: &wavespanv1.Value_ListValue{ListValue: ql}},
			"k": {Value: &wavespanv1.Value_IntValue{IntValue: int64(k)}},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for stream.Receive() {
		if row := stream.Msg().GetRow(); row != nil {
			ids = append(ids, row.GetColumns()["node"].GetStringValue())
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func containsID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestVectorSearchScattersAcrossCluster proves distributed vector search: each node ingests a
// distinct vector (so it lives ONLY there), then a single coordinator's search retrieves vectors
// held on the OTHER nodes — only possible because the executor scatters SearchLocal to holders and
// merges. Before this wiring, a node only saw its own vectors.
func TestVectorSearchScattersAcrossCluster(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const n1, n2, n3 = "7811", "7812", "7813" // data ports; docs index is cosine, dim 8
	e1 := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	e2 := []float32{0, 1, 0, 0, 0, 0, 0, 0}
	e3 := []float32{0, 0, 1, 0, 0, 0, 0, 0}
	ingestVectorOn(t, n1, "v-node1", e1)
	ingestVectorOn(t, n2, "v-node2", e2) // lives only on node2
	ingestVectorOn(t, n3, "v-node3", e3) // lives only on node3
	time.Sleep(2 * time.Second)          // let each node's live ANN index absorb its write

	// Query node1 for the vector that physically lives on node2: scatter-gather must find it.
	if got := searchApproxIDs(t, n1, e2, 3); !containsID(got, "v-node2") {
		t.Fatalf("coordinator node1 failed to retrieve v-node2 (held only on node2): got %v", got)
	}
	// ...and the one on node3.
	if got := searchApproxIDs(t, n1, e3, 3); !containsID(got, "v-node3") {
		t.Fatalf("coordinator node1 failed to retrieve v-node3 (held only on node3): got %v", got)
	}
	// A broad query from one coordinator must surface all three, proving full-cluster coverage.
	all := searchApproxIDs(t, n1, []float32{0.9, 0.9, 0.9, 0, 0, 0, 0, 0}, 3)
	for _, want := range []string{"v-node1", "v-node2", "v-node3"} {
		if !containsID(all, want) {
			t.Fatalf("broad search from node1 missing %s: got %v", want, all)
		}
	}
}
