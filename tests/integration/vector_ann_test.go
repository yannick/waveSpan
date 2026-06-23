//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// approxSearch runs vector.searchApprox and returns the result node ids.
func approxSearch(t *testing.T, port string, query []float32, k int) []string {
	t.Helper()
	ql := &wavespanv1.ValueList{}
	for _, v := range query {
		ql.Values = append(ql.Values, &wavespanv1.Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: float64(v)}})
	}
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{
		GraphId:    "g",
		Query:      "CALL vector.searchApprox('docs', $q, " + itoa(k) + ", {efSearch:64}) YIELD node, score RETURN node, score",
		Parameters: map[string]*wavespanv1.Value{"q": {Value: &wavespanv1.Value_ListValue{ListValue: ql}}},
	}))
	if err != nil {
		t.Fatalf("searchApprox: %v", err)
	}
	var out []string
	for stream.Receive() {
		if row := stream.Msg().GetRow(); row != nil {
			out = append(out, row.GetColumns()["node"].GetStringValue())
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func contains(s []string, want string) bool {
	for _, v := range s {
		if v == want {
			return true
		}
	}
	return false
}

func TestVectorANNDeltaVisibilityOverCluster(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const port = "7811"
	ctx := context.Background()
	put := func(id string, tombstone bool, vals ...float32) {
		if _, err := vectorClient(port).Put(ctx, connect.NewRequest(&wavespanv1.PutVectorRequest{
			Record: &wavespanv1.VectorRecord{Collection: "docs", VectorId: id, Values: vals, Tombstone: tombstone},
		})); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}

	put("a", false, 1, 0, 0, 0, 0, 0, 0, 0)
	put("b", false, 0, 1, 0, 0, 0, 0, 0, 0)
	// delta visibility: searchable immediately, before any background merge
	if got := approxSearch(t, port, []float32{1, 0, 0, 0, 0, 0, 0, 0}, 1); !contains(got, "a") {
		t.Fatalf("freshly ingested vector not visible via delta: %v", got)
	}
	// tombstone 'a' -> filtered from approximate results
	put("a", true, 1, 0, 0, 0, 0, 0, 0, 0)
	if got := approxSearch(t, port, []float32{1, 0, 0, 0, 0, 0, 0, 0}, 3); contains(got, "a") {
		t.Fatalf("tombstoned vector should be filtered: %v", got)
	}
}
