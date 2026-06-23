//go:build integration

package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
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

type embedding struct {
	ID     string    `json:"id"`
	Values []float32 `json:"values"`
}

func loadEmbeddings(t *testing.T) []embedding {
	t.Helper()
	f, err := os.Open("../../fixtures/vector/embeddings.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []embedding
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e embedding
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// bruteForceCosine returns the top-k ids nearest to query by cosine similarity.
func bruteForceCosine(embs []embedding, query []float32, k int) []string {
	cos := func(a, b []float32) float64 {
		var d, na, nb float64
		for i := range a {
			d += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		if na == 0 || nb == 0 {
			return 0
		}
		return d / (math.Sqrt(na) * math.Sqrt(nb))
	}
	type sc struct {
		id  string
		sim float64
	}
	var all []sc
	for _, e := range embs {
		all = append(all, sc{e.ID, cos(query, e.Values)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].sim != all[j].sim {
			return all[i].sim > all[j].sim
		}
		return all[i].id < all[j].id
	})
	var out []string
	for i := 0; i < k && i < len(all); i++ {
		out = append(out, all[i].id)
	}
	return out
}

func vectorClient(port string) wavespanv1connect.VectorServiceClient {
	return wavespanv1connect.NewVectorServiceClient(http.DefaultClient, "http://localhost:"+port)
}

func TestVectorExactSearchOverCluster(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "node up", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	const port = "7811"
	embs := loadEmbeddings(t)
	for _, e := range embs {
		if _, err := vectorClient(port).Put(context.Background(), connect.NewRequest(&wavespanv1.PutVectorRequest{
			Record: &wavespanv1.VectorRecord{Collection: "docs", VectorId: e.ID, Values: e.Values, Dtype: "float32"},
		})); err != nil {
			t.Fatalf("ingest %s: %v", e.ID, err)
		}
	}

	// query near v00 = [1,0,...]; cosine top-3 should match the brute-force oracle
	query := embs[0].Values
	want := bruteForceCosine(embs, query, 3)

	queryList := &wavespanv1.ValueList{}
	for _, v := range query {
		queryList.Values = append(queryList.Values, &wavespanv1.Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: float64(v)}})
	}
	stream, err := cypherClient(port).Query(context.Background(), connect.NewRequest(&wavespanv1.CypherRequest{
		GraphId: "g",
		Query:   "CALL vector.searchExact('docs', $q, 3) YIELD node, score RETURN node, score",
		Parameters: map[string]*wavespanv1.Value{
			"q": {Value: &wavespanv1.Value_ListValue{ListValue: queryList}},
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for stream.Receive() {
		if row := stream.Msg().GetRow(); row != nil {
			got = append(got, row.GetColumns()["node"].GetStringValue())
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if !streq(got, want) {
		t.Fatalf("exact search over cluster = %v, want oracle %v", got, want)
	}
}
