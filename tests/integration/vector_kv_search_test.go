//go:build integration

package integration

import (
	"bufio"
	"context"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// genVectors makes n deterministic dim-d unit-ish vectors (seeded, so runs are reproducible).
func genVectors(n, d int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, d)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		out[i] = v
	}
	return out
}

func vectorPut(t *testing.T, port, collection string, vec []float32, payload []byte) {
	t.Helper()
	if _, err := vectorClient(port).VectorPut(context.Background(), connect.NewRequest(&wavespanv1.VectorPutReq{
		Collection: collection, Vector: vec, Payload: payload,
	})); err != nil {
		t.Fatalf("VectorPut on :%s: %v", port, err)
	}
}

func vectorSearchIDs(t *testing.T, port, collection string, query []float32, k, nprobe int) ([]string, wavespanv1.Completeness) {
	t.Helper()
	resp, err := vectorClient(port).VectorSearch(context.Background(), connect.NewRequest(&wavespanv1.VectorSearchReq{
		Collection: collection, Query: query, K: uint32(k), Nprobe: uint32(nprobe),
	}))
	if err != nil {
		t.Fatalf("VectorSearch on :%s: %v", port, err)
	}
	var ids []string
	for _, n := range resp.Msg.GetNeighbors() {
		ids = append(ids, n.GetVectorId())
	}
	return ids, resp.Msg.GetCompleteness()
}

// searchLocalHolds reports whether the node at port physically holds (searchably) the exact vector.
func searchLocalHolds(t *testing.T, port string, query []float32) bool {
	t.Helper()
	resp, err := vectorClient(port).SearchLocal(context.Background(), connect.NewRequest(&wavespanv1.SearchLocalRequest{
		IndexName: "docs", Query: query, K: 1, EfSearch: 64,
	}))
	if err != nil {
		t.Fatalf("SearchLocal on :%s: %v", port, err)
	}
	hits := resp.Msg.GetHits()
	return len(hits) > 0 && hits[0].GetDistance() < 1e-4
}

// searchHolderCount counts how many of the data nodes searchably hold the exact vector.
func searchHolderCount(t *testing.T, dataPorts []string, query []float32) int {
	t.Helper()
	n := 0
	for _, p := range dataPorts {
		if searchLocalHolds(t, p, query) {
			n++
		}
	}
	return n
}

// metricGauge scrapes a single-series gauge value from a node's /metrics endpoint.
func metricGauge(t *testing.T, adminPort, name string) (float64, bool) {
	t.Helper()
	resp, err := http.Get("http://localhost:" + adminPort + "/metrics")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			return v, true
		}
	}
	return 0, false
}

// TestVectorKVSearch exercises the vector-as-key API, bucket routing, affinity placement +
// re-bucketing concentration, and automatic IVF training, against the live 3-node cluster.
func TestVectorKVSearch(t *testing.T) {
	compose(t, "up", "-d")
	t.Cleanup(func() { compose(t, "down", "-v") })
	waitFor(t, "3 nodes alive", 60*time.Second, func() bool { return len(membership(t, "7901")) == 3 })

	dataPorts := []string{"7811", "7812", "7813"}
	ctx := context.Background()

	t.Run("KVRoundTrip", func(t *testing.T) {
		vec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
		payload := []byte("hello-payload")
		vectorPut(t, "7811", "docs", vec, payload)

		// Exact Get returns the payload (from any coordinator, via the KV read path).
		waitFor(t, "payload readable on node3", 20*time.Second, func() bool {
			r, err := vectorClient("7813").VectorGet(ctx, connect.NewRequest(&wavespanv1.VectorGetReq{Collection: "docs", Vector: vec}))
			return err == nil && r.Msg.GetFound() && string(r.Msg.GetPayload()) == "hello-payload"
		})

		// Search finds it.
		ids, _ := vectorSearchIDs(t, "7812", "docs", vec, 1, 8)
		if len(ids) == 0 {
			t.Fatal("search returned no neighbour for an exact stored vector")
		}

		// Delete converges across every holder.
		if _, err := vectorClient("7811").VectorDelete(ctx, connect.NewRequest(&wavespanv1.VectorDeleteReq{Collection: "docs", Vector: vec})); err != nil {
			t.Fatalf("VectorDelete: %v", err)
		}
		waitFor(t, "delete converges on all nodes", 30*time.Second, func() bool {
			return searchHolderCount(t, dataPorts, vec) == 0
		})
	})

	// AffinityConcentration runs before the bulk inserts so the dataset stays below the IVF training
	// threshold — a quantizer-version change mid-test would re-assign buckets and re-churn placement.
	t.Run("AffinityConcentration", func(t *testing.T) {
		// Write each probe from EVERY node so the one node outside its 2-node ring becomes an off-ring
		// origin — putting the vector on all 3 nodes initially. The re-bucketer must then reclaim that
		// off-ring copy, concentrating the vector onto EXACTLY its 2 ring members (ring = target+1 = 2).
		probes := genVectors(6, 8, 99)
		for _, v := range probes {
			for _, p := range dataPorts {
				vectorPut(t, p, "docs", v, nil)
			}
		}
		// Sanity: at least one probe starts over-replicated on all 3 nodes (the off-ring origin copy).
		waitFor(t, "a probe is initially on all 3 nodes", 20*time.Second, func() bool {
			for _, v := range probes {
				if searchHolderCount(t, dataPorts, v) == 3 {
					return true
				}
			}
			return false
		})
		// Re-bucketing then concentrates every probe onto its 2-node ring.
		for i, v := range probes {
			v := v
			waitFor(t, "vector concentrates onto its 2-node ring", 90*time.Second, func() bool {
				return searchHolderCount(t, dataPorts, v) == 2
			})
			if got := searchHolderCount(t, dataPorts, v); got != 2 {
				t.Fatalf("probe %d held on %d nodes, want 2 (its affinity ring)", i, got)
			}
		}
	})

	t.Run("RoutedSearchMatchesBaseline", func(t *testing.T) {
		vecs := genVectors(40, 8, 7)
		for _, v := range vecs {
			vectorPut(t, "7811", "docs", v, nil)
		}
		query := vecs[3]
		// Routed (nprobe>0) must return the same top-k as the all-holders baseline (nprobe=0).
		waitFor(t, "routed == baseline top-5", 30*time.Second, func() bool {
			base, _ := vectorSearchIDs(t, "7813", "docs", query, 5, 0)
			routed, comp := vectorSearchIDs(t, "7813", "docs", query, 5, 8)
			return comp == wavespanv1.Completeness_COMPLETE && len(base) == 5 && streq(base, routed)
		})
	})

	t.Run("IVFTrainingInstalls", func(t *testing.T) {
		// Enough vectors to cross the trainer's min-sample threshold; the lowest-id node trains and
		// every node installs the artifact, advancing the quantizer version past the LSH default (1).
		for _, v := range genVectors(120, 8, 21) {
			vectorPut(t, "7811", "docs", v, nil)
		}
		for _, admin := range []string{"7901", "7902", "7903"} {
			admin := admin
			waitFor(t, "IVF quantizer installed on :"+admin, 60*time.Second, func() bool {
				v, ok := metricGauge(t, admin, `wavespan_vector_quantizer_version{collection="docs"}`)
				return ok && v >= 2
			})
		}
		// Search is still correct under the trained quantizer.
		query := []float32{0, 1, 0, 0, 0, 0, 0, 0}
		vectorPut(t, "7811", "docs", query, []byte("ivf-probe"))
		waitFor(t, "exact vector found under IVF", 30*time.Second, func() bool {
			ids, comp := vectorSearchIDs(t, "7812", "docs", query, 3, 8)
			return comp == wavespanv1.Completeness_COMPLETE && len(ids) > 0
		})
	})
}
