// Package bench holds WaveSpan recall/latency benchmarks. The vector ANN benchmark sweeps efSearch
// and reports recall@k vs the exact oracle plus query latency (M10 deliverable).
package bench

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/vector"
	"github.com/yannick/wavespan/internal/vector/ann"
)

func randVecs(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dim)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		out[i] = v
	}
	return out
}

func recallAt(approx []ann.Candidate, exact []ann.Candidate, k int) float64 {
	want := map[string]bool{}
	for i := 0; i < k && i < len(exact); i++ {
		want[exact[i].ID] = true
	}
	hit := 0
	for i := 0; i < k && i < len(approx); i++ {
		if want[approx[i].ID] {
			hit++
		}
	}
	return float64(hit) / float64(k)
}

func id(i int) string { return fmt.Sprintf("v%05d", i) }

// TestVectorANNRecallLatencyReport builds an HNSW over a fixture set and reports recall@10 and
// query latency across efSearch values, asserting the pure-Go HNSW meets the ADR-0006 target.
func TestVectorANNRecallLatencyReport(t *testing.T) {
	const n, dim, k = 5000, 32, 10
	metric := vector.Cosine
	dist := func(a, b []float32) float64 { return vector.Distance(metric, a, b) }

	vecs := randVecs(n, dim, 7)
	h := ann.NewHNSW(dist, ann.Params{M: 16, EfConstruction: 200, EfSearchDefault: 64, Seed: 3})
	exact := ann.NewBruteForce(dist)
	for i, v := range vecs {
		h.Insert(id(i), v)
		exact.Insert(id(i), v)
	}
	queries := randVecs(50, dim, 99)

	t.Logf("vector ANN benchmark: n=%d dim=%d metric=cosine k=%d", n, dim, k)
	t.Logf("%-10s %-12s %-14s", "efSearch", "recall@10", "p50 latency")
	var recallAt128 float64
	for _, ef := range []int{16, 32, 64, 128, 256} {
		var totalRecall float64
		lats := make([]time.Duration, 0, len(queries))
		for _, q := range queries {
			start := time.Now()
			approx := h.Search(q, k, ef)
			lats = append(lats, time.Since(start))
			totalRecall += recallAt(approx, exact.Search(q, k, 0), k)
		}
		recall := totalRecall / float64(len(queries))
		if ef == 128 {
			recallAt128 = recall
		}
		t.Logf("%-10d %-12.3f %-14s", ef, recall, p50(lats))
	}
	if recallAt128 < 0.90 {
		t.Fatalf("recall@10 at efSearch=128 = %.3f, below the 0.90 ADR-0006 target", recallAt128)
	}
}

func p50(lats []time.Duration) time.Duration {
	if len(lats) == 0 {
		return 0
	}
	// simple insertion sort (small n) then median
	cp := append([]time.Duration(nil), lats...)
	for i := 1; i < len(cp); i++ {
		for j := i; j > 0 && cp[j] < cp[j-1]; j-- {
			cp[j], cp[j-1] = cp[j-1], cp[j]
		}
	}
	return cp[len(cp)/2]
}
