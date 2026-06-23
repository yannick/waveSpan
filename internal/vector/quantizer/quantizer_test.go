package quantizer

import (
	"math"
	"math/rand"
	"testing"
)

func normalize(v []float32) []float32 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	s = math.Sqrt(s)
	if s == 0 {
		return v
	}
	for i := range v {
		v[i] = float32(float64(v[i]) / s)
	}
	return v
}

func randUnit(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return normalize(v)
}

func TestLSHDeterministic(t *testing.T) {
	q := NewLSH(16, 8, 42, 1)
	v := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if q.Bucket(v) != q.Bucket(v) {
		t.Fatal("bucket not deterministic")
	}
	// A different seed gives a different planeset (almost surely a different bucket for a random vec).
	if NewLSH(16, 8, 42, 1).Bucket(v) != q.Bucket(v) {
		t.Fatal("same seed should reproduce the same bucket")
	}
	probe := q.Probe(v, 5)
	if len(probe) != 5 || probe[0] != q.Bucket(v) {
		t.Fatalf("probe should start with the base bucket and have 5 entries, got %v", probe)
	}
}

// clusteredCorpus generates nClusters Gaussian blobs on the unit sphere — the realistic embedding
// shape (vectors have angular structure), where a coarse quantizer can actually route.
func clusteredCorpus(rng *rand.Rand, dim, nClusters, perCluster int, spread float64) (data [][]float32, centers [][]float32) {
	centers = make([][]float32, nClusters)
	for c := range centers {
		centers[c] = randUnit(rng, dim)
	}
	for c := 0; c < nClusters; c++ {
		for i := 0; i < perCluster; i++ {
			v := append([]float32(nil), centers[c]...)
			for j := range v {
				v[j] += float32(rng.NormFloat64() * spread)
			}
			data = append(data, normalize(v))
		}
	}
	return data, centers
}

// recall@nprobe over clustered data: the fraction of queries whose true nearest neighbour shares one
// of the query's probed buckets. A useful quantizer puts the NN in a probed bucket most of the time.
func recall(t *testing.T, q Quantizer, data [][]float32, centers [][]float32, nprobe int) float64 {
	t.Helper()
	rng := rand.New(rand.NewSource(99))
	bucket := make([]uint32, len(data))
	for i := range data {
		bucket[i] = q.Bucket(data[i])
	}
	hits, total := 0, 0
	for trial := 0; trial < 300; trial++ {
		c := centers[rng.Intn(len(centers))]
		query := append([]float32(nil), c...)
		for j := range query {
			query[j] += float32(rng.NormFloat64() * 0.03)
		}
		query = normalize(query)
		best, bestDot := -1, math.Inf(-1)
		for i, d := range data {
			if dd := dot(query, d); dd > bestDot {
				bestDot, best = dd, i
			}
		}
		probed := map[uint32]bool{}
		for _, b := range q.Probe(query, nprobe) {
			probed[b] = true
		}
		if probed[bucket[best]] {
			hits++
		}
		total++
	}
	return float64(hits) / float64(total)
}

func TestLSHRecallImprovesWithNprobe(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	data, centers := clusteredCorpus(rng, 32, 20, 100, 0.05)
	q := NewLSH(32, 8, 1, 1)
	r1 := recall(t, q, data, centers, 1)
	r16 := recall(t, q, data, centers, 16)
	t.Logf("LSH recall@1=%.2f recall@16=%.2f", r1, r16)
	if r16 < r1 {
		t.Errorf("recall should not decrease with nprobe: r@1=%.2f r@16=%.2f", r1, r16)
	}
	if r16 < 0.5 {
		t.Errorf("LSH recall@16 on clustered data unexpectedly low: %.2f", r16)
	}
}

func TestIVFTrainsAndRoutes(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	dim := 16
	// three well-separated clusters
	var sample [][]float32
	centers := [][]float32{randUnit(rng, dim), randUnit(rng, dim), randUnit(rng, dim)}
	for c := 0; c < 3; c++ {
		for i := 0; i < 200; i++ {
			v := append([]float32(nil), centers[c]...)
			for j := range v {
				v[j] += float32(rng.NormFloat64() * 0.02)
			}
			sample = append(sample, normalize(v))
		}
	}
	q := TrainIVF(sample, 3, 10, false, 9, 1)
	if q.NumBuckets() != 3 {
		t.Fatalf("expected 3 centroids, got %d", q.NumBuckets())
	}
	// points from the same cluster should mostly land in the same centroid's bucket
	r := recall(t, q, sample, centers, 1)
	t.Logf("IVF recall@1=%.2f", r)
	if r < 0.8 {
		t.Errorf("IVF recall@1 on 3 separated clusters should be high, got %.2f", r)
	}
	if q.Version() != 1 || q.Kind() != "ivf" {
		t.Fatalf("unexpected version/kind: %d %s", q.Version(), q.Kind())
	}
}
