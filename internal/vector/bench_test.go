package vector

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/cwire/wavespan/internal/vector/ann"
)

func randVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

// buildIndex inserts n random dim-vectors and merges them into the main HNSW segment.
func buildIndex(b *testing.B, n, dim int) (*LiveIndex, []float32) {
	b.Helper()
	rng := rand.New(rand.NewSource(1))
	li := NewLiveIndex(Cosine, ann.DefaultParams())
	for i := 0; i < n; i++ {
		li.Insert(strconv.Itoa(i), randVec(rng, dim))
	}
	li.Merge() // fold the delta into the HNSW segment (the steady state after the merger runs)
	return li, randVec(rng, dim)
}

// BenchmarkLiveIndexSearch measures ANN query latency over a merged index at a few scales — the hot
// path for VectorSearch's per-node fragment (SearchLocal).
func BenchmarkLiveIndexSearch(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 50_000} {
		li, q := buildIndex(b, n, 128)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = li.Search(q, 10, 64)
			}
		})
	}
}

// BenchmarkLiveIndexInsert measures delta-insert throughput (the write hot path before merge).
func BenchmarkLiveIndexInsert(b *testing.B) {
	rng := rand.New(rand.NewSource(2))
	li := NewLiveIndex(Cosine, ann.DefaultParams())
	vecs := make([][]float32, b.N)
	for i := range vecs {
		vecs[i] = randVec(rng, 128)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		li.Insert(strconv.Itoa(i), vecs[i])
	}
}

// BenchmarkMerge measures folding a delta of recent inserts into a fresh segment (the merger tick).
func BenchmarkMerge(b *testing.B) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		li := NewLiveIndex(Cosine, ann.DefaultParams())
		for j := 0; j < 1000; j++ {
			li.Insert(strconv.Itoa(j), randVec(rng, 128))
		}
		b.StartTimer()
		li.Merge()
	}
}

// BenchmarkVecHash measures the embedding→id derivation on the write path.
func BenchmarkVecHash(b *testing.B) {
	rng := rand.New(rand.NewSource(4))
	v := randVec(rng, 768)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = VecHash(v)
	}
}
