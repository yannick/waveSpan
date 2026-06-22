package ann

import (
	"math/rand"
	"testing"

	"github.com/cwire/wavespan/internal/vector"
)

func TestANNInterfaceContract(t *testing.T) {
	var idx Index = NewBruteForce(vector.Cosine)
	idx.Insert("a", []float32{1, 0})
	idx.Insert("b", []float32{0, 1})
	idx.Insert("c", []float32{0.9, 0.1})
	if idx.Len() != 3 {
		t.Fatalf("len = %d", idx.Len())
	}
	res := idx.Search([]float32{1, 0}, 2, 0)
	if len(res) != 2 || res[0].ID != "a" || res[1].ID != "c" {
		t.Fatalf("search wrong: %+v", res)
	}
	idx.Delete("a")
	if idx.Len() != 2 || idx.Search([]float32{1, 0}, 1, 0)[0].ID == "a" {
		t.Fatal("delete not honored")
	}
}

func randVecs(n, dim int, seed int64) ([]string, [][]float32) {
	rng := rand.New(rand.NewSource(seed))
	ids := make([]string, n)
	vecs := make([][]float32, n)
	for i := 0; i < n; i++ {
		ids[i] = "v" + itoa(i)
		v := make([]float32, dim)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		vecs[i] = v
	}
	return ids, vecs
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

func recallAt(approx []Candidate, exact []Candidate, k int) float64 {
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

func TestHNSWRecallVsExact(t *testing.T) {
	const n, dim, k = 3000, 16, 10
	ids, vecs := randVecs(n, dim, 7)
	exact := NewBruteForce(vector.Cosine)
	h := NewHNSW(vector.Cosine, Params{M: 16, EfConstruction: 200, EfSearchDefault: 128, Seed: 3})
	for i := range ids {
		exact.Insert(ids[i], vecs[i])
		h.Insert(ids[i], vecs[i])
	}
	_, queries := randVecs(20, dim, 99)
	var total float64
	for _, q := range queries {
		r := recallAt(h.Search(q, k, 200), exact.Search(q, k, 0), k)
		total += r
	}
	avg := total / float64(len(queries))
	if avg < 0.95 {
		t.Fatalf("HNSW recall@%d = %.3f, want >= 0.95", k, avg)
	}
}

func TestHNSWParamsHonored(t *testing.T) {
	const n, dim, k = 2000, 16, 10
	ids, vecs := randVecs(n, dim, 11)
	exact := NewBruteForce(vector.Cosine)
	h := NewHNSW(vector.Cosine, Params{M: 8, EfConstruction: 100, EfSearchDefault: 16, Seed: 5})
	for i := range ids {
		exact.Insert(ids[i], vecs[i])
		h.Insert(ids[i], vecs[i])
	}
	_, queries := randVecs(15, dim, 123)
	avg := func(ef int) float64 {
		var total float64
		for _, q := range queries {
			total += recallAt(h.Search(q, k, ef), exact.Search(q, k, 0), k)
		}
		return total / float64(len(queries))
	}
	low, high := avg(10), avg(300)
	if high < low {
		t.Fatalf("higher efSearch should not reduce recall: ef10=%.3f ef300=%.3f", low, high)
	}
}

func TestHNSWDelete(t *testing.T) {
	h := NewHNSW(vector.Cosine, DefaultParams())
	ids, vecs := randVecs(200, 8, 1)
	for i := range ids {
		h.Insert(ids[i], vecs[i])
	}
	target := h.Search(vecs[0], 1, 64)[0].ID
	h.Delete(target)
	for _, c := range h.Search(vecs[0], 10, 128) {
		if c.ID == target {
			t.Fatal("deleted id returned by search")
		}
	}
}
