package vector

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func approx(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

func TestDistanceMetrics(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0} // orthogonal
	c := []float32{1, 0, 0} // identical to a
	if !approx(Score(Cosine, a, b), 0, 1e-9) {
		t.Fatalf("orthogonal cosine should be 0, got %v", Score(Cosine, a, b))
	}
	if !approx(Score(Cosine, a, c), 1, 1e-9) {
		t.Fatalf("identical cosine should be 1, got %v", Score(Cosine, a, c))
	}
	if !approx(Score(Dot, a, c), 1, 1e-9) {
		t.Fatalf("dot of unit vectors should be 1, got %v", Score(Dot, a, c))
	}
	if !approx(Distance(L2, a, b), math.Sqrt2, 1e-6) {
		t.Fatalf("L2 between orthogonal unit vectors should be sqrt2, got %v", Distance(L2, a, b))
	}
}

func TestSimdMatchesScalar(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 50; trial++ {
		n := 1 + rng.Intn(64)
		a := make([]float32, n)
		b := make([]float32, n)
		for i := range a {
			a[i] = rng.Float32()*2 - 1
			b[i] = rng.Float32()*2 - 1
		}
		if !approx(dotProduct(a, b), dotScalar(a, b), 1e-4) {
			t.Fatalf("simd dot %v != scalar %v", dotProduct(a, b), dotScalar(a, b))
		}
	}
}

func TestTopKHeapKeepsBest(t *testing.T) {
	tk := NewTopK(3)
	for _, d := range []float64{5, 1, 4, 2, 9, 3} {
		tk.Add(Hit{VectorID: "v", Distance: d})
	}
	got := tk.Result()
	if len(got) != 3 || got[0].Distance != 1 || got[1].Distance != 2 || got[2].Distance != 3 {
		t.Fatalf("top-3 smallest distances wrong: %+v", got)
	}
}

func mkVec(id string, vals ...float32) *wavespanv1.VectorRecord {
	return &wavespanv1.VectorRecord{Collection: "c", VectorId: id, Values: vals, Dimensions: uint32(len(vals))}
}

func bruteForce(vecs []*wavespanv1.VectorRecord, query []float32, k int, metric Metric) []string {
	type sc struct {
		id string
		d  float64
	}
	var all []sc
	for _, v := range vecs {
		all = append(all, sc{v.GetVectorId(), Distance(metric, query, v.GetValues())})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].d != all[j].d {
			return all[i].d < all[j].d
		}
		return all[i].id < all[j].id
	})
	var out []string
	for i := 0; i < k && i < len(all); i++ {
		out = append(out, all[i].id)
	}
	return out
}

func ids(hits []Hit) []string {
	out := make([]string, len(hits))
	for i, h := range hits {
		out[i] = h.VectorID
	}
	return out
}

func TestExactSearchMatchesBruteForce(t *testing.T) {
	vecs := []*wavespanv1.VectorRecord{
		mkVec("a", 1, 0), mkVec("b", 0, 1), mkVec("c", 0.9, 0.1), mkVec("d", -1, 0), mkVec("e", 0.5, 0.5),
	}
	query := []float32{1, 0}
	for _, m := range []Metric{Cosine, Dot, L2} {
		got := ids(SearchPartition(vecs, query, 3, m, nil))
		want := bruteForce(vecs, query, 3, m)
		if !eqStr(got, want) {
			t.Fatalf("metric %v: exact = %v, want %v", m, got, want)
		}
	}
}

func TestExactDistributedMerge(t *testing.T) {
	vecs := []*wavespanv1.VectorRecord{
		mkVec("a", 1, 0), mkVec("b", 0, 1), mkVec("c", 0.9, 0.1), mkVec("d", -1, 0),
		mkVec("e", 0.5, 0.5), mkVec("f", 0.8, 0.2), mkVec("g", 0.1, 0.9),
	}
	query := []float32{1, 0}
	// split across 3 fragments
	frags := [][]*wavespanv1.VectorRecord{vecs[0:3], vecs[3:5], vecs[5:7]}
	var local [][]Hit
	for _, f := range frags {
		local = append(local, SearchPartition(f, query, 3, Cosine, nil))
	}
	merged := ids(MergeTopK(local, 3))
	want := bruteForce(vecs, query, 3, Cosine)
	if !eqStr(merged, want) {
		t.Fatalf("distributed merge = %v, want %v", merged, want)
	}
}

func TestExactFilterExcludes(t *testing.T) {
	vecs := []*wavespanv1.VectorRecord{mkVec("a", 1, 0), mkVec("b", 0.95, 0.05), mkVec("c", 0, 1)}
	// filter out "b"
	got := ids(SearchPartition(vecs, []float32{1, 0}, 3, Cosine, func(v *wavespanv1.VectorRecord) bool {
		return v.GetVectorId() != "b"
	}))
	for _, id := range got {
		if id == "b" {
			t.Fatal("filtered vector should be excluded")
		}
	}
}

func TestParseVectorIndexSpec(t *testing.T) {
	meta, err := ParseVectorIndexSpec(IndexSpec{Name: "doc-embedding", Dimensions: 8, Metric: "cosine", ExactEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if meta.Metric != Cosine || meta.Dimensions != 8 || !meta.ExactEnabled || meta.Collection != "doc-embedding" {
		t.Fatalf("parsed meta wrong: %+v", meta)
	}
	if _, err := ParseVectorIndexSpec(IndexSpec{Name: "bad", Dimensions: 0}); err == nil {
		t.Fatal("dimensions 0 must be rejected (CRD validation)")
	}
}

func eqStr(a, b []string) bool {
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
