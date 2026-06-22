// Package ann provides the Index abstraction and a pure-Go HNSW implementation (ADR 0006).
// cgo is prohibited project-wide (CGO_ENABLED=0); the interface is the only seam, so a future
// out-of-process backend can implement it without an in-process cgo binding.
package ann

import "github.com/cwire/wavespan/internal/vector"

// Candidate is one ANN search result (smaller Distance / larger Score = closer).
type Candidate struct {
	ID       string
	Distance float64
	Score    float64
}

// Index is an approximate nearest-neighbor index over vectors of a fixed metric.
type Index interface {
	// Insert adds or replaces a vector by id.
	Insert(id string, vec []float32)
	// Delete removes a vector by id (subsequent searches must not return it).
	Delete(id string)
	// Search returns up to k approximate nearest neighbors; efSearch widens the candidate list
	// (0 = the index default).
	Search(query []float32, k, efSearch int) []Candidate
	// Len reports the number of live vectors.
	Len() int
}

// BruteForce is an exact Index used as a test double and a small-collection fallback.
type BruteForce struct {
	metric vector.Metric
	vecs   map[string][]float32
}

// NewBruteForce builds an exact reference index.
func NewBruteForce(metric vector.Metric) *BruteForce {
	return &BruteForce{metric: metric, vecs: map[string][]float32{}}
}

// Insert adds or replaces a vector.
func (b *BruteForce) Insert(id string, vec []float32) {
	cp := make([]float32, len(vec))
	copy(cp, vec)
	b.vecs[id] = cp
}

// Delete removes a vector.
func (b *BruteForce) Delete(id string) { delete(b.vecs, id) }

// Len reports the number of vectors.
func (b *BruteForce) Len() int { return len(b.vecs) }

// Search returns the exact top-k (the oracle).
func (b *BruteForce) Search(query []float32, k, _ int) []Candidate {
	tk := vector.NewTopK(k)
	for id, v := range b.vecs {
		tk.Add(vector.Hit{VectorID: id, Distance: vector.Distance(b.metric, query, v), Score: vector.Score(b.metric, query, v)})
	}
	hits := tk.Result()
	out := make([]Candidate, len(hits))
	for i, h := range hits {
		out[i] = Candidate{ID: h.VectorID, Distance: h.Distance, Score: h.Score}
	}
	return out
}
