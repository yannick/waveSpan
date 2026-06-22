// Package ann provides the Index abstraction and a pure-Go HNSW implementation (ADR 0006).
// cgo is prohibited project-wide (CGO_ENABLED=0); the interface is the only seam, so a future
// out-of-process backend can implement it without an in-process cgo binding.
//
// ann is metric-agnostic: callers supply a distance function (smaller = closer). This keeps the
// package free of any dependency on the vector package (avoiding an import cycle).
package ann

import "sort"

// DistanceFunc returns a distance between two vectors where smaller means more similar.
type DistanceFunc func(a, b []float32) float64

// Candidate is one ANN search result (smaller Distance = closer).
type Candidate struct {
	ID       string
	Distance float64
}

// Index is an approximate nearest-neighbor index.
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
	dist DistanceFunc
	vecs map[string][]float32
}

// NewBruteForce builds an exact reference index over a distance function.
func NewBruteForce(dist DistanceFunc) *BruteForce {
	return &BruteForce{dist: dist, vecs: map[string][]float32{}}
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
	all := make([]Candidate, 0, len(b.vecs))
	for id, v := range b.vecs {
		all = append(all, Candidate{ID: id, Distance: b.dist(query, v)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Distance != all[j].Distance {
			return all[i].Distance < all[j].Distance
		}
		return all[i].ID < all[j].ID
	})
	if k < len(all) {
		all = all[:k]
	}
	return all
}
