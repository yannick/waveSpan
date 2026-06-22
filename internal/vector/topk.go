package vector

import (
	"container/heap"
	"sort"
)

// Hit is one vector search result.
type Hit struct {
	Collection  string
	VectorID    string
	GraphNodeID string
	Score       float64 // larger = more similar (user-facing)
	Distance    float64 // smaller = more similar (ranking)
}

// maxHeap orders hits by Distance with the largest at the root, so the worst of the k kept is
// cheapest to evict.
type maxHeap []Hit

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(Hit)) }
func (h *maxHeap) Pop() any {
	old := *h
	n := len(old)
	v := old[n-1]
	*h = old[:n-1]
	return v
}

// TopK keeps the k smallest-distance hits seen.
type TopK struct {
	k int
	h maxHeap
}

// NewTopK builds a bounded top-k accumulator.
func NewTopK(k int) *TopK { return &TopK{k: k} }

// Add offers a hit; it is kept only if it is among the k closest so far.
func (t *TopK) Add(hit Hit) {
	if t.k <= 0 {
		return
	}
	if t.h.Len() < t.k {
		heap.Push(&t.h, hit)
		return
	}
	if hit.Distance < t.h[0].Distance {
		t.h[0] = hit
		heap.Fix(&t.h, 0)
	}
}

// Result returns the kept hits sorted by ascending distance (closest first).
func (t *TopK) Result() []Hit {
	out := make([]Hit, len(t.h))
	copy(out, t.h)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Distance != out[j].Distance {
			return out[i].Distance < out[j].Distance
		}
		return out[i].VectorID < out[j].VectorID // stable tie-break
	})
	return out
}
