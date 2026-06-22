package ann

import (
	"container/heap"
	"math"
	"math/rand"
	"sync"
)

// HNSW is a pure-Go Hierarchical Navigable Small World index (ADR 0006). It is safe for concurrent
// use. Deleted ids are tombstoned (kept as graph waypoints but never returned); a rebuild compacts.
type HNSW struct {
	mu sync.RWMutex

	dist            DistanceFunc
	m               int // neighbors per insert
	mMax0           int // max neighbors at layer 0
	efConstruction  int
	efSearchDefault int
	mL              float64

	nodes      []*hnode
	idToIdx    map[string]int32
	entryPoint int32
	maxLevel   int
	live       int
	rng        *rand.Rand
}

type hnode struct {
	id      string
	vec     []float32
	conns   [][]int32 // conns[level] = neighbor indices
	deleted bool
}

// Params configure an HNSW index.
type Params struct {
	M               int
	EfConstruction  int
	EfSearchDefault int
	Seed            int64
}

// DefaultParams returns sensible HNSW parameters.
func DefaultParams() Params { return Params{M: 16, EfConstruction: 200, EfSearchDefault: 64, Seed: 1} }

// NewHNSW builds an empty HNSW index over a distance function (smaller = closer).
func NewHNSW(dist DistanceFunc, p Params) *HNSW {
	if p.M <= 0 {
		p.M = 16
	}
	if p.EfConstruction <= 0 {
		p.EfConstruction = 200
	}
	if p.EfSearchDefault <= 0 {
		p.EfSearchDefault = 64
	}
	return &HNSW{
		dist: dist, m: p.M, mMax0: 2 * p.M, efConstruction: p.EfConstruction,
		efSearchDefault: p.EfSearchDefault, mL: 1 / math.Log(float64(p.M)),
		idToIdx: map[string]int32{}, entryPoint: -1, rng: rand.New(rand.NewSource(p.Seed)),
	}
}

func (h *HNSW) distTo(q []float32, idx int32) float64 {
	return h.dist(q, h.nodes[idx].vec)
}

func (h *HNSW) randomLevel() int {
	l := int(-math.Log(h.rng.Float64()+1e-12) * h.mL)
	if l > 24 {
		l = 24
	}
	return l
}

// Len reports the number of live (non-deleted) vectors.
func (h *HNSW) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.live
}

// Delete tombstones a vector by id.
func (h *HNSW) Delete(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if idx, ok := h.idToIdx[id]; ok && !h.nodes[idx].deleted {
		h.nodes[idx].deleted = true
		h.live--
	}
}

// Insert adds or replaces a vector by id.
func (h *HNSW) Insert(id string, vec []float32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.idToIdx[id]; ok && !h.nodes[old].deleted {
		h.nodes[old].deleted = true // replace: tombstone old, insert fresh
		h.live--
	}
	cp := make([]float32, len(vec))
	copy(cp, vec)
	level := h.randomLevel()
	n := &hnode{id: id, vec: cp, conns: make([][]int32, level+1)}
	idx := int32(len(h.nodes))
	h.nodes = append(h.nodes, n)
	h.idToIdx[id] = idx
	h.live++

	if h.entryPoint == -1 {
		h.entryPoint = idx
		h.maxLevel = level
		return
	}

	ep := h.entryPoint
	// descend from the top down to level+1 using greedy ef=1 search
	for lc := h.maxLevel; lc > level; lc-- {
		ep = h.searchLayer(cp, ep, 1, lc)[0].idx
	}
	for lc := minInt(h.maxLevel, level); lc >= 0; lc-- {
		w := h.searchLayer(cp, ep, h.efConstruction, lc)
		mmax := h.m
		if lc == 0 {
			mmax = h.mMax0
		}
		neighbors := selectClosest(w, h.m)
		n.conns[lc] = neighbors
		for _, e := range neighbors {
			h.nodes[e].conns[lc] = append(h.nodes[e].conns[lc], idx)
			h.pruneConns(e, lc, mmax)
		}
		if len(w) > 0 {
			ep = w[0].idx
		}
	}
	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPoint = idx
	}
}

// pruneConns trims node e's neighbor list at layer lc to the mmax closest.
func (h *HNSW) pruneConns(e int32, lc, mmax int) {
	conns := h.nodes[e].conns[lc]
	if len(conns) <= mmax {
		return
	}
	cands := make([]elem, len(conns))
	for i, c := range conns {
		cands[i] = elem{idx: c, dist: h.dist(h.nodes[e].vec, h.nodes[c].vec)}
	}
	h.nodes[e].conns[lc] = selectClosest(cands, mmax)
}

// searchLayer returns up to ef nearest candidates to q at layer lc, sorted ascending by distance.
func (h *HNSW) searchLayer(q []float32, ep int32, ef, lc int) []elem {
	visited := map[int32]bool{ep: true}
	d0 := h.distTo(q, ep)
	cand := &minHeap{{ep, d0}}
	res := &maxHeap{{ep, d0}}
	heap.Init(cand)
	heap.Init(res)
	for cand.Len() > 0 {
		c := heap.Pop(cand).(elem)
		if res.Len() >= ef && c.dist > (*res)[0].dist {
			break
		}
		for _, nb := range h.nodes[c.idx].conns[lc] {
			if visited[nb] {
				continue
			}
			visited[nb] = true
			d := h.distTo(q, nb)
			if res.Len() < ef || d < (*res)[0].dist {
				heap.Push(cand, elem{nb, d})
				heap.Push(res, elem{nb, d})
				if res.Len() > ef {
					heap.Pop(res)
				}
			}
		}
	}
	out := make([]elem, res.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(res).(elem) // pop farthest first -> fill descending -> ascending order
	}
	return out
}

// Search returns up to k approximate nearest neighbors, skipping tombstoned nodes.
func (h *HNSW) Search(query []float32, k, efSearch int) []Candidate {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.entryPoint == -1 || k <= 0 {
		return nil
	}
	if efSearch <= 0 {
		efSearch = h.efSearchDefault
	}
	if efSearch < k {
		efSearch = k
	}
	ep := h.entryPoint
	for lc := h.maxLevel; lc > 0; lc-- {
		ep = h.searchLayer(query, ep, 1, lc)[0].idx
	}
	w := h.searchLayer(query, ep, efSearch, 0)
	out := make([]Candidate, 0, k)
	for _, e := range w {
		if h.nodes[e.idx].deleted {
			continue
		}
		out = append(out, Candidate{ID: h.nodes[e.idx].id, Distance: e.dist})
		if len(out) >= k {
			break
		}
	}
	return out
}

func selectClosest(cands []elem, m int) []int32 {
	if len(cands) > m {
		cands = cands[:m] // cands are already ascending by distance
	}
	out := make([]int32, len(cands))
	for i, c := range cands {
		out[i] = c.idx
	}
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- heaps over (idx, dist) ---

type elem struct {
	idx  int32
	dist float64
}

type minHeap []elem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(elem)) }
func (h *minHeap) Pop() any          { old := *h; n := len(old); v := old[n-1]; *h = old[:n-1]; return v }

type maxHeap []elem

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x any)        { *h = append(*h, x.(elem)) }
func (h *maxHeap) Pop() any          { old := *h; n := len(old); v := old[n-1]; *h = old[:n-1]; return v }
