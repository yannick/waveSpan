package vector

import (
	"sync"

	"github.com/yannick/wavespan/internal/vector/ann"
)

// Segment is an immutable, published ANN segment over a fixed vector set (design/08 "ANN segment").
// Readers are refcounted so a retired segment is GC'd only once no query references it.
type Segment struct {
	index ann.Index
	vecs  map[string][]float32

	mu     sync.Mutex
	refs   int
	retire bool
	gced   bool
}

func buildSegment(metric Metric, params ann.Params, vecs map[string][]float32) *Segment {
	idx := ann.NewHNSW(metricDist(metric), params)
	owned := make(map[string][]float32, len(vecs))
	for id, v := range vecs {
		cp := make([]float32, len(v))
		copy(cp, v)
		owned[id] = cp
		idx.Insert(id, cp)
	}
	return &Segment{index: idx, vecs: owned}
}

// Acquire registers an in-flight reader.
func (s *Segment) Acquire() {
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
}

// Release drops a reader; a retired segment with no readers is GC'd.
func (s *Segment) Release() {
	s.mu.Lock()
	s.refs--
	if s.retire && s.refs == 0 {
		s.gced = true
		s.index = nil
		s.vecs = nil
	}
	s.mu.Unlock()
}

// GCed reports whether the segment has been garbage-collected.
func (s *Segment) GCed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gced
}

func (s *Segment) markRetire() {
	s.mu.Lock()
	s.retire = true
	if s.refs == 0 {
		s.gced = true
		s.index = nil
		s.vecs = nil
	}
	s.mu.Unlock()
}

// LiveIndex ties an immutable main segment to a mutable delta: writes go to the delta for immediate
// visibility, and Merge folds the delta into a freshly published segment (design/08).
type LiveIndex struct {
	mu     sync.RWMutex
	metric Metric
	params ann.Params
	main   *Segment
	delta  *Delta
}

// NewLiveIndex builds an empty live index.
func NewLiveIndex(metric Metric, params ann.Params) *LiveIndex {
	return &LiveIndex{
		metric: metric, params: params,
		main:  buildSegment(metric, params, nil),
		delta: NewDelta(metric),
	}
}

// Insert makes a vector immediately searchable via the delta.
func (li *LiveIndex) Insert(id string, vec []float32) { li.delta.Insert(id, vec) }

// Delete tombstones a vector via the delta.
func (li *LiveIndex) Delete(id string) { li.delta.Delete(id) }

// Main returns the current main segment.
func (li *LiveIndex) Main() *Segment {
	li.mu.RLock()
	defer li.mu.RUnlock()
	return li.main
}

// Len reports the number of live vectors (main minus delta tombstones, plus delta inserts).
func (li *LiveIndex) Len() int {
	li.mu.RLock()
	main := li.main
	li.mu.RUnlock()
	live := 0
	for id := range main.vecs {
		if !li.delta.Tombstoned(id) {
			live++
		}
	}
	return live + li.delta.Len()
}

// Search queries the main segment and the delta, with delta entries (inserts and tombstones)
// overriding the main segment.
func (li *LiveIndex) Search(query []float32, k, efSearch int) []ann.Candidate {
	li.mu.RLock()
	main := li.main
	li.mu.RUnlock()
	main.Acquire()
	defer main.Release()

	pad := k + k // over-fetch so post-filter still yields k
	byID := map[string]ann.Candidate{}
	for _, c := range li.delta.Search(query, pad) {
		byID[c.ID] = c // delta inserts win
	}
	if main.index != nil {
		for _, c := range main.index.Search(query, pad, efSearch) {
			if li.delta.Tombstoned(c.ID) {
				continue
			}
			if _, ok := byID[c.ID]; ok {
				continue // superseded by a delta insert
			}
			byID[c.ID] = c
		}
	}
	tk := NewTopK(k)
	for _, c := range byID {
		tk.Add(Hit{VectorID: c.ID, Distance: c.Distance})
	}
	hits := tk.Result()
	out := make([]ann.Candidate, len(hits))
	for i, h := range hits {
		out[i] = ann.Candidate{ID: h.VectorID, Distance: h.Distance}
	}
	return out
}

// Merge folds the delta into a new main segment and retires the old one (background merge). It
// returns the retired segment (nil if unchanged).
func (li *LiveIndex) Merge() *Segment {
	inserts, tombs := li.delta.Drain()
	if len(inserts) == 0 && len(tombs) == 0 {
		return nil
	}
	li.mu.Lock()
	old := li.main
	newVecs := make(map[string][]float32, len(old.vecs)+len(inserts))
	for id, v := range old.vecs {
		if !tombs[id] {
			newVecs[id] = v
		}
	}
	for id, v := range inserts {
		newVecs[id] = v
	}
	li.main = buildSegment(li.metric, li.params, newVecs)
	li.mu.Unlock()
	old.markRetire()
	return old
}
