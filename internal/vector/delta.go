package vector

import (
	"sync"

	"github.com/cwire/wavespan/internal/vector/ann"
)

// Delta is the small, mutable, immediately-searchable index of recent inserts and deletes
// (design/08 "write-visible-with-delta"). A background merge folds it into the main segment.
type Delta struct {
	mu     sync.RWMutex
	metric Metric
	vecs   map[string][]float32
	tombs  map[string]bool
}

// NewDelta builds an empty delta index for a metric.
func NewDelta(metric Metric) *Delta {
	return &Delta{metric: metric, vecs: map[string][]float32{}, tombs: map[string]bool{}}
}

// Insert makes a vector immediately searchable.
func (d *Delta) Insert(id string, vec []float32) {
	cp := make([]float32, len(vec))
	copy(cp, vec)
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.tombs, id)
	d.vecs[id] = cp
}

// Delete records a tombstone that hides a vector (including one in a main segment).
func (d *Delta) Delete(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.vecs, id)
	d.tombs[id] = true
}

// Tombstoned reports whether the delta hides an id.
func (d *Delta) Tombstoned(id string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.tombs[id]
}

// Search brute-forces the delta's own inserts (it is small).
func (d *Delta) Search(query []float32, k int) []ann.Candidate {
	d.mu.RLock()
	defer d.mu.RUnlock()
	tk := NewTopK(k)
	for id, v := range d.vecs {
		tk.Add(Hit{VectorID: id, Distance: Distance(d.metric, query, v), Score: Score(d.metric, query, v)})
	}
	hits := tk.Result()
	out := make([]ann.Candidate, len(hits))
	for i, h := range hits {
		out[i] = ann.Candidate{ID: h.VectorID, Distance: h.Distance}
	}
	return out
}

// Len reports the number of delta inserts.
func (d *Delta) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.vecs)
}

// Drain returns the delta's inserts and tombstones and clears it (for merge).
func (d *Delta) Drain() (inserts map[string][]float32, tombstones map[string]bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	inserts, tombstones = d.vecs, d.tombs
	d.vecs = map[string][]float32{}
	d.tombs = map[string]bool{}
	return inserts, tombstones
}
