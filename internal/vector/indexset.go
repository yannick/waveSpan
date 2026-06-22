package vector

import (
	"sync"

	"github.com/cwire/wavespan/internal/vector/ann"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// IndexSet holds a live ANN index per configured vector index and routes raw-vector writes (local
// ingest or globally applied) into the matching live indexes (design/08). The ANN is derived; only
// raw records are authoritative and replicated.
type IndexSet struct {
	mu          sync.RWMutex
	metaByName  map[string]*IndexMeta
	liveByName  map[string]*LiveIndex
	namesByColl map[string][]string
}

// NewIndexSet builds a live index per metadata entry.
func NewIndexSet(metas []*IndexMeta, params ann.Params) *IndexSet {
	s := &IndexSet{metaByName: map[string]*IndexMeta{}, liveByName: map[string]*LiveIndex{}, namesByColl: map[string][]string{}}
	for _, m := range metas {
		s.metaByName[m.Name] = m
		s.liveByName[m.Name] = NewLiveIndex(m.Metric, params)
		s.namesByColl[m.Collection] = append(s.namesByColl[m.Collection], m.Name)
	}
	return s
}

// Meta resolves index metadata by name.
func (s *IndexSet) Meta(name string) (*IndexMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.metaByName[name]
	return m, ok
}

// Live resolves the live index by name.
func (s *IndexSet) Live(name string) (*LiveIndex, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.liveByName[name]
	return l, ok
}

// OnWrite applies a raw vector write to every index over its collection: an insert makes it
// immediately searchable via the delta; a tombstone hides it.
func (s *IndexSet) OnWrite(rec *wavespanv1.VectorRecord) {
	s.mu.RLock()
	names := s.namesByColl[rec.GetCollection()]
	lives := make([]*LiveIndex, 0, len(names))
	for _, n := range names {
		lives = append(lives, s.liveByName[n])
	}
	s.mu.RUnlock()
	for _, live := range lives {
		if rec.GetTombstone() {
			live.Delete(rec.GetVectorId())
		} else {
			live.Insert(rec.GetVectorId(), rec.GetValues())
		}
	}
}
