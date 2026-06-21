package cache

import (
	"sync"

	"github.com/cwire/wavespan/internal/recordstore"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// Store holds dynamic cache replicas (design/05 "Replica types"). A cache replica is persisted in
// the local record store so the next Get is served locally, but it is flagged derived: it is
// never recorded as a durable holder, never counts toward write ACK or target-N (ADR 0003), and
// is disposable (the evictor in M5.B may drop it).
type Store struct {
	rec    *recordstore.Store
	mu     sync.RWMutex
	cached map[string]int64 // keyID -> last-access unix ms (cache replicas only)
	nowMs  func() int64
}

// NewStore wraps a record store with dynamic-cache bookkeeping.
func NewStore(rec *recordstore.Store, nowMs func() int64) *Store {
	return &Store{rec: rec, cached: map[string]int64{}, nowMs: nowMs}
}

func cacheKeyID(namespace string, key []byte) string { return namespace + "\x00" + string(key) }

// Put stores a fetched record as a dynamic cache replica and serves it locally thereafter.
func (s *Store) Put(rec *wavespanv1.StoredRecord) error {
	kind := wavespanv1.MutationKind_MUTATION_KIND_PUT
	if rec.GetTombstone() {
		kind = wavespanv1.MutationKind_MUTATION_KIND_DELETE
	}
	if _, err := s.rec.Apply(rec, kind); err != nil {
		return err
	}
	s.mu.Lock()
	s.cached[cacheKeyID(rec.GetNamespace(), rec.GetLogicalKey())] = s.nowMs()
	s.mu.Unlock()
	return nil
}

// IsCacheReplica reports whether (namespace, key) is held as a dynamic cache replica (not a
// durable replica). It also refreshes the last-access time for the eviction loop.
func (s *Store) IsCacheReplica(namespace string, key []byte) bool {
	id := cacheKeyID(namespace, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cached[id]; ok {
		s.cached[id] = s.nowMs()
		return true
	}
	return false
}

// Promote marks a cache replica as durable (it became a durable holder), so it is no longer
// subject to the dynamic-cache evictor (design/05 "Cache eviction": never evict durable replicas).
func (s *Store) Promote(namespace string, key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cached, cacheKeyID(namespace, key))
}
