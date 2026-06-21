// Package local implements WaveSpan's local replication: the origin+1 StoreReplica receiver and
// client, idempotent mutation dedupe, and (from M4) target-N fanout and repair.
package local

import (
	"sync"

	"github.com/cwire/wavespan/internal/version"
)

// Idempotency is a bounded dedupe cache keyed by mutation_id. A retried mutation with the same id
// resolves to one logical mutation (property 5, IMPLEMENTATION_STRATEGY.md section 3) on both the
// coordinator and the StoreReplica receiver.
type Idempotency struct {
	mu    sync.Mutex
	seen  map[string]version.Version
	order []string
	cap   int
}

// NewIdempotency builds a cache holding up to capacity recent mutation ids.
func NewIdempotency(capacity int) *Idempotency {
	if capacity <= 0 {
		capacity = 8192
	}
	return &Idempotency{seen: make(map[string]version.Version, capacity), cap: capacity}
}

// Check returns the version previously recorded for a mutation id, if present.
func (i *Idempotency) Check(mutationID string) (version.Version, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	v, ok := i.seen[mutationID]
	return v, ok
}

// Record stores the result of applying a mutation id, evicting the oldest entry when full.
func (i *Idempotency) Record(mutationID string, v version.Version) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, ok := i.seen[mutationID]; ok {
		return
	}
	if len(i.order) >= i.cap {
		oldest := i.order[0]
		i.order = i.order[1:]
		delete(i.seen, oldest)
	}
	i.seen[mutationID] = v
	i.order = append(i.order, mutationID)
}
