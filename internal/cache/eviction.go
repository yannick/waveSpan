package cache

import (
	"context"
	"time"
)

// Evictor drops dynamic cache replicas that have gone idle past the idle TTL (design/05 "Cache
// eviction": idle-subscription TTL). It only ever evicts cache replicas — durable replicas are
// not in the cache index, so they are never touched by this loop.
type Evictor struct {
	store   *Store
	idleTTL time.Duration
	nowMs   func() int64
}

// NewEvictor builds an evictor over a cache store.
func NewEvictor(store *Store, idleTTL time.Duration, nowMs func() int64) *Evictor {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	if idleTTL <= 0 {
		idleTTL = 10 * time.Minute
	}
	return &Evictor{store: store, idleTTL: idleTTL, nowMs: nowMs}
}

// EvictIdle evicts all cache replicas idle longer than the idle TTL and returns the count evicted.
func (e *Evictor) EvictIdle() int {
	cutoff := e.nowMs() - e.idleTTL.Milliseconds()
	n := 0
	for _, ent := range e.store.idleBefore(cutoff) {
		if err := e.store.Evict(ent.namespace, ent.key); err == nil {
			n++
		}
	}
	return n
}

// Run evicts idle caches on the given interval until ctx is done.
func (e *Evictor) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.EvictIdle()
		}
	}
}
