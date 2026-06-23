// Package ttl implements WaveSpan's lazy, best-effort TTL: read-time hide-expired and a
// background sweeper that emits tombstones for expired keys (design/03 "TTL semantics",
// design/02 "TTL storage"). TTL is approximate: there is no promise all nodes detect expiry
// simultaneously, and expired records must not break conflict convergence.
package ttl

import (
	"context"
	"time"

	"github.com/yannick/wavespan/internal/recordstore"
)

// Expired reports whether a record with the given expiry (nil = none) is expired at nowMs.
// Best-effort hide-expired applies on every read; strict namespaces behave identically here.
func Expired(expiresAtMs *int64, nowMs int64) bool {
	return expiresAtMs != nil && *expiresAtMs <= nowMs
}

// TombstoneFunc emits a coordinated delete (tombstone) that replicates and participates in
// conflict resolution like any other delete (the KV coordinator's Delete, adapted by the node).
type TombstoneFunc func(ctx context.Context, namespace string, key []byte) error

// Source supplies due ttl entries and current expiry (satisfied by recordstore.Store).
type Source interface {
	ExpiredEntries(nowMs int64, limit int) ([]recordstore.ExpiredEntry, error)
	ExpiresAt(namespace string, key []byte) (int64, bool)
	ClearTTLIndex(indexKey []byte) error
}

// Sweeper scans due ttl buckets and tombstones keys whose current version has expired, then
// clears the index entry. It does not repair expired keys (design/05 "Repair loop").
type Sweeper struct {
	store     Source
	tombstone TombstoneFunc
	nowMs     func() int64
	batch     int
}

// NewSweeper builds a sweeper.
func NewSweeper(store Source, tombstone TombstoneFunc, nowMs func() int64) *Sweeper {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &Sweeper{store: store, tombstone: tombstone, nowMs: nowMs, batch: 256}
}

// SweepOnce processes one batch of due ttl entries and returns how many tombstones it emitted.
func (s *Sweeper) SweepOnce(ctx context.Context) int {
	now := s.nowMs()
	entries, err := s.store.ExpiredEntries(now, s.batch)
	if err != nil {
		return 0
	}
	emitted := 0
	for _, e := range entries {
		// only tombstone if the CURRENT winning version is still expired (it may have been
		// overwritten with a new, longer TTL since the index entry was written).
		if exp, ok := s.store.ExpiresAt(e.Namespace, e.Key); ok && exp <= now {
			if s.tombstone != nil {
				if err := s.tombstone(ctx, e.Namespace, e.Key); err == nil {
					emitted++
				}
			}
		}
		_ = s.store.ClearTTLIndex(e.IndexKey)
	}
	return emitted
}

// Run sweeps on the given interval until ctx is done.
func (s *Sweeper) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SweepOnce(ctx)
		}
	}
}
