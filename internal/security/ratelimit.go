package security

import (
	"sync"
	"time"
)

// PeerRateLimiter is a per-peer token-bucket rate limiter for inbound replication apply (design/15
// "Per-peer rate limits"). It bounds the apply rate a single peer can impose.
type PeerRateLimiter struct {
	mu         sync.Mutex
	ratePerSec float64
	burst      float64
	buckets    map[string]*bucket
	now        func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewPeerRateLimiter builds a limiter allowing ratePerSec sustained with the given burst.
func NewPeerRateLimiter(ratePerSec, burst float64) *PeerRateLimiter {
	return &PeerRateLimiter{ratePerSec: ratePerSec, burst: burst, buckets: map[string]*bucket{}, now: time.Now}
}

// Allow reports whether one apply from peer is permitted now, consuming a token if so.
func (l *PeerRateLimiter) Allow(peer string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[peer]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[peer] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	b.tokens += elapsed * l.ratePerSec
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
