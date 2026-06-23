package observability

import (
	"sync"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// GossipRing is a bounded ring buffer of gossip records with filtered fan-out to live subscribers
// (design/26). Producing into the ring NEVER blocks the gossip agent; a subscriber that falls
// behind has its oldest events dropped and receives a single GapMarker (design/26 "Performance and
// resource bounds").
type GossipRing struct {
	mu      sync.Mutex
	buf     []*wavespanv1.GossipRecord
	size    int
	head    int
	count   int
	nextSeq uint64

	nextSub int
	subs    map[int]*gossipSub
}

type gossipSub struct {
	mu      sync.Mutex
	filter  *wavespanv1.GossipFilter
	ch      chan *wavespanv1.GossipEvent
	dropped uint64
	sinceMs int64
}

// NewGossipRing builds a ring of the given capacity (default 4096).
func NewGossipRing(size int) *GossipRing {
	if size <= 0 {
		size = 4096
	}
	return &GossipRing{buf: make([]*wavespanv1.GossipRecord, size), size: size, subs: map[int]*gossipSub{}}
}

// Emit records an event and fans it out to matching subscribers without blocking.
func (r *GossipRing) Emit(rec *wavespanv1.GossipRecord) {
	r.mu.Lock()
	rec.Seq = r.nextSeq
	r.nextSeq++
	if r.count < r.size {
		r.buf[(r.head+r.count)%r.size] = rec
		r.count++
	} else {
		r.buf[r.head] = rec
		r.head = (r.head + 1) % r.size
	}
	subs := make([]*gossipSub, 0, len(r.subs))
	for _, s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.Unlock()

	for _, s := range subs {
		if matchesFilter(s.filter, rec) {
			s.offer(rec)
		}
	}
}

// offer enqueues a record, flushing any pending gap first; on overflow it drops and accumulates a
// gap marker.
func (s *gossipSub) offer(rec *wavespanv1.GossipRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropped > 0 {
		gap := &wavespanv1.GossipEvent{Event: &wavespanv1.GossipEvent_Gap{Gap: &wavespanv1.GapMarker{DroppedCount: s.dropped, SinceUnixMs: s.sinceMs}}}
		select {
		case s.ch <- gap:
			s.dropped = 0
		default:
			s.dropped++ // still backed up; keep accumulating
			return
		}
	}
	select {
	case s.ch <- &wavespanv1.GossipEvent{Event: &wavespanv1.GossipEvent_Record{Record: rec}}:
	default:
		if s.dropped == 0 {
			s.sinceMs = rec.GetAtUnixMs()
		}
		s.dropped++
	}
}

// Subscribe returns a channel of events matching filter. When backfill is set, the matching ring
// contents are replayed (oldest->newest) before live tailing. cancel removes the subscriber.
func (r *GossipRing) Subscribe(filter *wavespanv1.GossipFilter, backfill bool) (<-chan *wavespanv1.GossipEvent, func()) {
	s := &gossipSub{filter: filter, ch: make(chan *wavespanv1.GossipEvent, 512)}
	r.mu.Lock()
	if backfill {
		for i := 0; i < r.count; i++ {
			rec := r.buf[(r.head+i)%r.size]
			if rec != nil && matchesFilter(filter, rec) {
				s.offer(rec) // non-blocking; oversized backfill drops with a gap
			}
		}
	}
	id := r.nextSub
	r.nextSub++
	r.subs[id] = s
	r.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			r.mu.Lock()
			delete(r.subs, id)
			r.mu.Unlock()
			close(s.ch)
		})
	}
	return s.ch, cancel
}

// matchesFilter applies a GossipFilter server-side (design/26).
func matchesFilter(f *wavespanv1.GossipFilter, rec *wavespanv1.GossipRecord) bool {
	if f == nil {
		return true
	}
	if len(f.GetKinds()) > 0 {
		ok := false
		for _, k := range f.GetKinds() {
			if k == rec.GetKind() {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(f.GetPeers()) > 0 {
		ok := false
		for _, p := range f.GetPeers() {
			if p == rec.GetPeer() {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if f.GetDirection() != wavespanv1.GossipDirection_GOSSIP_DIRECTION_UNSPECIFIED && f.GetDirection() != rec.GetDirection() {
		return false
	}
	return true
}
