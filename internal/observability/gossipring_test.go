package observability

import (
	"testing"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func rec(kind wavespanv1.GossipKind, peer string, dir wavespanv1.GossipDirection) *wavespanv1.GossipRecord {
	return &wavespanv1.GossipRecord{Kind: kind, Peer: peer, Direction: dir, AtUnixMs: 1}
}

func drain(ch <-chan *wavespanv1.GossipEvent, n int) []*wavespanv1.GossipEvent {
	out := make([]*wavespanv1.GossipEvent, 0, n)
	for i := 0; i < n; i++ {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
	return out
}

func TestRingBackfillReplaysInOrder(t *testing.T) {
	r := NewGossipRing(16)
	for i := 0; i < 5; i++ {
		r.Emit(rec(wavespanv1.GossipKind_GOSSIP_PING, "p1", wavespanv1.GossipDirection_GOSSIP_SEND))
	}
	ch, cancel := r.Subscribe(nil, true)
	defer cancel()
	events := drain(ch, 5)
	if len(events) != 5 {
		t.Fatalf("backfill replayed %d, want 5", len(events))
	}
	var lastSeq uint64
	for i, e := range events {
		seq := e.GetRecord().GetSeq()
		if i > 0 && seq <= lastSeq {
			t.Fatalf("backfill out of order at %d", i)
		}
		lastSeq = seq
	}
}

func TestRingDropOldestEmitsOneGap(t *testing.T) {
	r := NewGossipRing(4096)
	ch, cancel := r.Subscribe(nil, false)
	defer cancel()
	// overflow the 512 subscriber buffer by 100 without reading -> 100 dropped
	for i := 0; i < 612; i++ {
		r.Emit(rec(wavespanv1.GossipKind_GOSSIP_PING, "p1", wavespanv1.GossipDirection_GOSSIP_SEND))
	}
	// the slow reader drains what buffered, freeing space
	got := drain(ch, 512)
	if len(got) != 512 {
		t.Fatalf("expected 512 buffered records, got %d", len(got))
	}
	// the next emit now has room to flush the accumulated gap marker first
	r.Emit(rec(wavespanv1.GossipKind_GOSSIP_PING, "p1", wavespanv1.GossipDirection_GOSSIP_SEND))
	ev := drain(ch, 2)
	if len(ev) == 0 || ev[0].GetGap() == nil {
		t.Fatalf("expected a gap marker first, got %+v", ev)
	}
	if ev[0].GetGap().GetDroppedCount() != 100 {
		t.Fatalf("gap dropped_count = %d, want 100", ev[0].GetGap().GetDroppedCount())
	}
}

func TestEmitNeverBlocks(t *testing.T) {
	r := NewGossipRing(4096)
	_, cancel := r.Subscribe(nil, false) // never read
	defer cancel()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			r.Emit(rec(wavespanv1.GossipKind_GOSSIP_ACK, "p", wavespanv1.GossipDirection_GOSSIP_RECV))
		}
		close(done)
	}()
	<-done // if Emit blocked on the full subscriber, this would hang
}

func TestFilterMatching(t *testing.T) {
	r := NewGossipRing(16)
	ch, cancel := r.Subscribe(&wavespanv1.GossipFilter{
		Kinds:     []wavespanv1.GossipKind{wavespanv1.GossipKind_GOSSIP_SUSPECT, wavespanv1.GossipKind_GOSSIP_ALIVE},
		Direction: wavespanv1.GossipDirection_GOSSIP_RECV,
	}, false)
	defer cancel()
	r.Emit(rec(wavespanv1.GossipKind_GOSSIP_PING, "p1", wavespanv1.GossipDirection_GOSSIP_RECV))    // wrong kind
	r.Emit(rec(wavespanv1.GossipKind_GOSSIP_SUSPECT, "p1", wavespanv1.GossipDirection_GOSSIP_SEND)) // wrong direction
	r.Emit(rec(wavespanv1.GossipKind_GOSSIP_SUSPECT, "p1", wavespanv1.GossipDirection_GOSSIP_RECV)) // match
	r.Emit(rec(wavespanv1.GossipKind_GOSSIP_ALIVE, "p2", wavespanv1.GossipDirection_GOSSIP_RECV))   // match
	events := drain(ch, 10)
	if len(events) != 2 {
		t.Fatalf("filter should pass 2 events, got %d", len(events))
	}
	for _, e := range events {
		k := e.GetRecord().GetKind()
		if k != wavespanv1.GossipKind_GOSSIP_SUSPECT && k != wavespanv1.GossipKind_GOSSIP_ALIVE {
			t.Fatalf("unexpected kind %v passed the filter", k)
		}
	}
}
