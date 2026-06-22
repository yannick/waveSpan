package observability

import (
	"testing"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func TestTapEmitsDecodedSummaries(t *testing.T) {
	ring := NewGossipRing(64)
	ch, cancel := ring.Subscribe(nil, false)
	defer cancel()
	tap := NewGossipTap(ring)

	tap.Ping("node2", wavespanv1.GossipDirection_GOSSIP_SEND, 1.5, 42)
	tap.StateChange("node3", wavespanv1.GossipKind_GOSSIP_SUSPECT, "SUSPECT")
	tap.HolderSummary("node2", wavespanv1.GossipDirection_GOSSIP_RECV, 100, 500, 256)

	events := drain(ch, 5)
	if len(events) != 3 {
		t.Fatalf("expected 3 tapped events, got %d", len(events))
	}
	if events[0].GetRecord().GetSummary().GetRttMs() != 1.5 {
		t.Fatal("ping rtt summary missing")
	}
	if events[1].GetRecord().GetKind() != wavespanv1.GossipKind_GOSSIP_SUSPECT || events[1].GetRecord().GetSummary().GetNewState() != "SUSPECT" {
		t.Fatalf("state-change summary wrong: %+v", events[1].GetRecord())
	}
	hs := events[2].GetRecord().GetSummary()
	if hs.GetWatermark() != 100 || hs.GetApproxCount() != 500 {
		t.Fatalf("holder summary decoded fields wrong: %+v", hs)
	}
}

func TestNilTapIsSafe(t *testing.T) {
	var tap *GossipTap
	tap.Ping("x", wavespanv1.GossipDirection_GOSSIP_SEND, 1, 1) // must not panic
}
