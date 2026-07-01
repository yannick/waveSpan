package membership

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// recordingObserver counts the gossip-tap events the driver emits.
type recordingObserver struct {
	mu                      sync.Mutex
	pings, edges, summaries int
}

func (o *recordingObserver) Ping(_ string, _ wavespanv1.GossipDirection, _ float64, _ uint32) {
	o.mu.Lock()
	o.pings++
	o.mu.Unlock()
}

func (o *recordingObserver) HolderSummary(_ string, _ wavespanv1.GossipDirection, _, _ uint64, _ uint32) {
	o.mu.Lock()
	o.summaries++
	o.mu.Unlock()
}

func (o *recordingObserver) LatencyEdge(_ string, _, _ float64) {
	o.mu.Lock()
	o.edges++
	o.mu.Unlock()
}

// TestGossipObserverEmitsLiveTraffic is the regression for the dead gossip inspector: a probing node
// must emit ping, latency-edge, and holder-summary events every round, not only on a liveness
// transition.
func TestGossipObserverEmitsLiveTraffic(t *testing.T) {
	net := newMemNetwork()
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}

	mkNode := func(id string, seedAddr string) *Gossip {
		self := Member{ClusterID: "dev", MemberID: id, NodeName: id, GossipAddr: id + ":7700"}
		r := NewRoster(self, fastLiveness(), 0)
		g := latencygraph.New(latencygraph.DefaultConfig())
		tr := &memTransport{net: net, selfAddr: self.GossipAddr}
		gs := NewGossip(r, g, tr, staticSeeds{seedAddr}, DefaultGossipConfig(), clock.now, 7)
		net.register(self.GossipAddr, gs)
		return gs
	}
	n1 := mkNode("node1", "node2:7700")
	n2 := mkNode("node2", "node1:7700")
	// node2 advertises a holder summary so node1 receives one on the probe reply.
	n2.SetHolderHooks(func() HolderSummaryWire {
		return HolderSummaryWire{MemberID: "node2", Bloom: []byte{1, 2, 3, 4}, GeneratedAtUnixMs: 99}
	}, nil)

	obs := &recordingObserver{}
	n1.SetObserver(obs)

	ctx := context.Background()
	n1.Join(ctx)
	n2.Join(ctx)
	for i := 0; i < 5; i++ {
		clock.advance(200 * time.Millisecond)
		n1.Tick(ctx) // node1 probes node2 (the only candidate)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.pings == 0 {
		t.Fatal("expected ping events from probes, got none (inspector would be empty)")
	}
	if obs.edges == 0 {
		t.Fatal("expected latency-edge events after successful probes")
	}
	if obs.summaries == 0 {
		t.Fatal("expected holder-summary events from the peer's piggybacked summary")
	}
}
