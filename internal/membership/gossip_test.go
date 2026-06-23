package membership

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/latencygraph"
)

// fakeClock is a controllable monotonic clock for deterministic timeout tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// staticSeeds is a fixed-list Discovery for tests.
type staticSeeds []string

func (s staticSeeds) Seeds() []string { return []string(s) }

// memNetwork wires gossip handlers by address and can mark nodes down.
type memNetwork struct {
	mu       sync.Mutex
	handlers map[string]*Gossip
	down     map[string]bool
}

func newMemNetwork() *memNetwork {
	return &memNetwork{handlers: map[string]*Gossip{}, down: map[string]bool{}}
}
func (n *memNetwork) register(addr string, g *Gossip) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[addr] = g
}
func (n *memNetwork) kill(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[addr] = true
}
func (n *memNetwork) reach(addr string) (*Gossip, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[addr] {
		return nil, false
	}
	h, ok := n.handlers[addr]
	return h, ok
}

type memTransport struct {
	net      *memNetwork
	selfAddr string
}

var errUnreachable = errors.New("unreachable")

func (t *memTransport) Ping(_ context.Context, addr string, msg *GossipMessage) (*GossipMessage, error) {
	if _, ok := t.net.reach(t.selfAddr); !ok {
		return nil, errUnreachable
	}
	h, ok := t.net.reach(addr)
	if !ok {
		return nil, errUnreachable
	}
	return h.HandleGossip(msg), nil
}

func (t *memTransport) IndirectPing(_ context.Context, relayAddr, targetAddr string, msg *GossipMessage) (*GossipMessage, error) {
	if _, ok := t.net.reach(relayAddr); !ok {
		return nil, errUnreachable // relay itself is unreachable
	}
	h, ok := t.net.reach(targetAddr)
	if !ok {
		return nil, errUnreachable // relay could not reach target
	}
	return h.HandleGossip(msg), nil
}

func fastLiveness() LivenessConfig {
	return LivenessConfig{SuspicionTimeout: 2 * time.Second, UnreachableTimeout: 4 * time.Second, DeadRetention: time.Minute}
}

func TestGossipThreeNodeFormUpThenKill(t *testing.T) {
	net := newMemNetwork()
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	ids := []string{"node1", "node2", "node3"}
	nodes := map[string]*Gossip{}

	for i, id := range ids {
		self := Member{ClusterID: "dev", MemberID: id, NodeName: id, GossipAddr: id + ":7700"}
		r := NewRoster(self, fastLiveness())
		g := latencygraph.New(latencygraph.DefaultConfig())
		seeds := staticSeeds{"node1:7700"}
		if id == "node1" {
			seeds = staticSeeds{"node2:7700"}
		}
		tr := &memTransport{net: net, selfAddr: self.GossipAddr}
		gs := NewGossip(r, g, tr, seeds, DefaultGossipConfig(), clock.now, int64(i+1))
		net.register(self.GossipAddr, gs)
		nodes[id] = gs
	}

	ctx := context.Background()
	for _, g := range nodes {
		g.Join(ctx)
	}

	// steady-state gossip until membership converges
	for round := 0; round < 30; round++ {
		clock.advance(200 * time.Millisecond)
		for _, id := range ids {
			nodes[id].Tick(ctx)
		}
	}

	// every node sees all three ALIVE
	for _, id := range ids {
		live := nodes[id].roster.Live()
		if len(live) != 3 {
			t.Fatalf("%s sees %d live members, want 3: %v", id, len(live), liveIDs(live))
		}
	}
	// latency edges are visible (TS-022 "edges visible")
	if len(nodes["node1"].graph.Edges()) == 0 {
		t.Fatal("node1 has no latency edges after gossip")
	}

	// kill node3; survivors must mark it SUSPECT then UNREACHABLE
	net.kill("node3:7700")
	for round := 0; round < 60; round++ {
		clock.advance(500 * time.Millisecond)
		nodes["node1"].Tick(ctx)
		nodes["node2"].Tick(ctx)
	}

	for _, id := range []string{"node1", "node2"} {
		v, ok := nodes[id].roster.Get("node3")
		if !ok {
			t.Fatalf("%s lost node3 entirely", id)
		}
		if v.State < StateSuspect {
			t.Fatalf("%s should mark node3 SUSPECT/UNREACHABLE after kill, got %s", id, v.State)
		}
	}
}

func liveIDs(vs []MemberView) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = v.Member.MemberID
	}
	return out
}
