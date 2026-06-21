package latencygraph

import (
	"context"
	"time"
)

// ProbeFunc measures round-trip time to a gossip address. It returns the RTT on success or an
// error on timeout/failure.
type ProbeFunc func(ctx context.Context, gossipAddr string) (time.Duration, error)

// Prober times probes and records the result into the graph. It rides the gossip tick
// (design/04 "Gossip protocol": ping, measure RTT, update latency graph).
type Prober struct {
	graph *Graph
	probe ProbeFunc
	now   func() time.Time
}

// NewProber builds a Prober. A nil clock uses time.Now.
func NewProber(g *Graph, probe ProbeFunc, now func() time.Time) *Prober {
	if now == nil {
		now = time.Now
	}
	return &Prober{graph: g, probe: probe, now: now}
}

// Probe measures RTT to a member's gossip address and records the sample. It returns true on a
// successful probe.
func (p *Prober) Probe(ctx context.Context, memberID, gossipAddr string) bool {
	rtt, err := p.probe(ctx, gossipAddr)
	p.graph.AddSample(memberID, rtt, err == nil, p.now())
	return err == nil
}
