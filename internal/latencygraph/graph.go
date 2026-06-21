// Package latencygraph maintains a directed, time-decayed latency graph from the local node to
// its peers (design/04_membership_latency_gossip.md "Latency graph"). Measured RTT — not static
// topology labels — is the source of truth for replica placement and closest-holder reads.
package latencygraph

import (
	"sort"
	"sync"
	"time"
)

// Edge is a directed latency edge self->to.
type Edge struct {
	To            string
	EWMARttMs     float64
	P95RttMs      float64
	PacketLoss    float64 // EWMA of failed probes in [0,1]
	LastSuccessMs int64
	LastFailureMs int64
	SampleCount   uint32
}

// ScoreWeights are the placement-score coefficients (design/04 "Compute score").
type ScoreWeights struct {
	EWMA, P95, PacketLoss, LoadPressure, DiskPressure, Topology float64
}

// DefaultScoreWeights are the weights from design/04.
func DefaultScoreWeights() ScoreWeights {
	return ScoreWeights{EWMA: 0.55, P95: 0.15, PacketLoss: 0.10, LoadPressure: 0.10, DiskPressure: 0.05, Topology: 0.05}
}

// Config tunes the graph.
type Config struct {
	EWMAAlpha    float64       // smoothing factor in (0,1]
	RefMaxRttMs  float64       // RTT normalisation reference (rtt/ref clamped to 1)
	EdgeExpiry   time.Duration // edges with no sample within this window are dropped
	RecentWindow int           // number of recent RTT samples kept for the p95 estimate
	Weights      ScoreWeights
}

// DefaultConfig returns sane defaults.
func DefaultConfig() Config {
	return Config{
		EWMAAlpha:    0.2,
		RefMaxRttMs:  50,
		EdgeExpiry:   30 * time.Second,
		RecentWindow: 64,
		Weights:      DefaultScoreWeights(),
	}
}

type edgeState struct {
	Edge
	recent []float64
}

// Graph holds directed edges from the local member to peers. Safe for concurrent use.
type Graph struct {
	mu    sync.RWMutex
	cfg   Config
	edges map[string]*edgeState
}

// New builds an empty graph.
func New(cfg Config) *Graph {
	if cfg.EWMAAlpha <= 0 || cfg.EWMAAlpha > 1 {
		cfg.EWMAAlpha = 0.2
	}
	if cfg.RefMaxRttMs <= 0 {
		cfg.RefMaxRttMs = 50
	}
	if cfg.RecentWindow <= 0 {
		cfg.RecentWindow = 64
	}
	return &Graph{cfg: cfg, edges: map[string]*edgeState{}}
}

// AddSample records a probe result to peer `to`. A successful probe updates EWMA/p95 and
// last-success; a failed probe raises packet loss and updates last-failure.
func (g *Graph) AddSample(to string, rtt time.Duration, success bool, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e, ok := g.edges[to]
	if !ok {
		e = &edgeState{Edge: Edge{To: to}}
		g.edges[to] = e
	}
	e.SampleCount++
	a := g.cfg.EWMAAlpha

	if success {
		ms := float64(rtt) / float64(time.Millisecond)
		if e.SampleCount == 1 || e.EWMARttMs == 0 {
			e.EWMARttMs = ms
		} else {
			e.EWMARttMs = a*ms + (1-a)*e.EWMARttMs
		}
		e.recent = append(e.recent, ms)
		if len(e.recent) > g.cfg.RecentWindow {
			e.recent = e.recent[len(e.recent)-g.cfg.RecentWindow:]
		}
		e.P95RttMs = percentile(e.recent, 0.95)
		e.PacketLoss = a*0 + (1-a)*e.PacketLoss
		e.LastSuccessMs = now.UnixMilli()
	} else {
		e.PacketLoss = a*1 + (1-a)*e.PacketLoss
		e.LastFailureMs = now.UnixMilli()
	}
}

// Edge returns the edge to a peer.
func (g *Graph) Edge(to string) (Edge, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	e, ok := g.edges[to]
	if !ok {
		return Edge{}, false
	}
	return e.Edge, true
}

// Edges returns all edges, sorted by destination.
func (g *Graph) Edges() []Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]Edge, 0, len(g.edges))
	for _, e := range g.edges {
		out = append(out, e.Edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].To < out[j].To })
	return out
}

// Expire drops edges with no successful or failed sample within the expiry window (design/04
// "time-decayed"; stale edges must not influence placement).
func (g *Graph) Expire(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cutoff := now.Add(-g.cfg.EdgeExpiry).UnixMilli()
	for to, e := range g.edges {
		last := e.LastSuccessMs
		if e.LastFailureMs > last {
			last = e.LastFailureMs
		}
		if last < cutoff {
			delete(g.edges, to)
		}
	}
}

// normalize maps an RTT in ms to [0,1] against the reference max.
func (g *Graph) normalize(rttMs float64) float64 {
	v := rttMs / g.cfg.RefMaxRttMs
	if v > 1 {
		return 1
	}
	if v < 0 {
		return 0
	}
	return v
}

// Score combines the latency edge to a peer with pressure and topology penalties into a
// placement score where LOWER is closer/better (design/04 "Compute score"). loadPressure and
// diskPressure come from gossiped member metadata (0 until populated in later milestones);
// topologyPenalty is computed by the caller (placement, M3) from the topology table.
func (g *Graph) Score(to string, loadPressure, diskPressure, topologyPenalty float64) float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	w := g.cfg.Weights
	var ewma, p95, loss float64
	if e, ok := g.edges[to]; ok {
		ewma = g.normalize(e.EWMARttMs)
		p95 = g.normalize(e.P95RttMs)
		loss = e.PacketLoss
	} else {
		// no measurement yet: treat as worst-case latency so measured peers are preferred
		ewma, p95 = 1, 1
	}
	return w.EWMA*ewma + w.P95*p95 + w.PacketLoss*loss +
		w.LoadPressure*loadPressure + w.DiskPressure*diskPressure + w.Topology*topologyPenalty
}

// TopologyPenalty scores static topology distance between two members (design/04 "Topology
// penalty"). Lower is closer. Static labels are only a hint; measured RTT dominates the score.
// Same-node is handled by the placement hard filter, not here.
func TopologyPenalty(selfZone, selfRegion, selfGeo, peerZone, peerRegion, peerGeo string) float64 {
	if selfGeo == "" || peerGeo == "" || selfRegion == "" || peerRegion == "" {
		return 0.5 // unknown topology
	}
	switch {
	case selfGeo != peerGeo:
		return 1.0 // outside geo
	case selfRegion != peerRegion:
		return 0.4 // same geo, different region
	case selfZone != peerZone:
		return 0.2 // same region, different zone
	default:
		return 0 // same zone
	}
}

// percentile returns the q-quantile (0..1) of vals using nearest-rank on a sorted copy.
func percentile(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	c := append([]float64(nil), vals...)
	sort.Float64s(c)
	idx := int(q * float64(len(c)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(c) {
		idx = len(c) - 1
	}
	return c[idx]
}
