package membership

import (
	"context"
	"math/rand"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
)

// HolderSummaryWire is a gossiped compact holder advertisement (design/04 "Holder summaries"):
// a bloom filter over the keys a member holds. The cache directory provides/consumes these.
type HolderSummaryWire struct {
	MemberID          string
	Bloom             []byte
	GeneratedAtUnixMs int64
}

// GossipMessage is the payload exchanged on a gossip round: the sender's identity, its membership
// delta, and its holder summary (piggybacked metadata, design/04 "Gossip protocol").
type GossipMessage struct {
	From      Member
	Members   []MemberView
	Summaries []HolderSummaryWire
}

// Transport carries gossip between nodes. The in-memory transport drives deterministic tests;
// the Connect/gRPC transport (M2.D) carries it between real nodes.
type Transport interface {
	// Ping sends a direct probe to a gossip address, returning the peer's gossip reply.
	Ping(ctx context.Context, addr string, msg *GossipMessage) (*GossipMessage, error)
	// IndirectPing asks relayAddr to probe targetAddr on our behalf (SWIM indirect probe).
	IndirectPing(ctx context.Context, relayAddr, targetAddr string, msg *GossipMessage) (*GossipMessage, error)
}

// GossipConfig tunes the gossip driver.
type GossipConfig struct {
	IndirectFanout int // number of relays for an indirect probe (SWIM k)
}

// DefaultGossipConfig returns sane defaults.
func DefaultGossipConfig() GossipConfig { return GossipConfig{IndirectFanout: 3} }

// Gossip is the SWIM-style membership driver. It probes peers, measures RTT into the latency
// graph, exchanges membership deltas, and advances liveness timeouts.
type Gossip struct {
	roster    *Roster
	graph     *latencygraph.Graph
	transport Transport
	discovery Discovery
	cfg       GossipConfig
	rng       *rand.Rand
	now       func() time.Time

	provideSummary func() HolderSummaryWire
	consumeSummary func(HolderSummaryWire)
}

// SetHolderHooks installs the holder-summary provider (this node's summary, gossiped outbound)
// and consumer (peers' summaries, fed to the holder directory). Either may be nil.
func (g *Gossip) SetHolderHooks(provide func() HolderSummaryWire, consume func(HolderSummaryWire)) {
	g.provideSummary = provide
	g.consumeSummary = consume
}

func (g *Gossip) consumeSummaries(ss []HolderSummaryWire) {
	if g.consumeSummary == nil {
		return
	}
	for _, s := range ss {
		g.consumeSummary(s)
	}
}

// NewGossip wires a gossip driver. A nil clock uses time.Now; rngSeed makes peer selection
// deterministic in tests.
func NewGossip(r *Roster, g *latencygraph.Graph, t Transport, d Discovery, cfg GossipConfig, now func() time.Time, rngSeed int64) *Gossip {
	if now == nil {
		now = time.Now
	}
	if cfg.IndirectFanout <= 0 {
		cfg.IndirectFanout = 3
	}
	return &Gossip{roster: r, graph: g, transport: t, discovery: d, cfg: cfg, rng: rand.New(rand.NewSource(rngSeed)), now: now}
}

// HandleGossip processes an incoming gossip message: it merges the sender's identity and delta
// into the local roster and returns the local roster delta in reply.
func (g *Gossip) HandleGossip(in *GossipMessage) *GossipMessage {
	now := g.now()
	g.roster.Upsert(in.From, now)
	g.roster.ObserveAck(in.From.MemberID, now)
	for _, mv := range in.Members {
		g.roster.ApplyGossip(mv, now)
	}
	g.consumeSummaries(in.Summaries)
	return g.outgoing()
}

// Join contacts the discovery seeds to bootstrap the roster (design/04 "Docker discovery").
func (g *Gossip) Join(ctx context.Context) {
	msg := g.outgoing()
	for _, addr := range g.discovery.Seeds() {
		if reply, err := g.transport.Ping(ctx, addr, msg); err == nil {
			g.merge(reply)
		}
	}
}

// Tick runs one gossip round: probe a random peer, measure RTT, exchange deltas, fall back to
// indirect probing and suspicion on failure, then advance liveness timeouts.
func (g *Gossip) Tick(ctx context.Context) {
	peer, ok := g.selectPeer()
	if ok {
		g.probe(ctx, peer)
	}
	g.roster.Tick(g.now())
}

func (g *Gossip) probe(ctx context.Context, peer MemberView) {
	msg := g.outgoing()
	start := time.Now()
	reply, err := g.transport.Ping(ctx, peer.Member.GossipAddr, msg)
	if err == nil {
		g.graph.AddSample(peer.Member.MemberID, time.Since(start), true, g.now())
		g.roster.ObserveAck(peer.Member.MemberID, g.now())
		g.merge(reply)
		return
	}
	g.graph.AddSample(peer.Member.MemberID, 0, false, g.now())

	// SWIM indirect probe via k random live relays before declaring suspicion.
	for _, relay := range g.selectRelays(peer.Member.MemberID) {
		if reply, err := g.transport.IndirectPing(ctx, relay.Member.GossipAddr, peer.Member.GossipAddr, msg); err == nil {
			g.roster.ObserveAck(peer.Member.MemberID, g.now())
			g.merge(reply)
			return
		}
	}
	g.roster.Suspect(peer.Member.MemberID, g.now())
}

func (g *Gossip) merge(reply *GossipMessage) {
	if reply == nil {
		return
	}
	now := g.now()
	g.roster.Upsert(reply.From, now)
	g.roster.ObserveAck(reply.From.MemberID, now)
	for _, mv := range reply.Members {
		g.roster.ApplyGossip(mv, now)
	}
	g.consumeSummaries(reply.Summaries)
}

// outgoing builds the local gossip delta plus this node's holder summary.
func (g *Gossip) outgoing() *GossipMessage {
	msg := &GossipMessage{From: g.roster.Self(), Members: g.roster.Members()}
	if g.provideSummary != nil {
		msg.Summaries = []HolderSummaryWire{g.provideSummary()}
	}
	return msg
}

// selectPeer picks a random non-self member worth probing (alive, suspect, or unreachable — to
// re-confirm), skipping dead/forgotten members.
func (g *Gossip) selectPeer() (MemberView, bool) {
	self := g.roster.Self().MemberID
	var cand []MemberView
	for _, m := range g.roster.Members() {
		if m.Member.MemberID == self || m.State == StateDead {
			continue
		}
		cand = append(cand, m)
	}
	if len(cand) == 0 {
		return MemberView{}, false
	}
	return cand[g.rng.Intn(len(cand))], true
}

// selectRelays picks up to IndirectFanout random live members other than self and the target.
func (g *Gossip) selectRelays(targetID string) []MemberView {
	self := g.roster.Self().MemberID
	var cand []MemberView
	for _, m := range g.roster.Live() {
		if m.Member.MemberID == self || m.Member.MemberID == targetID {
			continue
		}
		cand = append(cand, m)
	}
	g.rng.Shuffle(len(cand), func(i, j int) { cand[i], cand[j] = cand[j], cand[i] })
	if len(cand) > g.cfg.IndirectFanout {
		cand = cand[:g.cfg.IndirectFanout]
	}
	return cand
}
