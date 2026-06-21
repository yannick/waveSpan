package membership

import (
	"context"
	"hash/fnv"
	"net/http"
	"time"

	"github.com/cwire/wavespan/internal/latencygraph"
)

// ServiceConfig tunes the membership service.
type ServiceConfig struct {
	GossipInterval time.Duration
	Liveness       LivenessConfig
	Gossip         GossipConfig
	Graph          latencygraph.Config
}

// DefaultServiceConfig returns sane defaults.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		GossipInterval: time.Second,
		Liveness:       DefaultLivenessConfig(),
		Gossip:         DefaultGossipConfig(),
		Graph:          latencygraph.DefaultConfig(),
	}
}

// Service is the public membership facade: it owns the roster, latency graph, gossip driver,
// and the Connect gossip handler, and runs the periodic gossip loop.
type Service struct {
	roster    *Roster
	graph     *latencygraph.Graph
	gossip    *Gossip
	server    *GossipConnectServer
	transport *ConnectTransport
	cfg       ServiceConfig
}

// NewService wires a membership service for the local member over the given transport.
func NewService(self Member, disc Discovery, transport *ConnectTransport, cfg ServiceConfig) *Service {
	roster := NewRoster(self, cfg.Liveness)
	graph := latencygraph.New(cfg.Graph)
	gossip := NewGossip(roster, graph, transport, disc, cfg.Gossip, time.Now, seedFor(self.MemberID))
	server := NewGossipConnectServer(gossip, transport)
	return &Service{roster: roster, graph: graph, gossip: gossip, server: server, transport: transport, cfg: cfg}
}

// GossipHandler returns the mountable Connect handler (path, handler) for the gossip port.
func (s *Service) GossipHandler() (string, http.Handler) { return s.server.Handler() }

// SetHolderHooks installs the holder-summary provider/consumer so the cache directory's bloom
// rides gossip (design/04 "Holder summaries").
func (s *Service) SetHolderHooks(provide func() HolderSummaryWire, consume func(HolderSummaryWire)) {
	s.gossip.SetHolderHooks(provide, consume)
}

// Run joins the cluster via seeds, then gossips on the configured interval until ctx is done.
func (s *Service) Run(ctx context.Context) {
	s.gossip.Join(ctx)
	t := time.NewTicker(s.cfg.GossipInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gossip.Tick(ctx)
			s.graph.Expire(time.Now())
		}
	}
}

// Self returns the local member.
func (s *Service) Self() Member { return s.roster.Self() }

// Members returns the current roster (excluding forgotten members).
func (s *Service) Members() []MemberView { return s.roster.Members() }

// Live returns the currently-ALIVE members.
func (s *Service) Live() []MemberView { return s.roster.Live() }

// LatencyEdges returns the local latency-graph edges.
func (s *Service) LatencyEdges() []latencygraph.Edge { return s.graph.Edges() }

// Graph returns the latency graph for placement scoring (M3).
func (s *Service) Graph() *latencygraph.Graph { return s.graph }

// seedFor derives a stable RNG seed from the member id so peer selection is reproducible.
func seedFor(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64())
}
