package membership

import (
	"context"
	"hash/fnv"
	"time"

	"google.golang.org/grpc"

	"github.com/yannick/wavespan/internal/latencygraph"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// ServiceConfig tunes the membership service.
type ServiceConfig struct {
	GossipInterval time.Duration
	// ReseedInterval re-resolves and re-contacts the configured seeds periodically. Static-seed
	// discovery only contacts seeds once at startup, so a node that came up isolated (seeds not yet
	// resolvable — e.g. a simultaneous cold deploy) would stay fragmented forever. Re-seeding lets it
	// re-discover the cluster. Cheap (a few pings), and a no-op merge when already converged.
	ReseedInterval time.Duration
	Liveness       LivenessConfig
	Gossip         GossipConfig
	Graph          latencygraph.Config
	// SelfIncarnation seeds this node's SWIM incarnation. It MUST be monotonic across restarts of the same
	// MemberID so a restarted node's new address propagates (see NewRoster). Production sets boot-time
	// unix-millis; 0 (the default) preserves test behavior.
	SelfIncarnation uint64
}

// DefaultServiceConfig returns sane defaults.
func DefaultServiceConfig() ServiceConfig {
	return ServiceConfig{
		GossipInterval: time.Second,
		ReseedInterval: 30 * time.Second,
		Liveness:       DefaultLivenessConfig(),
		Gossip:         DefaultGossipConfig(),
		Graph:          latencygraph.DefaultConfig(),
	}
}

// Service is the public membership facade: it owns the roster, latency graph, gossip driver,
// and the gRPC gossip handler, and runs the periodic gossip loop.
type Service struct {
	roster    *Roster
	graph     *latencygraph.Graph
	gossip    *Gossip
	server    *GossipGRPCServer
	transport *GRPCTransport
	cfg       ServiceConfig
}

// NewService wires a membership service for the local member over the given transport.
func NewService(self Member, disc Discovery, transport *GRPCTransport, cfg ServiceConfig) *Service {
	roster := NewRoster(self, cfg.Liveness, cfg.SelfIncarnation)
	graph := latencygraph.New(cfg.Graph)
	gossip := NewGossip(roster, graph, transport, disc, cfg.Gossip, time.Now, seedFor(self.MemberID))
	server := NewGossipGRPCServer(gossip, transport)
	return &Service{roster: roster, graph: graph, gossip: gossip, server: server, transport: transport, cfg: cfg}
}

// RegisterGRPC registers the gossip service on the given gRPC server registrar (the dedicated
// gossip-port gRPC server).
func (s *Service) RegisterGRPC(reg grpc.ServiceRegistrar) {
	wavespanv1.RegisterGossipServiceServer(reg, s.server)
}

// SetStateObserver installs a liveness-transition observer (for the observability gossip tap, M13).
func (s *Service) SetStateObserver(fn func(memberID string, newState State)) {
	s.roster.SetStateObserver(fn)
}

// SetGossipObserver installs the gossip-event tap so probes, latency-edge updates, and holder
// summaries surface in the gossip inspector (design/26).
func (s *Service) SetGossipObserver(o GossipObserver) {
	s.gossip.SetObserver(o)
}

// SetHolderHooks installs the holder-summary provider/consumer so the cache directory's bloom
// rides gossip (design/04 "Holder summaries").
func (s *Service) SetHolderHooks(provide func() HolderSummaryWire, consume func(HolderSummaryWire)) {
	s.gossip.SetHolderHooks(provide, consume)
}

// SetConfigHooks installs the runtime config-override provider/consumer so tunable changes ride
// gossip and converge cluster-wide (LWW). See internal/tunables.Overrides.
func (s *Service) SetConfigHooks(provide func() []ConfigDeltaWire, consume func([]ConfigDeltaWire)) {
	s.gossip.SetConfigHooks(provide, consume)
}

// SetBucketHooks installs the held-bucket provider/consumer so vector bucket advertisements ride
// gossip and feed the kNN routing directory (design/29).
func (s *Service) SetBucketHooks(provide func() []HeldBucketWire, consume func([]HeldBucketWire)) {
	s.gossip.SetBucketHooks(provide, consume)
}

// Run joins the cluster via seeds, then gossips on the configured interval until ctx is done.
func (s *Service) Run(ctx context.Context) {
	s.gossip.Join(ctx)
	t := time.NewTicker(s.cfg.GossipInterval)
	defer t.Stop()
	reseed := s.cfg.ReseedInterval
	if reseed <= 0 {
		reseed = 30 * time.Second
	}
	rt := time.NewTicker(reseed)
	defer rt.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gossip.Tick(ctx)
			s.graph.Expire(time.Now())
		case <-rt.C:
			// Re-resolve + re-contact seeds so an isolated node re-discovers the cluster.
			s.gossip.Join(ctx)
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
