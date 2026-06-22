package observability

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/tunables"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// NodeConfigProto snapshots the live tunables registry into the wire NodeConfig (effective value +
// provenance + docs per tunable). Powers the UI Config tab and cross-node config inspection.
func NodeConfigProto(reg *tunables.Registry, clusterID, memberID string) *wavespanv1.NodeConfig {
	nc := &wavespanv1.NodeConfig{ClusterId: clusterID, MemberId: memberID}
	for _, p := range reg.All() {
		nc.Tunables = append(nc.Tunables, &wavespanv1.TunableState{
			Key:          p.Key,
			Group:        p.Group,
			Value:        p.String(),
			DefaultValue: p.Default(),
			Source:       p.Source().String(),
			Kind:         p.Kind.String(),
			Category:     p.Category.String(),
			Doc:          p.Doc,
			Why:          p.Why,
			Version:      p.Version(),
			EnvVar:       tunables.EnvName(p.Key),
		})
	}
	return nc
}

// ConfigServer implements the peer-reachable ConfigService (mounted on the data port) so any node
// can read another node's effective config — the admin GetNodeConfig forwards here.
type ConfigServer struct {
	reg       *tunables.Registry
	clusterID string
	memberID  string
}

// NewConfigServer builds the data-port config server over the live registry.
func NewConfigServer(reg *tunables.Registry, clusterID, memberID string) *ConfigServer {
	return &ConfigServer{reg: reg, clusterID: clusterID, memberID: memberID}
}

// GetConfig returns this node's effective tunable set.
func (s *ConfigServer) GetConfig(_ context.Context, _ *connect.Request[wavespanv1.GetConfigRequest]) (*connect.Response[wavespanv1.NodeConfig], error) {
	return connect.NewResponse(NodeConfigProto(s.reg, s.clusterID, s.memberID)), nil
}

// Handler returns the Connect path + handler for mounting on the data-port mux.
func (s *ConfigServer) Handler() (string, http.Handler) {
	return wavespanv1connect.NewConfigServiceHandler(s)
}

// --- admin (UI-facing) handlers on ObservabilityService -----------------------------------------

// ConfigFetcher reads a peer's effective config over its data-port ConfigService (forwarder wired
// from the node's shared HTTP client).
type ConfigFetcher func(ctx context.Context, target membership.Member) (*wavespanv1.NodeConfig, error)

// WithTunables enables GetNodeConfig + AdminSetTunable: the registry is read for this node's config,
// the overrides manager applies + gossips runtime changes, and fetch forwards GetNodeConfig to a
// chosen peer (nil fetch limits GetNodeConfig to this node).
func (s *ObsService) WithTunables(reg *tunables.Registry, overrides *tunables.Overrides, fetch ConfigFetcher) *ObsService {
	s.tunables = reg
	s.overrides = overrides
	s.configFetch = fetch
	return s
}

// GetNodeConfig returns the effective config of this node, or forwards to the requested member.
func (s *ObsService) GetNodeConfig(ctx context.Context, req *connect.Request[wavespanv1.GetNodeConfigRequest]) (*connect.Response[wavespanv1.NodeConfig], error) {
	if s.tunables == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errConfigDisabled)
	}
	target := req.Msg.GetTargetMemberId()
	if target == "" || target == s.self.MemberID {
		return connect.NewResponse(NodeConfigProto(s.tunables, s.self.ClusterID, s.self.MemberID)), nil
	}
	m, ok := s.resolveTarget(target)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errUnknownMember(target))
	}
	if s.configFetch == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errConfigDisabled)
	}
	nc, err := s.configFetch(ctx, m)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(nc), nil
}

// AdminSetTunable sets a runtime override on this node and gossips it cluster-wide.
func (s *ObsService) AdminSetTunable(_ context.Context, req *connect.Request[wavespanv1.AdminSetTunableRequest]) (*connect.Response[wavespanv1.AdminSetTunableResponse], error) {
	if s.overrides == nil {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: "runtime config not enabled on this node"}), nil
	}
	m := req.Msg
	version, requiresRestart, err := s.overrides.Set(m.GetKey(), m.GetValue())
	if err != nil {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: err.Error()}), nil
	}
	return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Ok: true, Version: version, RequiresRestart: requiresRestart}), nil
}

// capFor reads a Hot integer cap from the live registry (so a runtime override applies on the next
// request), falling back to the compiled-in default when tunables aren't wired.
func (s *ObsService) capFor(key string, fallback int) int {
	if s.tunables != nil {
		if p := s.tunables.Get(key); p != nil {
			return p.Int()
		}
	}
	return fallback
}

type strErr string

func (e strErr) Error() string { return string(e) }

const errConfigDisabled strErr = "config introspection not enabled on this node"

func errUnknownMember(id string) error { return strErr("unknown target member: " + id) }
