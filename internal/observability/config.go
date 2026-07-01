package observability

import (
	"context"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/tunables"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// NodeConfigProto snapshots the live tunables registry into the wire NodeConfig (effective value +
// provenance + override scope + docs per tunable). overrides may be nil (no scope annotation).
func NodeConfigProto(reg *tunables.Registry, overrides *tunables.Overrides, clusterID, memberID string) *wavespanv1.NodeConfig {
	nc := &wavespanv1.NodeConfig{ClusterId: clusterID, MemberId: memberID}
	for _, p := range reg.All() {
		ts := &wavespanv1.TunableState{
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
		}
		if overrides != nil {
			ts.OverrideScope = overrides.Scope(p.Key)
		}
		nc.Tunables = append(nc.Tunables, ts)
	}
	return nc
}

// --- admin (UI-facing) handlers on ObservabilityService -----------------------------------------

// ConfigFetcher reads a peer's effective config over its data-port ConfigService.
type ConfigFetcher func(ctx context.Context, target membership.Member) (*wavespanv1.NodeConfig, error)

// ConfigSetter pins a node-local override on a peer over its data-port ConfigService.
type ConfigSetter func(ctx context.Context, target membership.Member, key, value string) (*wavespanv1.SetTunableResponse, error)

// WithTunables enables GetNodeConfig + AdminSetTunable. fetch forwards reads to a peer; set forwards
// a node-local pin to a peer. Either forwarder may be nil to limit those operations to this node.
func (s *ObsService) WithTunables(reg *tunables.Registry, overrides *tunables.Overrides, fetch ConfigFetcher, set ConfigSetter) *ObsService {
	s.tunables = reg
	s.overrides = overrides
	s.configFetch = fetch
	s.configSet = set
	return s
}

// GetNodeConfig returns the effective config of this node, or forwards to the requested member.
func (s *ObsService) GetNodeConfig(ctx context.Context, req *connect.Request[wavespanv1.GetNodeConfigRequest]) (*connect.Response[wavespanv1.NodeConfig], error) {
	if s.tunables == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errConfigDisabled)
	}
	target := req.Msg.GetTargetMemberId()
	if target == "" || target == s.self.MemberID {
		return connect.NewResponse(NodeConfigProto(s.tunables, s.overrides, s.self.ClusterID, s.self.MemberID)), nil
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

// AdminSetTunable applies a runtime override. cluster_wide=true sets it on this node and gossips it
// to every node (LWW). cluster_wide=false pins it on target_member_id only (empty = serving node)
// without gossiping.
func (s *ObsService) AdminSetTunable(ctx context.Context, req *connect.Request[wavespanv1.AdminSetTunableRequest]) (*connect.Response[wavespanv1.AdminSetTunableResponse], error) {
	if s.overrides == nil {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: "runtime config not enabled on this node"}), nil
	}
	m := req.Msg

	if m.GetClusterWide() {
		version, requiresRestart, err := s.overrides.Set(m.GetKey(), m.GetValue(), false) // gossips
		if err != nil {
			return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: err.Error()}), nil
		}
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Ok: true, Version: version, RequiresRestart: requiresRestart}), nil
	}

	// Node-local pin. Apply on this node, or forward to the chosen peer's ConfigService.
	target := m.GetTargetMemberId()
	if target == "" || target == s.self.MemberID {
		version, requiresRestart, err := s.overrides.Set(m.GetKey(), m.GetValue(), true)
		if err != nil {
			return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: err.Error()}), nil
		}
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Ok: true, Version: version, RequiresRestart: requiresRestart}), nil
	}
	peer, ok := s.resolveTarget(target)
	if !ok {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: "unknown target member: " + target}), nil
	}
	if s.configSet == nil {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: "node-local set forwarding not enabled"}), nil
	}
	r, err := s.configSet(ctx, peer, m.GetKey(), m.GetValue())
	if err != nil {
		return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Error: err.Error()}), nil
	}
	return connect.NewResponse(&wavespanv1.AdminSetTunableResponse{Ok: r.GetOk(), Error: r.GetError(), Version: r.GetVersion(), RequiresRestart: r.GetRequiresRestart()}), nil
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
