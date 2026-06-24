package grpcsrv

import (
	"context"

	"github.com/yannick/wavespan/internal/observability"
	"github.com/yannick/wavespan/internal/tunables"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Config is the gRPC ConfigService adapter (peer-reachable, data-port). It mirrors the Connect
// ConfigServer in internal/observability, delegating to the SAME exported cores: the live tunables
// Registry (read via observability.NodeConfigProto) and the Overrides manager (node-local pins). Only
// the transport (gRPC vs Connect) differs.
type Config struct {
	wavespanv1.UnimplementedConfigServiceServer
	reg       *tunables.Registry
	overrides *tunables.Overrides
	clusterID string
	memberID  string
}

// NewConfig wires the gRPC ConfigService adapter over the same dependencies the Connect ConfigServer
// takes (see observability.NewConfigServer).
func NewConfig(reg *tunables.Registry, overrides *tunables.Overrides, clusterID, memberID string) *Config {
	return &Config{reg: reg, overrides: overrides, clusterID: clusterID, memberID: memberID}
}

// GetConfig returns this node's effective tunable set.
func (s *Config) GetConfig(_ context.Context, _ *wavespanv1.GetConfigRequest) (*wavespanv1.NodeConfig, error) {
	return observability.NodeConfigProto(s.reg, s.overrides, s.clusterID, s.memberID), nil
}

// SetTunable pins a node-local override on this node (no gossip). Errors are returned in-band in the
// response's Error field (mirroring the Connect ConfigServer), not as a gRPC status.
func (s *Config) SetTunable(_ context.Context, m *wavespanv1.SetTunableRequest) (*wavespanv1.SetTunableResponse, error) {
	if s.overrides == nil {
		return &wavespanv1.SetTunableResponse{Error: "runtime config not enabled on this node"}, nil
	}
	version, requiresRestart, err := s.overrides.Set(m.GetKey(), m.GetValue(), true)
	if err != nil {
		return &wavespanv1.SetTunableResponse{Error: err.Error()}, nil
	}
	return &wavespanv1.SetTunableResponse{Ok: true, Version: version, RequiresRestart: requiresRestart}, nil
}
