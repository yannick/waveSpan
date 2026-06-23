package membership

import (
	"strings"

	"github.com/yannick/wavespan/internal/config"
)

// Discovery yields the seed addresses a node contacts to join the cluster. The data node never
// depends on the Kubernetes API (design/README.md hard rule 2); Kubernetes discovery is wired
// at M11 as a separate provider.
type Discovery interface {
	// Seeds returns gossip seed addresses (host:port), excluding self where known.
	Seeds() []string
}

// DockerDiscovery is static seed discovery from WAVESPAN_SEEDS (design/04 "Docker discovery").
type DockerDiscovery struct {
	seeds []string
	self  string
}

// NewDockerDiscovery builds docker static-seed discovery from config, dropping the node's own
// gossip address from the seed list.
func NewDockerDiscovery(cfg *config.Config, selfGossipAddr string) *DockerDiscovery {
	out := make([]string, 0, len(cfg.Membership.Seeds))
	for _, s := range cfg.Membership.Seeds {
		s = strings.TrimSpace(s)
		if s == "" || s == selfGossipAddr {
			continue
		}
		out = append(out, s)
	}
	return &DockerDiscovery{seeds: out, self: selfGossipAddr}
}

// Seeds returns the configured static seeds minus self.
func (d *DockerDiscovery) Seeds() []string { return d.seeds }

// KubernetesDiscovery is a typed stub wired but unused until M11 (headless-Service DNS). It
// exists so the discovery seam is in place without importing the Kubernetes API now.
type KubernetesDiscovery struct{}

// Seeds returns no seeds; the M11 implementation resolves the headless Service.
func (KubernetesDiscovery) Seeds() []string { return nil }

// NewDiscovery selects a provider by runtime mode.
func NewDiscovery(cfg *config.Config, selfGossipAddr string) Discovery {
	if cfg.Membership.Runtime == config.RuntimeKubernetes {
		return KubernetesDiscovery{}
	}
	return NewDockerDiscovery(cfg, selfGossipAddr)
}
