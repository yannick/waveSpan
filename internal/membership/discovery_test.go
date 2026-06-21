package membership

import (
	"testing"

	"github.com/cwire/wavespan/internal/config"
)

func TestDockerDiscoveryParsesSeedsAndDropsSelf(t *testing.T) {
	cfg := &config.Config{
		Membership: config.MembershipConfig{
			Runtime: config.RuntimeDocker,
			Seeds:   []string{"node1:7700", "node2:7700", " node3:7700 "},
		},
	}
	d := NewDockerDiscovery(cfg, "node2:7700")
	seeds := d.Seeds()
	if len(seeds) != 2 {
		t.Fatalf("expected 2 seeds (self dropped), got %v", seeds)
	}
	for _, s := range seeds {
		if s == "node2:7700" {
			t.Fatal("self should be excluded from seeds")
		}
	}
	if seeds[1] != "node3:7700" {
		t.Fatalf("seed not trimmed: %q", seeds[1])
	}
}

func TestNewDiscoverySelectsByRuntime(t *testing.T) {
	k := NewDiscovery(&config.Config{Membership: config.MembershipConfig{Runtime: config.RuntimeKubernetes}}, "x:7700")
	if _, ok := k.(KubernetesDiscovery); !ok {
		t.Fatalf("kubernetes runtime should select KubernetesDiscovery, got %T", k)
	}
	d := NewDiscovery(&config.Config{Membership: config.MembershipConfig{Runtime: config.RuntimeDocker, Seeds: []string{"a:1"}}}, "x:7700")
	if _, ok := d.(*DockerDiscovery); !ok {
		t.Fatalf("docker runtime should select DockerDiscovery, got %T", d)
	}
}
