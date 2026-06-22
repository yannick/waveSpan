// Package config loads and validates WaveSpan node configuration from a YAML file with
// WAVESPAN_-prefixed environment overrides. It validates eagerly and fails fast on
// invalid input (TS-002), mirroring design/17_source_tree.md "Configuration file" and the
// docker/kubernetes discovery inputs in design/04_membership_latency_gossip.md.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Runtime selects the membership discovery mode.
type Runtime string

// Supported membership discovery runtimes.
const (
	RuntimeDocker     Runtime = "docker"
	RuntimeKubernetes Runtime = "kubernetes"
)

// Config is the full node configuration.
type Config struct {
	ClusterID     string            `yaml:"clusterId"`
	MemberID      string            `yaml:"memberId"`
	NodeName      string            `yaml:"nodeName"`
	AdvertiseHost string            `yaml:"advertiseHost"`
	Topology      TopologyConfig    `yaml:"topology"`
	Storage       StorageConfig     `yaml:"storage"`
	Membership    MembershipConfig  `yaml:"membership"`
	Replication   ReplicationConfig `yaml:"replication"`
	Admin         AdminConfig       `yaml:"admin"`
	Ports         PortsConfig       `yaml:"ports"`
	Security      SecurityConfig    `yaml:"security"`

	GlobalReplication GlobalReplicationConfig `yaml:"globalReplication"`
	Namespaces        []NamespaceConfig       `yaml:"namespaces"`
}

// TopologyConfig holds the static topology labels (hints; the latency graph is authoritative,
// design/04 "Topology penalty").
type TopologyConfig struct {
	Zone   string `yaml:"zone"`
	Region string `yaml:"region"`
	Geo    string `yaml:"geo"`
}

// PortsConfig holds the advertised gossip and data ports (design/04 "Member identity",
// design/09 ports). The advertised host defaults to memberId in docker (service DNS).
type PortsConfig struct {
	Gossip int `yaml:"gossip"`
	Data   int `yaml:"data"`
}

// StorageConfig configures the local wavesdb engine.
type StorageConfig struct {
	Path   string `yaml:"path"`
	Engine string `yaml:"engine"`
}

// MembershipConfig configures discovery and the gossip seed list.
type MembershipConfig struct {
	Runtime Runtime  `yaml:"runtime"`
	Seeds   []string `yaml:"seeds"`
}

// ReplicationConfig selects the replication policy applied to this node. The replica counts are
// pointers so an explicit 0 (single-node / local-only dev) is distinct from "unset" (defaults).
type ReplicationConfig struct {
	PolicyRef            string `yaml:"policyRef"`
	TargetNearbyReplicas *int   `yaml:"targetNearbyReplicas"`
	MinAckNearbyReplicas *int   `yaml:"minAckNearbyReplicas"`
}

// Target returns the target nearby replica count (default 3).
func (r ReplicationConfig) Target() int {
	if r.TargetNearbyReplicas != nil {
		return *r.TargetNearbyReplicas
	}
	return 3
}

// MinAck returns the minimum nearby durable replicas for write ACK (default 1, origin+1).
func (r ReplicationConfig) MinAck() int {
	if r.MinAckNearbyReplicas != nil {
		return *r.MinAckNearbyReplicas
	}
	return 1
}

// AdminConfig is the listen address for the admin HTTP server (/healthz, /readyz, /metrics).
type AdminConfig struct {
	Listen string `yaml:"listen"`
}

// SecurityConfig holds transport-security and dev-mode toggles.
type SecurityConfig struct {
	InsecureDevMode bool `yaml:"insecureDevMode"`
}

// Defaults applied when fields are unset.
const (
	defaultEngine      = "wavesdb"
	defaultAdminListen = ":7900"
	defaultStoragePath = "/var/lib/wavespan"
	defaultGossipPort  = 7700
	defaultDataPort    = 7800
)

// Load reads the YAML file at path (optional; empty path skips file load), applies
// WAVESPAN_-prefixed overrides from env (nil env reads the process environment),
// fills defaults, and validates. It returns an error on any invalid configuration.
func Load(path string, env map[string]string) (*Config, error) {
	cfg := &Config{}

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}

	cfg.applyEnv(envLookup(env))
	cfg.applyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// envLookup returns a lookup function over the supplied map, or the process environment
// when the map is nil.
func envLookup(env map[string]string) func(string) (string, bool) {
	if env == nil {
		return os.LookupEnv
	}
	return func(k string) (string, bool) { v, ok := env[k]; return v, ok }
}

func (c *Config) applyEnv(get func(string) (string, bool)) {
	if v, ok := get("WAVESPAN_CLUSTER_ID"); ok {
		c.ClusterID = v
	}
	if v, ok := get("WAVESPAN_MEMBER_ID"); ok {
		c.MemberID = v
	}
	if v, ok := get("WAVESPAN_RUNTIME"); ok {
		c.Membership.Runtime = Runtime(v)
	}
	if v, ok := get("WAVESPAN_NODE_NAME"); ok {
		c.NodeName = v
	}
	if v, ok := get("WAVESPAN_ADVERTISE_HOST"); ok {
		c.AdvertiseHost = v
	}
	if v, ok := get("WAVESPAN_ZONE"); ok {
		c.Topology.Zone = v
	}
	if v, ok := get("WAVESPAN_REGION"); ok {
		c.Topology.Region = v
	}
	if v, ok := get("WAVESPAN_GEO"); ok {
		c.Topology.Geo = v
	}
	if v, ok := get("WAVESPAN_SEEDS"); ok {
		c.Membership.Seeds = splitSeeds(v)
	}
	if v, ok := get("WAVESPAN_STORAGE_PATH"); ok {
		c.Storage.Path = v
	}
	if v, ok := get("WAVESPAN_ADMIN_LISTEN"); ok {
		c.Admin.Listen = v
	}
	if v, ok := get("WAVESPAN_INSECURE_DEV_MODE"); ok {
		c.Security.InsecureDevMode = v == "true" || v == "1"
	}
	if v, ok := get("WAVESPAN_TARGET_NEARBY_REPLICAS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.Replication.TargetNearbyReplicas = &n
		}
	}
	if v, ok := get("WAVESPAN_MIN_ACK_NEARBY_REPLICAS"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			c.Replication.MinAckNearbyReplicas = &n
		}
	}
	c.applyGlobalEnv(get)
}

func (c *Config) applyDefaults() {
	if c.Storage.Engine == "" {
		c.Storage.Engine = defaultEngine
	}
	if c.Storage.Path == "" {
		c.Storage.Path = defaultStoragePath
	}
	if c.Admin.Listen == "" {
		c.Admin.Listen = defaultAdminListen
	}
	if c.Ports.Gossip == 0 {
		c.Ports.Gossip = defaultGossipPort
	}
	if c.Ports.Data == 0 {
		c.Ports.Data = defaultDataPort
	}
	// replica-count defaults are provided by ReplicationConfig.Target()/MinAck() so an explicit 0
	// (single-node dev) is preserved.
}

// Validate enforces the fail-fast rules (TS-002).
func (c *Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("config: clusterId is required")
	}
	if c.MemberID == "" {
		return fmt.Errorf("config: memberId is required")
	}
	switch c.Membership.Runtime {
	case RuntimeDocker:
		if len(c.Membership.Seeds) == 0 {
			return fmt.Errorf("config: membership.seeds must be non-empty in docker runtime (static seed discovery)")
		}
	case RuntimeKubernetes:
		// seeds are discovered from the headless Service at runtime; not required here.
	default:
		return fmt.Errorf("config: membership.runtime %q is invalid (want %q or %q)",
			c.Membership.Runtime, RuntimeDocker, RuntimeKubernetes)
	}
	if c.Storage.Engine != defaultEngine {
		return fmt.Errorf("config: storage.engine %q is unsupported; v1 links %q at build time",
			c.Storage.Engine, defaultEngine)
	}
	return nil
}

func splitSeeds(v string) []string {
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
