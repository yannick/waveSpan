package config

import "strings"

// ClusterPeer is a peer cluster for global active-active replication (design/06).
type ClusterPeer struct {
	ClusterID    string `yaml:"clusterId"`
	Geo          string `yaml:"geo"`
	ReplEndpoint string `yaml:"replEndpoint"` // host:port of a peer data pod's GlobalReplication
}

// GlobalReplicationConfig configures cross-cluster replication.
type GlobalReplicationConfig struct {
	Mode                       string        `yaml:"mode"` // active-active-async | off (default off)
	Peers                      []ClusterPeer `yaml:"peers"`
	ReadPolicy                 string        `yaml:"readPolicy"`
	AntiEntropyIntervalSeconds int           `yaml:"antiEntropyIntervalSeconds"`
	OutLogDiskBudgetBytes      int64         `yaml:"outLogDiskBudgetBytes"`
}

// Enabled reports whether global replication is on.
func (g GlobalReplicationConfig) Enabled() bool {
	return g.Mode == "active-active-async" && len(g.Peers) > 0
}

// NamespaceConfig sets per-namespace policy: the conflict resolver, whether writes must be globally
// durable (block the local ACK when the out-log is full), and the replication factor.
type NamespaceConfig struct {
	Name                     string `yaml:"name"`
	ConflictPolicy           string `yaml:"conflictPolicy"` // "" => hlc-last-write-wins
	GlobalDurabilityRequired bool   `yaml:"globalDurabilityRequired"`
	// ReplicationFactor selects how widely writes in this namespace are replicated: "" (default) uses
	// the cluster's nearby target-N; "all" replicates every record to EVERY alive node (reference /
	// "everywhere" data), and a joining node streams the existing records on bootstrap; a positive
	// integer overrides the target holder count for this namespace.
	ReplicationFactor string `yaml:"replicationFactor"`
}

// ReplicateEverywhere reports whether this namespace replicates to all nodes.
func (n NamespaceConfig) ReplicateEverywhere() bool {
	return strings.EqualFold(strings.TrimSpace(n.ReplicationFactor), "all")
}

// applyGlobalEnv layers WAVESPAN_GLOBAL_* overrides.
//
//	WAVESPAN_GLOBAL_MODE=active-active-async
//	WAVESPAN_GLOBAL_PEERS=clusterB@b1:7801,clusterB@b2:7801
//	WAVESPAN_KEEP_SIBLINGS_NAMESPACES=siblings,notes
func (c *Config) applyGlobalEnv(get func(string) (string, bool)) {
	if v, ok := get("WAVESPAN_GLOBAL_MODE"); ok {
		c.GlobalReplication.Mode = v
	}
	if v, ok := get("WAVESPAN_GLOBAL_PEERS"); ok {
		c.GlobalReplication.Peers = parsePeers(v)
	}
	if v, ok := get("WAVESPAN_KEEP_SIBLINGS_NAMESPACES"); ok {
		for _, ns := range strings.Split(v, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				c.Namespaces = append(c.Namespaces, NamespaceConfig{Name: ns, ConflictPolicy: "keep-siblings"})
			}
		}
	}
	// WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES=ref,config marks namespaces whose records live on
	// every node (and are streamed to a joining node on bootstrap).
	if v, ok := get("WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES"); ok {
		for _, ns := range strings.Split(v, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				c.setNamespaceReplication(ns, "all")
			}
		}
	}
}

// setNamespaceReplication sets (or adds) the replication factor for a namespace.
func (c *Config) setNamespaceReplication(name, factor string) {
	for i := range c.Namespaces {
		if c.Namespaces[i].Name == name {
			c.Namespaces[i].ReplicationFactor = factor
			return
		}
	}
	c.Namespaces = append(c.Namespaces, NamespaceConfig{Name: name, ReplicationFactor: factor})
}

// EverywhereNamespaces returns the set of namespaces that replicate to all nodes.
func (c *Config) EverywhereNamespaces() map[string]bool {
	out := map[string]bool{}
	for _, n := range c.Namespaces {
		if n.ReplicateEverywhere() {
			out[n.Name] = true
		}
	}
	return out
}

func parsePeers(v string) []ClusterPeer {
	var out []ClusterPeer
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// clusterId@host:port
		at := strings.Index(p, "@")
		if at <= 0 {
			continue
		}
		out = append(out, ClusterPeer{ClusterID: p[:at], ReplEndpoint: p[at+1:]})
	}
	return out
}

// ConflictPolicy returns the configured conflict policy for a namespace (default LWW).
func (c *Config) ConflictPolicy(namespace string) string {
	for _, ns := range c.Namespaces {
		if ns.Name == namespace {
			if ns.ConflictPolicy != "" {
				return ns.ConflictPolicy
			}
			return "hlc-last-write-wins"
		}
	}
	return "hlc-last-write-wins"
}

// GlobalDurabilityRequired reports whether a namespace's writes must be globally durable.
func (c *Config) GlobalDurabilityRequired(namespace string) bool {
	for _, ns := range c.Namespaces {
		if ns.Name == namespace {
			return ns.GlobalDurabilityRequired
		}
	}
	return false
}
