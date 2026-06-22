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
	// ReplicationFactor selects how widely writes in this namespace are replicated:
	//   ""       => the cluster's nearby target-N (default);
	//   "N"      => override the target holder count for this namespace;
	//   "all"    => every node of the CURRENT cluster (does NOT cross to peer clusters);
	//   "global" => every node of EVERY cluster (current cluster + shipped to all peers).
	// "all"/"global" also stream existing records to a joining node on bootstrap.
	ReplicationFactor string `yaml:"replicationFactor"`
}

// GlobalScope says whether a namespace crosses the global (cross-cluster) boundary — orthogonal to
// how widely it replicates WITHIN a cluster.
type GlobalScope int

const (
	// GlobalScopeDefault follows the cluster's global-replication config (ships to peers iff global
	// replication is enabled) — the historical behaviour for unmarked namespaces.
	GlobalScopeDefault GlobalScope = iota
	// GlobalScopeLocalOnly never ships to peer clusters ("all" = the CURRENT cluster only).
	GlobalScopeLocalOnly
	// GlobalScopeGlobal always ships to peer clusters ("global" = every node of every cluster).
	GlobalScopeGlobal
)

// ReplicateEverywhere reports whether this namespace replicates to every node of a cluster (true for
// both "all" — local cluster only — and "global" — every cluster).
func (n NamespaceConfig) ReplicateEverywhere() bool {
	switch strings.ToLower(strings.TrimSpace(n.ReplicationFactor)) {
	case "all", "global":
		return true
	default:
		return false
	}
}

// Scope maps the replication factor to its cross-cluster behaviour.
func (n NamespaceConfig) Scope() GlobalScope {
	switch strings.ToLower(strings.TrimSpace(n.ReplicationFactor)) {
	case "all":
		return GlobalScopeLocalOnly
	case "global":
		return GlobalScopeGlobal
	default:
		return GlobalScopeDefault
	}
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
	// every node of the LOCAL cluster (current cluster only; streamed to a joining node on bootstrap).
	if v, ok := get("WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES"); ok {
		for _, ns := range strings.Split(v, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				c.setNamespaceReplication(ns, "all")
			}
		}
	}
	// WAVESPAN_REPLICATE_GLOBAL_NAMESPACES=ref marks namespaces that live on every node of EVERY
	// cluster (local everywhere + shipped to all peers).
	if v, ok := get("WAVESPAN_REPLICATE_GLOBAL_NAMESPACES"); ok {
		for _, ns := range strings.Split(v, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				c.setNamespaceReplication(ns, "global")
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

// EverywhereNamespaces returns the set of namespaces that replicate to every node of a cluster
// (both "all" and "global"); they share the intra-cluster fanout + join-time backfill.
func (c *Config) EverywhereNamespaces() map[string]bool {
	out := map[string]bool{}
	for _, n := range c.Namespaces {
		if n.ReplicateEverywhere() {
			out[n.Name] = true
		}
	}
	return out
}

// LocalOnlyNamespaces returns namespaces that must NOT cross the global boundary ("all"), so the
// global tap skips them even when cross-cluster replication is enabled.
func (c *Config) LocalOnlyNamespaces() map[string]bool {
	out := map[string]bool{}
	for _, n := range c.Namespaces {
		if n.Scope() == GlobalScopeLocalOnly {
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
