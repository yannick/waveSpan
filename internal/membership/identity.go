// Package membership is WaveSpan's runtime membership layer: identity, discovery, SWIM-style
// gossip, liveness, and the holder/range directory. It works under Docker without Kubernetes
// (design/README.md hard rule 2; design/04_membership_latency_gossip.md).
package membership

import (
	"fmt"

	"github.com/yannick/wavespan/internal/config"
)

// Member is a participant's identity and addressing (design/04 "Member identity"). memberId is
// runtime identity; storageUuid is durable storage identity (a pod on an empty volume is a new
// storage member). Topology labels are hints; the latency graph is authoritative.
type Member struct {
	ClusterID   string
	MemberID    string
	StorageUUID string
	NodeName    string
	Zone        string
	Region      string
	Geo         string

	GossipAddr string
	DataAddr   string
	AdminAddr  string
}

// MemberFromConfig builds the local Member from config and the durable storage UUID (M1).
// The advertised host must be DNS-resolvable by peers: in docker it is the service name
// (== memberId); in Kubernetes it is the pod DNS name. It defaults to memberId and can be
// overridden with WAVESPAN_ADVERTISE_HOST. nodeName is the *physical* node (for same-node
// placement), not an address, so it is not used for advertising.
func MemberFromConfig(cfg *config.Config, storageUUID string) Member {
	host := cfg.AdvertiseHost
	if host == "" {
		host = cfg.MemberID
	}
	return Member{
		ClusterID:   cfg.ClusterID,
		MemberID:    cfg.MemberID,
		StorageUUID: storageUUID,
		NodeName:    cfg.NodeName,
		Zone:        cfg.Topology.Zone,
		Region:      cfg.Topology.Region,
		Geo:         cfg.Topology.Geo,
		GossipAddr:  fmt.Sprintf("%s:%d", host, cfg.Ports.Gossip),
		DataAddr:    fmt.Sprintf("%s:%d", host, cfg.Ports.Data),
		AdminAddr:   cfg.Admin.Listen,
	}
}

// SameNode reports whether two members are on the same physical node (used by the distinct-node
// placement filter in M3). Falls back to false when node names are unknown.
func (m Member) SameNode(other Member) bool {
	return m.NodeName != "" && m.NodeName == other.NodeName
}
