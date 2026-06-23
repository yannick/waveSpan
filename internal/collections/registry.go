package collections

import (
	"errors"
	"sync"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
)

// NodeRegistryFactory builds the node registry (design/30 §12, Appendix B.5). Setting a custom
// registry switches dragonboat into NodeHostID addressing: cluster membership carries stable
// NodeHostIDs (not host:port), and the registry turns a (shardID, replicaID) into a reachable address
// in two steps — dragonboat populates (shardID, replicaID) -> NodeHostID via Add as it learns
// membership, and the Resolver maps that NodeHostID to its current address. Backing the Resolver with
// the cluster's SWIM membership means replica addresses follow the same gossip as the rest of the node
// and survive a peer restarting at a new address, without a second gossip stack.
type NodeRegistryFactory struct {
	// Resolver maps a NodeHostID to its current "host:port". Back it with SWIM in production. When nil,
	// the NodeHostID target is assumed to already be a reachable address (static deployments / tests).
	Resolver func(nodeHostID string) (string, bool)
}

var _ config.NodeRegistryFactory = (*NodeRegistryFactory)(nil)

// Create builds a registry instance for a NodeHost.
func (f *NodeRegistryFactory) Create(_ string, _ uint64, _ config.TargetValidator) (raftio.INodeRegistry, error) {
	return &nodeRegistry{addrs: map[regKey]string{}, resolver: f.Resolver}, nil
}

type regKey struct{ shard, replica uint64 }

type nodeRegistry struct {
	mu       sync.RWMutex
	addrs    map[regKey]string // (shard, replica) -> NodeHostID (or address when no resolver)
	resolver func(nodeHostID string) (string, bool)
}

var _ raftio.INodeRegistry = (*nodeRegistry)(nil)

func (r *nodeRegistry) Add(shardID, replicaID uint64, url string) {
	r.mu.Lock()
	r.addrs[regKey{shardID, replicaID}] = url
	r.mu.Unlock()
}

func (r *nodeRegistry) Remove(shardID, replicaID uint64) {
	r.mu.Lock()
	delete(r.addrs, regKey{shardID, replicaID})
	r.mu.Unlock()
}

func (r *nodeRegistry) RemoveShard(shardID uint64) {
	r.mu.Lock()
	for k := range r.addrs {
		if k.shard == shardID {
			delete(r.addrs, k)
		}
	}
	r.mu.Unlock()
}

// Resolve returns the target address (and connection key) for a replica: the membership-populated
// NodeHostID, then the Resolver (SWIM) to turn it into a current address.
func (r *nodeRegistry) Resolve(shardID, replicaID uint64) (string, string, error) {
	r.mu.RLock()
	target, ok := r.addrs[regKey{shardID, replicaID}]
	r.mu.RUnlock()
	if !ok {
		return "", "", errors.New("wavespan: replica not in registry")
	}
	if r.resolver == nil {
		return target, target, nil // static: the target is already an address
	}
	addr, ok := r.resolver(target)
	if !ok {
		return "", "", errors.New("wavespan: NodeHostID not resolved by SWIM: " + target)
	}
	return addr, addr, nil
}

func (r *nodeRegistry) Close() error { return nil }
