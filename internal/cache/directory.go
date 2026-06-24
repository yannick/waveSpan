package cache

import (
	"sort"
	"sync"
)

// combinedKey namespaces a key for the holder bloom.
func combinedKey(namespace string, key []byte) []byte {
	out := make([]byte, 0, len(namespace)+1+len(key))
	out = append(out, namespace...)
	out = append(out, 0)
	return append(out, key...)
}

// HolderSummaryWire is the gossiped compact advertisement of a node's held keys (design/04
// "Holder summaries"): a bloom filter over the keys the member holds durably, an HLL sketch for
// distinct-cardinality estimation, and the member's exact held-key count.
type HolderSummaryWire struct {
	MemberID          string
	Bloom             []byte
	HLL               []byte
	ApproxKeys        uint64
	Namespaces        []string
	GeneratedAtUnixMs int64
}

type peerSummary struct {
	bloom      *Bloom
	hll        *HLL
	approxKeys uint64
	namespaces []string
	generated  int64
}

// Directory tracks the local node's held-key bloom and aggregates peers' gossiped summaries so a
// read miss can resolve likely holders without broadcasting. It also keeps an HLL of the node's held
// keys so the cluster's distinct logical key count can be estimated by unioning members' sketches.
type Directory struct {
	mu      sync.RWMutex
	self    string
	own     *Bloom
	ownHLL  *HLL
	ownNS   map[string]struct{}
	ownGen  int64
	peers   map[string]peerSummary
	nowMs   func() int64
	changed bool
}

// NewDirectory builds an empty directory for the local member.
func NewDirectory(self string, nowMs func() int64) *Directory {
	return &Directory{self: self, own: NewBloom(), ownHLL: NewHLL(), ownNS: map[string]struct{}{}, peers: map[string]peerSummary{}, nowMs: nowMs}
}

// AddHeldKey records that this node now holds (namespace, key) durably, updating the own bloom, HLL,
// and namespace set. All three are insert-idempotent, so re-storing an existing key is harmless. The
// namespace set is never pruned: an emptied namespace lingers until the node restarts, which is
// acceptable (we cannot reliably observe the deletion of a namespace's last key).
func (d *Directory) AddHeldKey(namespace string, key []byte) {
	ck := combinedKey(namespace, key)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.own.Add(ck)
	d.ownHLL.Add(ck)
	if _, ok := d.ownNS[namespace]; !ok {
		d.ownNS[namespace] = struct{}{}
	}
	d.ownGen = d.nowMs()
	d.changed = true
}

// OwnSummary returns the local summary for gossip. ApproxKeys is left for the caller to fill with the
// authoritative live-key count (the bloom/HLL are insert-only estimates).
func (d *Directory) OwnSummary() HolderSummaryWire {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ns := make([]string, 0, len(d.ownNS))
	for n := range d.ownNS {
		ns = append(ns, n)
	}
	sort.Strings(ns)
	return HolderSummaryWire{MemberID: d.self, Bloom: d.own.Bytes(), HLL: d.ownHLL.Bytes(), Namespaces: ns, GeneratedAtUnixMs: d.ownGen}
}

// ApplyPeerSummary merges a peer's gossiped summary (newer generations win).
func (d *Directory) ApplyPeerSummary(s HolderSummaryWire) {
	if s.MemberID == "" || s.MemberID == d.self {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if cur, ok := d.peers[s.MemberID]; ok && cur.generated >= s.GeneratedAtUnixMs {
		return
	}
	d.peers[s.MemberID] = peerSummary{bloom: BloomFromBytes(s.Bloom), hll: HLLFromBytes(s.HLL), approxKeys: s.ApproxKeys, namespaces: append([]string(nil), s.Namespaces...), generated: s.GeneratedAtUnixMs}
}

// DistinctKeysEstimate unions this node's HLL with every alive peer's sketch and returns the
// estimated number of distinct logical keys across the cluster. A key hashes identically on each of
// its holders, so replicas collide in the union and are counted once. isAlive may be nil (count all
// known peers). ownKeys is folded in via a fresh sketch only if this node has never gossiped — in
// practice the own HLL already reflects local keys, so it is always included.
func (d *Directory) DistinctKeysEstimate(isAlive func(string) bool) uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	union := NewHLL()
	union.Merge(d.ownHLL)
	for member, s := range d.peers {
		if isAlive != nil && !isAlive(member) {
			continue
		}
		union.Merge(s.hll)
	}
	return union.Estimate()
}

// PeerReplicaSum sums the exact held-key counts gossiped by alive peers (excluding self). Add the
// local count to get the cluster's total stored replicas. isAlive may be nil (sum all known peers).
func (d *Directory) PeerReplicaSum(isAlive func(string) bool) uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var total uint64
	for member, s := range d.peers {
		if isAlive != nil && !isAlive(member) {
			continue
		}
		total += s.approxKeys
	}
	return total
}

// Namespaces returns the sorted union of namespaces held by this node and every alive peer. Emptied
// namespaces may linger (see AddHeldKey). isAlive may be nil (union over all known peers).
func (d *Directory) Namespaces(isAlive func(string) bool) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	set := make(map[string]struct{}, len(d.ownNS))
	for n := range d.ownNS {
		set[n] = struct{}{}
	}
	for member, s := range d.peers {
		if isAlive != nil && !isAlive(member) {
			continue
		}
		for _, n := range s.namespaces {
			set[n] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// ResolveHolders returns peer member ids whose bloom indicates they may hold (namespace, key).
// It never returns self and never broadcasts (design/README.md hard rule 3).
func (d *Directory) ResolveHolders(namespace string, key []byte) []string {
	ck := combinedKey(namespace, key)
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []string
	for member, s := range d.peers {
		if s.bloom.MaybeContains(ck) {
			out = append(out, member)
		}
	}
	return out
}

// DropPeer removes a peer's summary (e.g. when it is forgotten).
func (d *Directory) DropPeer(member string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.peers, member)
}
