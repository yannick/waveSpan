package cache

import (
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
// "Holder summaries"): a bloom filter over the keys the member holds durably.
type HolderSummaryWire struct {
	MemberID          string
	Bloom             []byte
	GeneratedAtUnixMs int64
}

type peerSummary struct {
	bloom     *Bloom
	generated int64
}

// Directory tracks the local node's held-key bloom and aggregates peers' gossiped summaries so a
// read miss can resolve likely holders without broadcasting.
type Directory struct {
	mu      sync.RWMutex
	self    string
	own     *Bloom
	ownGen  int64
	peers   map[string]peerSummary
	nowMs   func() int64
	changed bool
}

// NewDirectory builds an empty directory for the local member.
func NewDirectory(self string, nowMs func() int64) *Directory {
	return &Directory{self: self, own: NewBloom(), peers: map[string]peerSummary{}, nowMs: nowMs}
}

// AddHeldKey records that this node now holds (namespace, key) durably, updating the own bloom.
func (d *Directory) AddHeldKey(namespace string, key []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.own.Add(combinedKey(namespace, key))
	d.ownGen = d.nowMs()
	d.changed = true
}

// OwnSummary returns the local summary for gossip.
func (d *Directory) OwnSummary() HolderSummaryWire {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return HolderSummaryWire{MemberID: d.self, Bloom: d.own.Bytes(), GeneratedAtUnixMs: d.ownGen}
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
	d.peers[s.MemberID] = peerSummary{bloom: BloomFromBytes(s.Bloom), generated: s.GeneratedAtUnixMs}
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
