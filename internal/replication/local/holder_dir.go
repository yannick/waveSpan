package local

import (
	"sort"
	"sync"

	"github.com/yannick/wavespan/internal/version"
)

// HolderDirectory tracks the durable holders of each key so a read miss can resolve a holder
// without broadcasting (design/README.md hard rule 3; design/04 "Range directory"). It is the
// in-memory fast path; durable holder records and gossiped HolderSummary roll-ups build on it.
//
// It need not be perfectly accurate: a stale entry just triggers fallback to another holder.
type HolderDirectory struct {
	mu      sync.RWMutex
	self    string
	holders map[string]map[string]version.Version // key-id -> memberID -> version held
}

// NewHolderDirectory builds an empty directory for the local member.
func NewHolderDirectory(selfMemberID string) *HolderDirectory {
	return &HolderDirectory{self: selfMemberID, holders: map[string]map[string]version.Version{}}
}

// keyID identifies a logical key within a namespace.
func keyID(namespace string, key []byte) string {
	return namespace + "\x00" + string(key)
}

// RecordHolder notes that member holds a durable replica of (namespace, key) at version v.
func (d *HolderDirectory) RecordHolder(namespace string, key []byte, member string, v version.Version) {
	id := keyID(namespace, key)
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.holders[id]
	if !ok {
		m = map[string]version.Version{}
		d.holders[id] = m
	}
	m[member] = v
}

// RemoveHolder drops a member from a key's holder set (e.g. when the member dies).
func (d *HolderDirectory) RemoveHolder(namespace string, key []byte, member string) {
	id := keyID(namespace, key)
	d.mu.Lock()
	defer d.mu.Unlock()
	if m, ok := d.holders[id]; ok {
		delete(m, member)
		if len(m) == 0 {
			delete(d.holders, id)
		}
	}
}

// Holders returns the member ids known to hold (namespace, key), sorted.
func (d *HolderDirectory) Holders(namespace string, key []byte) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	m := d.holders[keyID(namespace, key)]
	out := make([]string, 0, len(m))
	for member := range m {
		out = append(out, member)
	}
	sort.Strings(out)
	return out
}

// Count returns the number of known holders of a key.
func (d *HolderDirectory) Count(namespace string, key []byte) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.holders[keyID(namespace, key)])
}

// UnderReplicatedEstimate counts keys whose alive-holder count is below targetHolders — the
// alerting signal for the spot-churn risk (design/14, IMPLEMENTATION_STRATEGY.md section 4).
func (d *HolderDirectory) UnderReplicatedEstimate(targetHolders int, isAlive func(string) bool) int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	under := 0
	for _, m := range d.holders {
		alive := 0
		for member := range m {
			if isAlive == nil || isAlive(member) {
				alive++
			}
		}
		if alive < targetHolders {
			under++
		}
	}
	return under
}

// keysHeldBy returns the (namespace,key) ids a member holds, for repair when it dies.
func (d *HolderDirectory) keysHeldBy(member string) []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []string
	for id, m := range d.holders {
		if _, ok := m[member]; ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// splitKeyID reverses keyID into namespace and key.
func splitKeyID(id string) (namespace string, key []byte) {
	for i := 0; i < len(id); i++ {
		if id[i] == 0 {
			return id[:i], []byte(id[i+1:])
		}
	}
	return id, nil
}
