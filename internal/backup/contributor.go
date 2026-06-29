package backup

import "github.com/yannick/wavespan/internal/storage"

// CFSpec declares one column family a contributor owns and whether it is
// authoritative (backed up) or derived (skipped, rebuilt on restore).
type CFSpec struct {
	CF            storage.ColumnFamily
	Authoritative bool
}

// RestoreInfo is passed to rebuild hooks; it carries restore context so a
// datatype can reconcile (e.g. time-relative state). Grows in later phases.
type RestoreInfo struct {
	CaptureWallClockMs int64
	RestoreWallClockMs int64
	Clone              bool // new cluster identity (vs same-cluster DR)
}

// Contributor is how a subsystem participates in backup without the core
// knowing the datatype. New datatypes implement this and Register; the engine
// never names a datatype.
type Contributor interface {
	Name() string
	CFs() []CFSpec
	// RebuildAfterRestore rebuilds this contributor's derived indexes (and, in
	// later phases, reconciles time-relative state) after raw data is restored.
	RebuildAfterRestore(dst storage.LocalStore, ri RestoreInfo) error
}

// Registry holds the registered contributors.
type Registry struct{ contributors []Contributor }

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) Register(c Contributor) { r.contributors = append(r.contributors, c) }

func (r *Registry) Contributors() []Contributor { return r.contributors }

// AuthoritativeCFs returns the deduplicated set of authoritative CFs across all
// contributors, in CF order.
func (r *Registry) AuthoritativeCFs() []storage.ColumnFamily {
	seen := map[storage.ColumnFamily]bool{}
	var out []storage.ColumnFamily
	for _, c := range r.contributors {
		for _, s := range c.CFs() {
			if s.Authoritative && !seen[s.CF] {
				seen[s.CF] = true
				out = append(out, s.CF)
			}
		}
	}
	return out
}
