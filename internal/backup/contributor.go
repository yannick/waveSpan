package backup

import "github.com/yannick/wavespan/internal/storage"

// storageIdentityKey is the node-local storage identity key in CFSys. It mirrors
// storage.storageUUIDKey ("/sys/storage_uuid"), which is unexported. Export reads
// it informationally (read-only) and restore skips it, so the target keeps its
// own identity (storage/identity.go). Defined here so both share one definition.
const storageIdentityKey = "/sys/storage_uuid"

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
	// CollectionsDataShards, when > 0, is the target cluster's collections shard
	// count: CFReplData rows are re-routed to their shard under this N (re-shard).
	// 0 means restore CFReplData verbatim (same-shape, Phase 2a behavior).
	CollectionsDataShards uint64
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
	// Selects reports whether a key in cf should be included under sel (consulted
	// only when sel is non-empty — see ExportLogical). It decodes the key's
	// selector entity (namespace / graph / collection) for its CF.
	Selects(cf storage.ColumnFamily, key []byte, sel Selector) bool
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
