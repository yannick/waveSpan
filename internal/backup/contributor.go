package backup

import (
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
)

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
	// RebuildWhenCut marks an authoritative CF that must NOT be exported while an
	// HLC ≤T cut is active (captureMs > 0); instead it is rebuilt on restore from
	// the surviving ≤T data (RebuildAfterRestore). With no cut (a full backup) it
	// is exported verbatim like any authoritative CF. This preserves state the
	// rebuild cannot reconstruct on the existing full-backup path — for CFKVMeta,
	// the LWW rebuild recovers only the winner/tombstone/expiry, not
	// SiblingVersions / conflict-tracking — while still avoiding the dangling
	// latest-pointer that a verbatim copy would leave once a cut drops the >T
	// winner (design/backup §5.2).
	RebuildWhenCut bool
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
	// VersionOf decodes the HLC version of a (cf,key,value) record, for the HLC
	// consistent cut (Phase 3a.1). ok is false for CFs that carry no per-record
	// version usable for the ≤T cut — derived/index CFs, system config, and the
	// raft-consistent collections CF (CFReplData). Only CFKVData's version drives
	// the cut today (ExportLogical filters CFKVData only); graph/vector decode their
	// version for completeness but are exported snapshot-current, not sealed to T.
	VersionOf(cf storage.ColumnFamily, key, value []byte) (version.Version, bool)
}

// versionLEQ reports whether a record's HLC version is at or below the frontier ceiling T (physical-ms
// comparison; equal ms is included). It is the per-record test that realises the consistent cut.
func versionLEQ(v version.Version, frontierMs int64) bool {
	return v.HLCPhysicalMs <= uint64(frontierMs)
}

// Registry holds the registered contributors.
type Registry struct{ contributors []Contributor }

// NewRegistry returns an empty contributor registry.
func NewRegistry() *Registry { return &Registry{} }

// Register adds a contributor to the registry.
func (r *Registry) Register(c Contributor) { r.contributors = append(r.contributors, c) }

// Contributors returns the registered contributors in registration order.
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

// CutDerivedCFs returns the set of authoritative CFs flagged RebuildWhenCut — the
// CFs ExportLogical skips while an HLC ≤T cut is active (rebuilt on restore from
// the surviving ≤T data) but exports verbatim on a full backup. See CFSpec.RebuildWhenCut.
func (r *Registry) CutDerivedCFs() map[storage.ColumnFamily]bool {
	out := map[storage.ColumnFamily]bool{}
	for _, c := range r.contributors {
		for _, s := range c.CFs() {
			if s.RebuildWhenCut {
				out[s.CF] = true
			}
		}
	}
	return out
}
