package backup

import (
	"context"
	"fmt"
	"path"

	"github.com/yannick/wavespan/internal/storage"
	"wavesdb"
)

// dbProvider is the optional capability a store exposes when it is backed by a real wavesdb engine that
// can write SSTable checkpoints (physical plane). MemStore does not implement it, so a physical export
// against a non-wavesdb store fails with a clear error rather than silently producing nothing.
type dbProvider interface {
	UnderlyingDB() *wavesdb.DB
}

func hasPlane(planes []Plane, p Plane) bool {
	for _, x := range planes {
		if x == p {
			return true
		}
	}
	return false
}

// Agent is the node-side executor of a backup. A coordinator fans PrepareBackup/ExportBackup out to
// every live node; on each node the Agent seals a consistent view at the frontier (Prepare) and exports
// that node's assignment to the object store (Export), reusing the Phase 2 ExportLogical engine
// (design/backup phase 3a).
type Agent struct {
	reg *Registry
}

// NewAgent builds a node agent using reg (nil = DefaultRegistry, the full authoritative-CF set).
func NewAgent(reg *Registry) *Agent {
	if reg == nil {
		reg = DefaultRegistry()
	}
	return &Agent{reg: reg}
}

// PrepareResult reports a node's readiness. GlobalSeq echoes the frontier the node was asked to prepare
// at (carried for provenance — 3a does not yet seal at it; see Prepare); HeldRanges enumerates the
// ranges this node will export, echoed for the coordinator's commit-time coverage cross-check.
type PrepareResult struct {
	GlobalSeq  uint64
	HeldRanges []string
}

// Prepare confirms this node can serve a coherent export and reports the ranges it holds: it pins (and
// immediately releases) a read-consistent snapshot as a readiness check. It does NOT yet seal the view
// at frontierT. In 3a each node's Export takes its own snapshot at write time, so a backup captures each
// node's own consistent point-in-time view (per-node snapshot isolation) — NOT a single cluster-wide
// cut: ExportLogical writes every key with no Version<=T filter, and frontierT is carried for provenance
// only. The cluster-wide HLC frontier T plus Version<=T AP-tier sealing (spec §1/§3) is deferred to
// Phase 3a.1. (Authoritative per-node range discovery from the local range directory is a 3d
// refinement; in 3a the coordinator supplies the assigned ranges, which the node echoes as held.)
func (a *Agent) Prepare(ctx context.Context, store storage.LocalStore, backupID string, frontierT int64, heldRanges []string) (PrepareResult, error) {
	snap, err := store.Snapshot()
	if err != nil {
		return PrepareResult{}, err
	}
	_ = snap.Close()
	return PrepareResult{GlobalSeq: uint64(frontierT), HeldRanges: heldRanges}, nil
}

// ExportResult is the outcome of one node's export: the logical object/byte counts, the key of the
// per-node logical sub-manifest, the node's stable storage identity (recorded in the cluster manifest
// for 3c restore), the decoded logical manifest, and — when the physical plane ran — the resulting
// wavesdb checkpoint (pass back as a parent for the next incremental) plus its sub-manifest key.
type ExportResult struct {
	Objects             int64
	Bytes               int64
	SubManifestKey      string
	StorageUUID         string
	Manifest            *NodeManifest
	Checkpoint          *wavesdb.CheckpointManifest // physical plane only
	PhysicalManifestKey string                      // physical plane only
	PhysicalGlobalSeq   uint64                      // physical plane only
}

// Export writes this node's assignment to objStore under <backupID>/nodes/<memberID>/, running the
// requested planes (logical and/or physical; an empty planes defaults to logical for 3a compatibility).
//
// Logical (ExportLogical): one object per authoritative CF + a per-node node.manifest.json; full-only.
// frontierT is passed as captureMs and recorded for provenance only — it does NOT filter keys in 3a;
// the Version<=frontierT AP-tier cut is deferred to 3a.1.
//
// Physical (CheckpointToObjectStore): a consistent SSTable checkpoint under <prefix>/physical/. When
// parentCkpt is non-nil this is an INCREMENTAL — only SSTable ids absent from the parent are uploaded —
// and the returned checkpoint still lists the full cumulative table set. The result is recorded in a
// per-node physical.manifest.json (full table set + parent watermark). The physical plane needs the
// wavesdb engine handle (dbProvider) and a wavesdb-capable object store; a MemStore cannot take one.
//
// Re-running an export is idempotent: object keys are deterministic, so a resumed coordinator re-exports
// safely (design/backup phase 3a/3b).
func (a *Agent) Export(ctx context.Context, store storage.LocalStore, objStore ObjectStore, backupID, memberID string, assignment Selector, planes []Plane, frontierT int64, parentCkpt *wavesdb.CheckpointManifest) (ExportResult, error) {
	keyPrefix := path.Join(backupID, "nodes", memberID)
	wantLogical := len(planes) == 0 || hasPlane(planes, PlaneLogical)
	wantPhysical := hasPlane(planes, PlanePhysical)

	var res ExportResult
	if wantLogical {
		man, err := ExportLogical(store, objStore, keyPrefix, a.reg, frontierT, assignment)
		if err != nil {
			return ExportResult{}, err
		}
		var objects, bytes int64
		for _, e := range man.CFs {
			objects++ // one object per non-empty authoritative CF
			bytes += e.Bytes
		}
		res.Objects = objects
		res.Bytes = bytes
		res.SubManifestKey = keyPrefix + "/node.manifest.json"
		res.StorageUUID = man.StorageUUID
		res.Manifest = man
	}
	if wantPhysical {
		prov, ok := store.(dbProvider)
		if !ok {
			return ExportResult{}, fmt.Errorf("backup: physical plane requires a wavesdb-backed store, got %T", store)
		}
		ws, ok := objStore.(wavesdb.ObjectStore)
		if !ok {
			return ExportResult{}, fmt.Errorf("backup: physical plane requires a wavesdb object store, got %T", objStore)
		}
		cm, err := prov.UnderlyingDB().CheckpointToObjectStore(ctx, ws, keyPrefix+"/physical", parentCkpt)
		if err != nil {
			return ExportResult{}, err
		}
		var parentSeq uint64
		if parentCkpt != nil {
			parentSeq = parentCkpt.GlobalSeq
		}
		pmKey := PhysicalManifestKey(backupID, memberID)
		if err := WritePhysicalManifest(objStore, pmKey, physicalManifestFromCheckpoint(cm, parentSeq)); err != nil {
			return ExportResult{}, err
		}
		res.Checkpoint = cm
		res.PhysicalManifestKey = pmKey
		res.PhysicalGlobalSeq = cm.GlobalSeq
	}

	// Capture the node's stable storage identity even on a physical-only backup (3c restore needs it).
	if res.StorageUUID == "" {
		if v, ok, _ := store.Get(storage.CFSys, []byte(storageIdentityKey)); ok {
			res.StorageUUID = string(v)
		}
	}
	return res, nil
}
