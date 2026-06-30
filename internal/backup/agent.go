package backup

import (
	"context"
	"path"

	"github.com/yannick/wavespan/internal/storage"
)

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

// PrepareResult reports a node's sealed view. GlobalSeq is the sealed sequence the node guarantees its
// export reflects (at or beyond the frontier); HeldRanges enumerates the ranges this node will export,
// echoed for the coordinator's commit-time coverage cross-check.
type PrepareResult struct {
	GlobalSeq  uint64
	HeldRanges []string
}

// Prepare seals this node's view at frontierT: it pins (and immediately releases) a read-consistent
// snapshot to confirm the node can serve a coherent export, and reports the ranges it holds. Export
// takes its own snapshot at write time; because the AP-tier cut is bounded by the captureMs/frontier
// ceiling, re-deriving the snapshot at export time observes the same sealed state. (Authoritative
// per-node range discovery from the local range directory is a 3d refinement; in 3a the coordinator
// supplies the assigned ranges, which the node echoes as held.)
func (a *Agent) Prepare(ctx context.Context, store storage.LocalStore, backupID string, frontierT int64, heldRanges []string) (PrepareResult, error) {
	snap, err := store.Snapshot()
	if err != nil {
		return PrepareResult{}, err
	}
	_ = snap.Close()
	return PrepareResult{GlobalSeq: uint64(frontierT), HeldRanges: heldRanges}, nil
}

// ExportResult is the outcome of one node's export: the object/byte counts, the key of the per-node
// sub-manifest, and the decoded manifest itself (for the coordinator's cluster.manifest).
type ExportResult struct {
	Objects        int64
	Bytes          int64
	SubManifestKey string
	Manifest       *NodeManifest
}

// Export writes this node's assignment to objStore under <backupID>/nodes/<memberID>/. It runs the
// logical plane via ExportLogical (one object per authoritative CF + a per-node sub-manifest); the
// physical plane (CheckpointToObjectStore) operates on the wavesdb.DB handle rather than the
// storage.LocalStore the agent holds, so it is plumbed at the DB layer and is a no-op here in 3a.
// Re-running an export is idempotent: object keys are deterministic, so a resumed coordinator may
// re-export safely (design/backup phase 3a, resumability).
func (a *Agent) Export(ctx context.Context, store storage.LocalStore, objStore ObjectStore, backupID, memberID string, assignment Selector, planes []Plane, frontierT int64) (ExportResult, error) {
	keyPrefix := path.Join(backupID, "nodes", memberID)
	man, err := ExportLogical(store, objStore, keyPrefix, a.reg, frontierT, assignment)
	if err != nil {
		return ExportResult{}, err
	}
	var objects, bytes int64
	for _, e := range man.CFs {
		objects++ // one object per non-empty authoritative CF
		bytes += e.Bytes
	}
	return ExportResult{
		Objects:        objects,
		Bytes:          bytes,
		SubManifestKey: keyPrefix + "/node.manifest.json",
		Manifest:       man,
	}, nil
}
