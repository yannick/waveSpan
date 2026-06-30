package backup

import (
	"fmt"
	"path"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
)

// RestoreBootstrapLogical reconstitutes a fresh store from a logical cluster backup (clone / re-shard,
// design/backup §5.0/§5.1). It restores every per-node logical sub-manifest into dst (re-routing
// collections to the target shard count when ri.CollectionsDataShards > 0), then strips collections raft
// bookkeeping so the subsequent FRESH collections bootstrap starts clean. The node's own storage identity
// (/sys/storage_uuid) is preserved — RestoreLogical skips it — so a clone keeps its own identity and the
// immutable backup is read-only.
//
// dst may be a single store (single-node clone — holds all shards) or one node of a larger target; every
// per-node export is restored and re-routed, and the collections tier sorts ownership out at bootstrap.
func RestoreBootstrapLogical(dst storage.LocalStore, objStore ObjectStore, backupID string, ri RestoreInfo) error {
	cm, err := ReadClusterManifest(objStore, backupID)
	if err != nil {
		return err
	}
	if !containsString(cm.Planes, "logical") {
		return fmt.Errorf("backup: %q has no logical plane; cannot logical-restore from it (planes=%v)", backupID, cm.Planes)
	}
	reg := DefaultRegistry()
	for _, ref := range cm.PerNode {
		if ref.Ref == "" {
			continue // a physical-only node carries no logical sub-manifest
		}
		keyPrefix := path.Dir(ref.Ref) // "<id>/nodes/<m>/node.manifest.json" -> "<id>/nodes/<m>"
		if err := RestoreLogical(dst, objStore, keyPrefix, reg, ri); err != nil {
			return fmt.Errorf("backup: restore node %q: %w", ref.MemberID, err)
		}
	}
	// §5.0: drop collections raft bookkeeping on EVERY path (re-shard already dropped it; same-shape
	// relies on this) so the fresh bootstrap starts at applied-index 0 with a new LogDB.
	return collections.StripRaftBookkeeping(dst)
}
