package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

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

// RestoreBootstrapPhysical reconstitutes a node's data directory from a physical backup chain (same-shape
// DR, design/backup §5.0). It resolves the chain (base→leaf), gathers every chain member's SSTable
// objects for this node into dataDir (the union is the full table set — 3b writes each backup's delta
// under its own prefix), and installs the LEAF's MANIFEST (the full cumulative table set). dataDir is then
// openable as a wavesdb store. Finally it strips collections raft bookkeeping from the restored CFReplData
// so the fresh collections bootstrap starts at applied-index 0 with a new LogDB. KV/graph/vector are
// raft-free and recovered as-is.
//
// memberID selects the source node to restore from — the caller matches THIS node to a source node by
// stable identity (member id / storage uuid from the manifest topology) before calling.
func RestoreBootstrapPhysical(ctx context.Context, objStore ObjectStore, backupID, memberID, dataDir string) error {
	chain, err := ResolveChain(objStore, backupID)
	if err != nil {
		return err
	}
	if len(chain) == 0 {
		return fmt.Errorf("backup: empty chain for %q", backupID)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	// Gather SSTables from every chain member's physical prefix (base + each increment's delta).
	for _, bid := range chain {
		prefix := path.Join(bid, "nodes", memberID, "physical")
		if err := copyPrefixSSTables(ctx, objStore, prefix, dataDir); err != nil {
			return fmt.Errorf("backup: gather physical objects for %q: %w", bid, err)
		}
	}
	// Ensure a cf_<name> directory exists for every column family the engine opens — a CF with no
	// SSTables produced no objects to copy, but wavesdb.Open still expects its directory.
	for _, cf := range knownColumnFamilies {
		if err := os.MkdirAll(filepath.Join(dataDir, "cf_"+cf.Name()), 0o755); err != nil {
			return err
		}
	}
	// Install the LEAF's MANIFEST (the full cumulative table set as of the restored backup).
	leafPrefix := path.Join(chain[len(chain)-1], "nodes", memberID, "physical")
	if err := copyObject(objStore, path.Join(leafPrefix, "MANIFEST"), filepath.Join(dataDir, "MANIFEST")); err != nil {
		return fmt.Errorf("backup: install MANIFEST: %w", err)
	}
	// Reset collections raft bookkeeping in the restored store (§5.0).
	db, err := storage.OpenWavesdb(dataDir)
	if err != nil {
		return fmt.Errorf("backup: open restored data dir: %w", err)
	}
	defer func() { _ = db.Close() }()
	return collections.StripRaftBookkeeping(db)
}

// copyPrefixSSTables downloads every SSTable object under prefix (cf_<cf>/<id>.klog|.vlog) into dataDir,
// preserving the cf_<cf>/<file> layout. The MANIFEST is handled separately (the leaf's wins).
func copyPrefixSSTables(ctx context.Context, objStore ObjectStore, prefix, dataDir string) error {
	keys, err := objStore.List(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel := strings.TrimPrefix(k, prefix+"/")
		if rel == "MANIFEST" || rel == k { // skip the manifest and any non-prefixed stray
			continue
		}
		if err := copyObject(objStore, k, filepath.Join(dataDir, filepath.FromSlash(rel))); err != nil {
			return err
		}
	}
	return nil
}

// copyObject downloads one object to a local file (creating parent dirs).
func copyObject(objStore ObjectStore, key, destPath string) error {
	rc, err := objStore.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return f.Close()
}
