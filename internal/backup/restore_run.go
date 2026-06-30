package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// restoreMarkerKey records, in CFSys, the backup a node was reconstituted from. Its presence makes the
// bootstrap one-shot: a node does not re-restore on every boot (unless WAVESPAN_RESTORE_FORCE).
const restoreMarkerKey = "/sys/restored_from"

// RunBootstrapRestore reconstitutes a node's storage from a backup before it opens for serving (Phase 3c).
// It MUST run before the main store is opened: physical DR injects SSTables at the file level, and a
// logical restore writes into a fresh store and resets collections raft bookkeeping — neither is safe
// against a live store. It is one-shot (guarded by the CFSys marker) and selects the path by intent +
// shape (master spec §7): clone or a shard-count change → logical clone/re-shard; same-shape DR → physical
// when the backup has a physical plane, else logical same-shape.
//
// NOTE (backup catalog reset): both paths reset the collections meta shard (StripRaftBookkeeping), which
// clears the Intent catalog (subBackup) — a restored/cloned cluster starts with NO backup history
// or schedule. The S3 backups themselves remain; the operator re-registers backup intents/schedule after
// restore. For a clone this is correct (don't inherit the source's schedule); for same-cluster DR the
// catalog can be rebuilt by listing S3 / re-registering.
func RunBootstrapRestore(dataDir, memberID string, rc *RestoreConfig, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if !rc.Force {
		done, from, err := restoreAlreadyApplied(dataDir)
		if err != nil {
			return err
		}
		if done {
			logger.Info("backup: bootstrap-restore already applied; skipping", "from", from)
			return nil
		}
	}

	objStore, err := rc.OpenSource()
	if err != nil {
		return fmt.Errorf("backup: open restore source: %w", err)
	}
	cm, err := ReadClusterManifest(objStore, rc.BackupID)
	if err != nil {
		return fmt.Errorf("backup: read cluster.manifest %q: %w", rc.BackupID, err)
	}

	// clone or a shape change → logical; same-shape DR → physical when available, else logical same-shape.
	logical := rc.Intent == IntentClone || rc.Shards > 0 || !containsString(cm.Planes, "physical")
	now := time.Now().UnixMilli()

	if logical {
		if !containsString(cm.Planes, "logical") {
			return fmt.Errorf("backup: %q has no logical plane; cannot %s-restore from it", rc.BackupID, rc.Intent)
		}
		ri := RestoreInfo{
			RestoreWallClockMs:    now,
			Clone:                 rc.Intent == IntentClone,
			CollectionsDataShards: rc.Shards,
		}
		if err := withStore(dataDir, func(dst storage.LocalStore) error {
			if err := RestoreBootstrapLogical(dst, objStore, rc.BackupID, ri); err != nil {
				return err
			}
			return markRestored(dst, rc.BackupID)
		}); err != nil {
			return err
		}
		logger.Info("backup: logical bootstrap-restore complete", "backup", rc.BackupID, "intent", rc.Intent, "shards", rc.Shards)
		return nil
	}

	member, err := matchSourceMember(cm, memberID)
	if err != nil {
		return err
	}
	if err := RestoreBootstrapPhysical(context.Background(), objStore, rc.BackupID, member, dataDir); err != nil {
		return err
	}
	if err := withStore(dataDir, func(dst storage.LocalStore) error { return markRestored(dst, rc.BackupID) }); err != nil {
		return err
	}
	logger.Info("backup: physical bootstrap-restore complete", "backup", rc.BackupID, "source_member", member)
	return nil
}

// matchSourceMember resolves THIS node to a source node in the backup topology by stable identity (member
// id). Same-shape DR assumes a node restores from its own counterpart in the backup.
//
// NOTE: it matches by MemberID only. The topology also carries StorageUUID (recorded in 3c Task 0) but it
// is not consulted here — correct while member ids are stable. If member ids are ever reassigned (a node
// taking over another's id with a different storage identity), the match would need to also verify
// StorageUUID to avoid restoring the wrong node's checkpoint.
func matchSourceMember(cm *ClusterManifest, memberID string) (string, error) {
	for _, te := range cm.SourceTopology {
		if te.MemberID == memberID {
			return te.MemberID, nil
		}
	}
	return "", fmt.Errorf("backup: this node %q has no matching source node in backup %q topology; "+
		"physical DR restores a node from its own counterpart (use a logical clone to fork)", memberID, cm.BackupID)
}

// restoreAlreadyApplied reports whether dataDir already carries the one-shot restore marker.
func restoreAlreadyApplied(dataDir string) (done bool, from string, err error) {
	err = withStore(dataDir, func(s storage.LocalStore) error {
		v, ok, gerr := s.Get(storage.CFSys, []byte(restoreMarkerKey))
		if gerr != nil {
			return gerr
		}
		done, from = ok, string(v)
		return nil
	})
	return done, from, err
}

// markRestored writes the one-shot marker recording which backup the node was restored from.
func markRestored(s storage.LocalStore, backupID string) error {
	return s.Put(storage.CFSys, []byte(restoreMarkerKey), []byte(backupID))
}

// withStore opens dataDir as a wavesdb store, runs fn, and closes it — used for the short-lived opens the
// bootstrap needs (marker check / write, logical restore) before the node's main store opens for serving.
// It opens with plain OpenWavesdb (default engine options), not the serving store's
// OpenWavesdbWith(engineOptions(tun)) — the difference is runtime tunables (memtable sizes, gogc), not the
// on-disk format, and this store is always closed before the serving store opens, so it is harmless.
func withStore(dataDir string, fn func(storage.LocalStore) error) error {
	s, err := storage.OpenWavesdb(dataDir)
	if err != nil {
		return fmt.Errorf("backup: open store at %q: %w", dataDir, err)
	}
	defer func() { _ = s.Close() }()
	return fn(s)
}
