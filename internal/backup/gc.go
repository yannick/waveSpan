package backup

import (
	"context"
	"fmt"
	"strings"
)

// This file implements Phase 3d lifecycle GC: the leader-gated intent sweep (lease-expiry → FAILED,
// retention deletion), chain-aware DeleteBackup (intent + objects), and S3 orphan reconciliation.
//
// NOTE (alt-destination GC gap, 3e): all three functions take a single objStore — the node's DEFAULT
// destination as wired by the coordinator. A backup written to a non-default (named/explicit) destination
// keeps its objects in that alt bucket, which these passes do NOT touch: retention/delete remove the
// intent but leave the alt objects orphaned, and ReconcileOrphans only scans the default store. Handling
// alt destinations needs per-descriptor store re-resolution (the coordinator's storeForDescriptor), which
// lands in 3c Task 0; until then alt-destination backups are restricted to single-node clusters (see the
// BeginBackup guard).
//
// Object layout (3a/3b): every object a backup writes lives under "<backupID>/..." — logical CFs and
// node manifests at <id>/nodes/<member>/..., physical SSTables at <id>/nodes/<member>/physical/..., the
// cluster manifest at <id>/cluster.manifest.json. An incremental writes ONLY its delta SSTables under
// ITS OWN prefix; the base's objects stay under the base's prefix. So "a backup's objects" is exactly
// the set under its prefix, and deleting that prefix never touches a chain's shared base objects.

// isTerminal reports whether a status is a finished state (no further phase transitions).
func isTerminal(s Status) bool {
	return s == StatusComplete || s == StatusPartial || s == StatusFailed
}

// SweepStats reports what a sweep changed (for observability / test assertions).
type SweepStats struct {
	Failed  int // RUNNING intents lease-expired to FAILED
	Deleted int // terminal intents past retention that were deleted
}

// SweepIntents is the lifecycle pass over the (low-cardinality) backup catalog: a RUNNING intent past
// its lease deadline transitions to FAILED with a retention deadline set; a terminal intent past its
// retention deadline is deleted (intent + objects), unless a live incremental child still depends on it
// (then it is left for a later sweep, once the child is gone). It is idempotent — a second sweep finds
// the FAILED intent not-yet-due and the deleted intent absent — and every mutation is a meta-shard
// proposal (routed through the raft leader), so transitions are durable. retainMs is the retention window
// applied to a freshly-FAILED intent.
//
// This is a full-scan over ListIntents rather than a meta-shard due-index: backups are low-cardinality
// (the catalog holds tens to low-thousands of intents, not the millions of keys the budget/TTL due-index
// exists for), so a periodic scan is simpler and cheap. Callers gate it on meta-shard leadership.
func SweepIntents(ctx context.Context, store MetaStore, objStore ObjectStore, nowMs, retainMs int64) (SweepStats, error) {
	intents, err := ListIntents(ctx, store)
	if err != nil {
		return SweepStats{}, err
	}
	live := map[string]bool{}
	childrenOf := map[string][]string{}
	for _, in := range intents {
		live[in.BackupID] = true
		if in.Parent != "" {
			childrenOf[in.Parent] = append(childrenOf[in.Parent], in.BackupID)
		}
	}

	var stats SweepStats
	for _, in := range intents {
		switch {
		case in.Status == StatusRunning && in.LeaseDeadlineMs > 0 && nowMs > in.LeaseDeadlineMs:
			in.Status = StatusFailed
			in.FinishedMs = nowMs
			in.RetainUntilMs = nowMs + retainMs
			if err := PutIntent(ctx, store, in); err != nil {
				return stats, err
			}
			stats.Failed++
		case isTerminal(in.Status) && in.RetainUntilMs > 0 && nowMs > in.RetainUntilMs:
			if hasLiveChild(in.BackupID, childrenOf, live) {
				continue // a live incremental depends on this base; defer deletion until it is gone
			}
			deleted, err := DeleteBackup(ctx, store, objStore, in.BackupID, false)
			if err != nil {
				return stats, err
			}
			if deleted {
				stats.Deleted++
				live[in.BackupID] = false
			}
		}
	}
	return stats, nil
}

// hasLiveChild reports whether any recorded child of backupID is still a live intent.
func hasLiveChild(backupID string, childrenOf map[string][]string, live map[string]bool) bool {
	for _, ch := range childrenOf[backupID] {
		if live[ch] {
			return true
		}
	}
	return false
}

// DeleteBackup removes a backup's intent and its object-store objects, chain-aware. It refuses (error)
// when a live incremental child depends on the backup, unless force is set — which cascades, deleting
// the dependent children (leaf→base) first. It returns deleted=false for an unknown id (idempotent).
func DeleteBackup(ctx context.Context, store MetaStore, objStore ObjectStore, backupID string, force bool) (bool, error) {
	_, found, err := GetIntent(ctx, store, backupID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	children, err := liveChildren(ctx, store, backupID)
	if err != nil {
		return false, err
	}
	if len(children) > 0 {
		if !force {
			return false, fmt.Errorf("backup: %q has live incremental children %v; delete them first or use force", backupID, children)
		}
		for _, ch := range children {
			if _, err := DeleteBackup(ctx, store, objStore, ch, true); err != nil {
				return false, err
			}
		}
	}
	if err := deletePrefix(objStore, backupID+"/"); err != nil {
		return false, err
	}
	if err := DeleteIntent(ctx, store, backupID); err != nil {
		return false, err
	}
	return true, nil
}

// liveChildren returns the ids of live intents whose parent is backupID.
func liveChildren(ctx context.Context, store MetaStore, backupID string) ([]string, error) {
	intents, err := ListIntents(ctx, store)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, in := range intents {
		if in.Parent == backupID {
			out = append(out, in.BackupID)
		}
	}
	return out, nil
}

// deletePrefix removes every object under prefix.
func deletePrefix(objStore ObjectStore, prefix string) error {
	keys, err := objStore.List(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := objStore.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileOrphans deletes objects under clusterPrefix whose backup id (the first path segment after the
// prefix) has no live intent — debris from a failed or partially-written export, or from a backup whose
// intent was already removed. It never touches objects of a live backup (chain-aware retention keeps an
// ancestor's intent alive while a child depends on it, so all chain members' objects are retained). It
// returns the keys it deleted.
//
// TOCTOU safety: the live set is a snapshot taken before listing objects, so a backup that Begins in
// between would have objects on disk but be absent from the snapshot. Before deleting any candidate we
// therefore re-check the intent FRESH (GetIntent) and skip it if it now exists — an in-flight backup's
// objects are never collected. The fresh check is memoised per backup id, so a backup with many objects
// costs one extra read, not one per object.
//
// clusterPrefix MUST be the dedicated backups root (the node points objstore at <storagePath>/backups),
// so every first path segment under it is a backup id. Do NOT point this at a shared/foreign bucket
// root — it would treat unrelated top-level keys as orphan backups and delete them.
func ReconcileOrphans(ctx context.Context, store MetaStore, objStore ObjectStore, clusterPrefix string) ([]string, error) {
	intents, err := ListIntents(ctx, store)
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, in := range intents {
		live[in.BackupID] = true
	}
	keys, err := objStore.List(clusterPrefix)
	if err != nil {
		return nil, err
	}
	orphan := map[string]bool{} // memoised per-id decision after the fresh re-check
	isOrphan := func(id string) (bool, error) {
		if v, ok := orphan[id]; ok {
			return v, nil
		}
		if live[id] {
			orphan[id] = false
			return false, nil
		}
		_, found, err := GetIntent(ctx, store, id) // fresh re-check: did a backup Begin since the snapshot?
		if err != nil {
			return false, err
		}
		orphan[id] = !found
		return !found, nil
	}
	var deleted []string
	for _, k := range keys {
		id := backupIDOf(k, clusterPrefix)
		if id == "" {
			continue
		}
		dead, err := isOrphan(id)
		if err != nil {
			return deleted, err
		}
		if !dead {
			continue
		}
		if err := objStore.Delete(k); err != nil {
			return deleted, err
		}
		deleted = append(deleted, k)
	}
	return deleted, nil
}

// backupIDOf extracts the backup id (first path segment) from an object key, after stripping
// clusterPrefix. Returns "" if the key has no segment.
func backupIDOf(key, clusterPrefix string) string {
	rest := strings.TrimPrefix(key, clusterPrefix)
	rest = strings.TrimPrefix(rest, "/")
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[:i]
	}
	return rest
}
