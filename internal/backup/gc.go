package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// defaultReconcileGraceMs is the age below which an object is never reaped as an orphan: a backup's
// objects, once written, are immune to orphan-GC for this long, so a sweep racing a just-finished (or
// in-flight) backup cannot destroy it. One hour by default.
const defaultReconcileGraceMs int64 = 60 * 60 * 1000

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

// StoreForIntent resolves the object store holding a backup's objects, from its persisted destination
// descriptor (Phase 3c Task 0). Returning an error aborts the sweep/delete for that backup.
type StoreForIntent func(in *Intent) (ObjectStore, error)

// SweepIntents is the lifecycle pass over the (low-cardinality) backup catalog: a RUNNING intent past
// its lease deadline transitions to FAILED with a retention deadline set; a terminal intent past its
// retention deadline is deleted (intent + objects in its OWN destination, via storeFor), unless a live
// incremental child still depends on it (then it is left for a later sweep, once the child is gone). It
// is idempotent — a second sweep finds the FAILED intent not-yet-due and the deleted intent absent — and
// every mutation is a meta-shard proposal (routed through the raft leader), so transitions are durable.
// retainMs is the retention window applied to a freshly-FAILED intent.
//
// This is a full-scan over ListIntents rather than a meta-shard due-index: backups are low-cardinality
// (the catalog holds tens to low-thousands of intents, not the millions of keys the budget/TTL due-index
// exists for), so a periodic scan is simpler and cheap. Callers gate it on meta-shard leadership.
func SweepIntents(ctx context.Context, store MetaStore, storeFor StoreForIntent, nowMs, retainMs int64) (SweepStats, error) {
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
			objStore, err := storeFor(in) // delete objects in the backup's OWN destination
			if err != nil {
				return stats, err
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

// ReconcileOptions configures orphan reconciliation. NowMs/GraceMs gate the age grace; DefaultKey is the
// node default destination's descriptor key (so a default-S3 backup isn't scanned twice); ClusterPrefix
// MUST be the dedicated backups root. Logger may be nil (→ slog.Default()).
type ReconcileOptions struct {
	StoreFor      StoreForIntent
	DefaultStore  ObjectStore
	DefaultKey    string
	ClusterPrefix string
	NowMs         int64
	GraceMs       int64
	Logger        *slog.Logger
}

// ReconcileOrphans deletes objects whose backup id (the first path segment after ClusterPrefix) has no
// live intent — debris from a failed or partially-written export, or from a backup whose intent was
// removed. It scans the default store AND each distinct destination store referenced by a live intent
// (Phase 3c Task 0), so alt-bucket debris of live-intent backups is reconciled in its own bucket. It
// never touches objects of a live backup (chain-aware retention keeps an ancestor's intent alive while a
// child depends on it). It returns the keys it deleted.
//
// FAIL-SAFE: an EMPTY intent catalog reaps NOTHING (never "everything is an orphan"), and an object
// younger than GraceMs is never reaped — so a sweep racing a just-finished/in-flight backup can't destroy
// it. Both guard the catastrophic case where a node's catalog view is empty or stale.
//
// LIMITATION: a fully-deleted alt-destination backup (no live intent points at its bucket) cannot be
// discovered — there is no record naming that bucket to scan. And an inline-credential destination's
// store can't be re-resolved (creds were never persisted; storeFor falls back to the default), so its
// alt bucket is not scanned. Default/named/secret-ref destinations with at least one live intent are
// reconciled.
//
// TOCTOU safety: the live set is a snapshot taken before listing; before deleting any candidate the
// intent is re-checked FRESH (GetIntent), so an in-flight backup that Began after the snapshot is never
// collected.
func ReconcileOrphans(ctx context.Context, store MetaStore, opt ReconcileOptions) ([]string, error) {
	logger := opt.Logger
	if logger == nil {
		logger = slog.Default()
	}
	intents, err := ListIntents(ctx, store)
	if err != nil {
		return nil, err
	}

	// FAIL-SAFE (critical): never reap against an empty/unconfirmed catalog. An empty ListIntents must NOT
	// be read as "every object is an orphan" — doing so destroyed a live COMPLETE backup when a node's
	// meta-shard catalog view was empty (e.g. a not-fully-joined meta replica). Keeping debris is the safe
	// error; deleting live backups is catastrophic. A genuinely empty cluster has no objects to reap anyway.
	if len(intents) == 0 {
		logger.Warn("backup: orphan reconcile skipped — empty intent catalog (fail-safe; nothing reaped)")
		return nil, nil
	}

	live := map[string]bool{}
	for _, in := range intents {
		live[in.BackupID] = true
	}

	// Distinct stores to scan: the default, plus each live intent's destination (deduped by descriptor;
	// the default destination's own key is pre-seeded so a default-S3 backup is not scanned twice).
	stores := []ObjectStore{opt.DefaultStore}
	seen := map[string]bool{"": true, opt.DefaultKey: true}
	for _, in := range intents {
		k := destinationKey(in.Destination)
		if k == "" || seen[k] {
			continue
		}
		s, ferr := opt.StoreFor(in)
		if ferr != nil {
			continue // unresolvable (e.g. inline creds) — its bucket can't be scanned; documented limitation
		}
		seen[k] = true
		stores = append(stores, s)
	}

	var deleted []string
	for _, s := range stores {
		d, rerr := reconcileStore(ctx, store, s, live, opt, logger)
		deleted = append(deleted, d...)
		if rerr != nil {
			return deleted, rerr
		}
	}
	return deleted, nil
}

// reconcileStore deletes orphan objects (backup id not in live, confirmed by a fresh re-check, and older
// than the age grace) from a single object store.
func reconcileStore(ctx context.Context, store MetaStore, objStore ObjectStore, live map[string]bool, opt ReconcileOptions, logger *slog.Logger) ([]string, error) {
	keys, err := objStore.List(opt.ClusterPrefix)
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
		id := backupIDOf(k, opt.ClusterPrefix)
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
		// AGE GRACE: never reap an object younger than the grace window — protects an in-flight or
		// just-finished backup from a racing sweep (the bug that destroyed a COMPLETE backup ~21s after it
		// finished). If the mtime can't be read, fail safe and KEEP the object.
		if opt.GraceMs > 0 {
			mt, merr := objStore.ModTime(k)
			if merr != nil {
				logger.Warn("backup: orphan reconcile keeping object (mtime unavailable)", "key", k, "err", merr)
				continue
			}
			if opt.NowMs-mt.UnixMilli() < opt.GraceMs {
				continue // too young to reap
			}
		}
		if err := objStore.Delete(k); err != nil {
			return deleted, err
		}
		deleted = append(deleted, k)
	}
	return deleted, nil
}

// destinationKey is a stable identity for a destination descriptor, used to dedup stores during orphan
// reconciliation. "" means the default store. Inline-credential destinations return "" (their store
// can't be re-resolved, so they fold into the default and their bucket is not separately scanned).
func destinationKey(d Descriptor) string {
	switch {
	case d.DefaultFS, d.SecretName == "inline":
		return ""
	case d.Name != "":
		return "name:" + d.Name
	case d.Bucket != "":
		return "s3:" + d.Endpoint + "/" + d.Bucket
	default:
		return ""
	}
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
