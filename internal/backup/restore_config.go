package backup

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"wavesdb/objstore"
)

// RestoreIntent selects how a node reconstitutes from a backup.
type RestoreIntent string

const (
	// IntentDR restores same-shape into the SAME logical cluster (disaster recovery): physical fast-path.
	IntentDR RestoreIntent = "dr"
	// IntentClone forks a NEW cluster identity from the backup: logical clone / re-shard.
	IntentClone RestoreIntent = "clone"
)

// RestoreConfig is the parsed bootstrap-restore request (env-driven). The source object store is one of
// FS (dev/tests) or S3; its credentials come from the WAVESPAN_BACKUP_* env (never inlined here).
type RestoreConfig struct {
	BackupID string
	Intent   RestoreIntent
	Shards   uint64 // target collections shard count for a re-shard clone (0 = same shape)
	Force    bool   // re-restore even if the one-shot marker is present

	scheme string // "fs" | "s3"
	fsDir  string
	s3     objstore.S3Config
}

// ParseRestoreConfig reads the restore request from the environment. ok is false (no error) when
// WAVESPAN_RESTORE_FROM is unset — the node boots normally. WAVESPAN_RESTORE_FROM is a URL:
//
//	fs:///abs/path/to/root/<backupID>      — local FS object store rooted at the parent dir (dev/tests)
//	s3://bucket/prefix.../<backupID>       — S3; endpoint/region/creds/use_ssl from WAVESPAN_BACKUP_*
//
// WAVESPAN_RESTORE_INTENT=dr|clone (default dr); WAVESPAN_RESTORE_SHARDS=<N> (re-shard target, clone
// only); WAVESPAN_RESTORE_FORCE=1 re-runs even past the one-shot marker.
func ParseRestoreConfig(getenv func(string) (string, bool)) (*RestoreConfig, bool, error) {
	src, ok := getenv("WAVESPAN_RESTORE_FROM")
	if !ok || src == "" {
		return nil, false, nil
	}
	rc := &RestoreConfig{Intent: IntentDR}

	switch {
	case strings.HasPrefix(src, "fs://"):
		p := strings.TrimPrefix(src, "fs://") // fs:///a/b/<id> → /a/b/<id>
		dir, id := path.Split(strings.TrimRight(p, "/"))
		if id == "" || dir == "" {
			return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_FROM %q lacks a /<backupID>", src)
		}
		rc.scheme, rc.fsDir, rc.BackupID = "fs", strings.TrimRight(dir, "/"), id
	case strings.HasPrefix(src, "s3://"):
		rest := strings.TrimPrefix(src, "s3://")
		bucket, keyPath, found := strings.Cut(rest, "/")
		if !found || bucket == "" || keyPath == "" {
			return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_FROM %q must be s3://bucket/prefix/<backupID>", src)
		}
		prefix, id := path.Split(strings.TrimRight(keyPath, "/"))
		if id == "" {
			return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_FROM %q lacks a /<backupID>", src)
		}
		rc.scheme, rc.BackupID = "s3", id
		rc.s3 = objstore.S3Config{
			Bucket:   bucket,
			Prefix:   strings.TrimRight(prefix, "/"),
			Endpoint: envOr(getenv, "WAVESPAN_BACKUP_ENDPOINT", ""),
			Region:   envOr(getenv, "WAVESPAN_BACKUP_REGION", ""),
			AccessKey: envOr(getenv, "WAVESPAN_BACKUP_ACCESS_KEY", ""),
			SecretKey: envOr(getenv, "WAVESPAN_BACKUP_SECRET_KEY", ""),
		}
		if v, _ := getenv("WAVESPAN_BACKUP_USE_SSL"); v == "true" || v == "1" {
			rc.s3.UseSSL = true
		}
		if v, _ := getenv("WAVESPAN_BACKUP_USE_PATH_STYLE"); v == "true" || v == "1" {
			rc.s3.UsePathStyle = true
		}
	default:
		return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_FROM %q has an unsupported scheme (want fs:// or s3://)", src)
	}

	if v, ok := getenv("WAVESPAN_RESTORE_INTENT"); ok && v != "" {
		switch RestoreIntent(v) {
		case IntentDR, IntentClone:
			rc.Intent = RestoreIntent(v)
		default:
			return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_INTENT %q invalid (want dr|clone)", v)
		}
	}
	if v, ok := getenv("WAVESPAN_RESTORE_SHARDS"); ok && v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, false, fmt.Errorf("backup: WAVESPAN_RESTORE_SHARDS %q is not a number: %w", v, err)
		}
		rc.Shards = n
	}
	if v, _ := getenv("WAVESPAN_RESTORE_FORCE"); v == "true" || v == "1" {
		rc.Force = true
	}
	return rc, true, nil
}

func envOr(getenv func(string) (string, bool), key, def string) string {
	if v, ok := getenv(key); ok {
		return v
	}
	return def
}

// OpenSource opens the source object store the backup is read from.
func (rc *RestoreConfig) OpenSource() (ObjectStore, error) {
	switch rc.scheme {
	case "fs":
		return objstore.NewFS(rc.fsDir)
	case "s3":
		return objstore.NewS3(rc.s3)
	default:
		return nil, fmt.Errorf("backup: restore source scheme %q not opened", rc.scheme)
	}
}

// LoadClusterManifest reads the cluster.manifest for backupID from the source object store (topology,
// planes, namespace inventory, parent chain pointer).
func LoadClusterManifest(objStore ObjectStore, backupID string) (*ClusterManifest, error) {
	return ReadClusterManifest(objStore, backupID)
}
