package backup

import (
	"path/filepath"
	"testing"

	"wavesdb/objstore"
)

func envMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestParseRestoreConfigFS(t *testing.T) {
	rc, ok, err := ParseRestoreConfig(envMap(map[string]string{
		"WAVESPAN_RESTORE_FROM":   "fs:///var/backups/bk-123",
		"WAVESPAN_RESTORE_INTENT": "clone",
		"WAVESPAN_RESTORE_SHARDS": "8",
	}))
	if err != nil || !ok {
		t.Fatalf("parse fs: ok %v err %v", ok, err)
	}
	if rc.BackupID != "bk-123" || rc.scheme != "fs" || rc.fsDir != "/var/backups" {
		t.Fatalf("fs config = %+v", rc)
	}
	if rc.Intent != IntentClone || rc.Shards != 8 {
		t.Fatalf("intent/shards = %v/%d", rc.Intent, rc.Shards)
	}
}

func TestParseRestoreConfigS3(t *testing.T) {
	rc, ok, err := ParseRestoreConfig(envMap(map[string]string{
		"WAVESPAN_RESTORE_FROM":      "s3://my-bucket/wavespan/prod/bk-9",
		"WAVESPAN_BACKUP_ENDPOINT":   "s3.de.io.cloud.ovh.net",
		"WAVESPAN_BACKUP_REGION":     "de",
		"WAVESPAN_BACKUP_ACCESS_KEY": "AK",
		"WAVESPAN_BACKUP_SECRET_KEY": "SK",
		"WAVESPAN_BACKUP_USE_SSL":    "true",
	}))
	if err != nil || !ok {
		t.Fatalf("parse s3: ok %v err %v", ok, err)
	}
	if rc.BackupID != "bk-9" || rc.scheme != "s3" {
		t.Fatalf("s3 backupID/scheme = %q/%q", rc.BackupID, rc.scheme)
	}
	if rc.s3.Bucket != "my-bucket" || rc.s3.Prefix != "wavespan/prod" || rc.s3.Endpoint != "s3.de.io.cloud.ovh.net" || !rc.s3.UseSSL {
		t.Fatalf("s3 cfg = %+v", rc.s3)
	}
	if rc.s3.AccessKey != "AK" || rc.s3.SecretKey != "SK" {
		t.Fatalf("s3 creds not resolved from env")
	}
}

func TestParseRestoreConfigUnset(t *testing.T) {
	_, ok, err := ParseRestoreConfig(envMap(map[string]string{}))
	if ok || err != nil {
		t.Fatalf("unset RESTORE_FROM = ok %v err %v, want false nil", ok, err)
	}
}

func TestParseRestoreConfigBad(t *testing.T) {
	for _, bad := range []map[string]string{
		{"WAVESPAN_RESTORE_FROM": "http://x/bk"},                                       // bad scheme
		{"WAVESPAN_RESTORE_FROM": "fs:///"},                                            // no backupID
		{"WAVESPAN_RESTORE_FROM": "s3://bucket-only"},                                  // no key path / backupID
		{"WAVESPAN_RESTORE_FROM": "fs:///a/bk", "WAVESPAN_RESTORE_INTENT": "sideways"}, // bad intent
		{"WAVESPAN_RESTORE_FROM": "fs:///a/bk", "WAVESPAN_RESTORE_SHARDS": "lots"},     // bad shards
	} {
		if _, _, err := ParseRestoreConfig(envMap(bad)); err == nil {
			t.Fatalf("expected error for %v", bad)
		}
	}
}

func TestLoadClusterManifest(t *testing.T) {
	dir := t.TempDir()
	objStore, err := objstore.NewFS(filepath.Join(dir, "backups"))
	if err != nil {
		t.Fatal(err)
	}
	cm := &ClusterManifest{
		FormatVersion:      clusterManifestFormatVersion,
		BackupID:           "bk-load",
		Planes:             []string{"logical"},
		SourceTopology:     []TopologyEntry{{MemberID: "m1", StorageUUID: "uuid-m1"}},
		NamespaceInventory: []string{"app"},
		PerNode:            []PerNodeRef{{MemberID: "m1", Ref: "bk-load/nodes/m1/node.manifest.json"}},
		Status:             "COMPLETE",
	}
	if err := WriteClusterManifest(objStore, cm); err != nil {
		t.Fatal(err)
	}
	got, err := LoadClusterManifest(objStore, "bk-load")
	if err != nil {
		t.Fatalf("LoadClusterManifest: %v", err)
	}
	if got.BackupID != "bk-load" || len(got.SourceTopology) != 1 || got.SourceTopology[0].StorageUUID != "uuid-m1" {
		t.Fatalf("loaded manifest = %+v", got)
	}
}
