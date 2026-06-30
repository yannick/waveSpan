package config

import "testing"

const backupYAML = `
clusterId: dev
memberId: node1
membership:
  runtime: docker
  seeds: ["node1:7700"]
security:
  insecureDevMode: true
backup:
  allowInlineDestinationCreds: true
  defaultDestination:
    bucket: primary-backups
    endpoint: s3.example.net
    region: de
    useSSL: true
  namedDestinations:
    - name: cold
      bucket: cold-bucket
      endpoint: s3.cold.net
      region: us
      accessKeyEnv: COLD_AK
      secretKeyEnv: COLD_SK
    - name: dr
      bucket: dr-bucket
      endpoint: s3.dr.net
`

func TestBackupConfigParse(t *testing.T) {
	cfg, err := Load(writeTemp(t, backupYAML), nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	d := cfg.Backup.DefaultDestination
	if d.Bucket != "primary-backups" || d.Endpoint != "s3.example.net" || d.Region != "de" || !d.UseSSL {
		t.Fatalf("default destination not parsed: %+v", d)
	}
	// Credential env-var names default for the default destination.
	if d.AccessKeyEnv != defaultBackupAccessKeyEnv || d.SecretKeyEnv != defaultBackupSecretKeyEnv {
		t.Fatalf("default cred env names = %q/%q, want %q/%q", d.AccessKeyEnv, d.SecretKeyEnv, defaultBackupAccessKeyEnv, defaultBackupSecretKeyEnv)
	}
	if !cfg.Backup.AllowInlineDestinationCreds {
		t.Fatalf("allowInlineDestinationCreds not parsed")
	}

	// Named lookup resolves; its own cred refs are kept; unknown errors.
	cold, ok := cfg.Backup.NamedDestination("cold")
	if !ok || cold.Bucket != "cold-bucket" || cold.AccessKeyEnv != "COLD_AK" || cold.SecretKeyEnv != "COLD_SK" {
		t.Fatalf("named 'cold' = %+v ok %v", cold, ok)
	}
	if _, ok := cfg.Backup.NamedDestination("dr"); !ok {
		t.Fatalf("named 'dr' not found")
	}
	if _, ok := cfg.Backup.NamedDestination("ghost"); ok {
		t.Fatalf("unknown destination 'ghost' resolved, want not-found")
	}
}

func TestBackupConfigEnvOverride(t *testing.T) {
	env := map[string]string{
		"WAVESPAN_BACKUP_BUCKET":   "env-bucket",
		"WAVESPAN_BACKUP_ENDPOINT": "s3.env.net",
		"WAVESPAN_BACKUP_USE_SSL":  "true",
	}
	cfg, err := Load(writeTemp(t, backupYAML), env)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	d := cfg.Backup.DefaultDestination
	if d.Bucket != "env-bucket" || d.Endpoint != "s3.env.net" || !d.UseSSL {
		t.Fatalf("env override not applied: %+v", d)
	}
}

func TestBackupConfigUnsetDefaultsToFS(t *testing.T) {
	// No backup block: default destination has no bucket → the node falls back to FS (Bucket == "").
	cfg, err := Load(writeTemp(t, validYAML), nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Backup.DefaultDestination.Bucket != "" {
		t.Fatalf("unset backup default bucket = %q, want empty (FS fallback)", cfg.Backup.DefaultDestination.Bucket)
	}
}
