package config

// BackupConfig configures backup destinations (design/backup phase 3e). When the default destination has
// no bucket, the node falls back to a local FS object store under <storage.Path>/backups (dev). Object
// store credentials are never stored as plaintext here: a destination references the NAMES of the env
// vars that hold its access/secret keys, resolved at use time.
type BackupConfig struct {
	// DefaultDestination is written to when a BackupSpec omits a destination (the common case).
	DefaultDestination BackupDestination `yaml:"defaultDestination"`
	// NamedDestinations are operator-pre-registered alternates a request can select by name.
	NamedDestinations []BackupDestination `yaml:"namedDestinations"`
	// AllowInlineDestinationCreds permits a request to carry inline (transient) credentials for an ad-hoc
	// destination. When false, only the default and named destinations are usable (named-only mode).
	AllowInlineDestinationCreds bool `yaml:"allowInlineDestinationCreds"`
}

// BackupDestination describes an object-store target. An empty Bucket selects the local FS fallback. The
// credentials live in the env vars named by AccessKeyEnv/SecretKeyEnv (resolved at use time, never held
// in this struct or persisted in a backup's intent/manifest).
type BackupDestination struct {
	Name         string `yaml:"name"`
	Bucket       string `yaml:"bucket"`
	Prefix       string `yaml:"prefix"`
	Region       string `yaml:"region"`
	Endpoint     string `yaml:"endpoint"` // host:port, no scheme
	UseSSL       bool   `yaml:"useSSL"`
	UsePathStyle bool   `yaml:"usePathStyle"`
	AccessKeyEnv string `yaml:"accessKeyEnv"`
	SecretKeyEnv string `yaml:"secretKeyEnv"`
}

// Default env-var names for the default destination's credentials.
const (
	defaultBackupAccessKeyEnv = "WAVESPAN_BACKUP_ACCESS_KEY"
	defaultBackupSecretKeyEnv = "WAVESPAN_BACKUP_SECRET_KEY"
)

// NamedDestination returns the registered named destination, or ok=false if none matches.
func (b BackupConfig) NamedDestination(name string) (BackupDestination, bool) {
	for _, d := range b.NamedDestinations {
		if d.Name == name {
			return d, true
		}
	}
	return BackupDestination{}, false
}

// applyBackupEnv layers WAVESPAN_BACKUP_* overrides onto the default destination (the named alternates
// come from YAML). Only the non-secret descriptor fields are read here; the access/secret keys
// themselves are resolved later, from the env vars named by AccessKeyEnv/SecretKeyEnv.
func (c *Config) applyBackupEnv(get func(string) (string, bool)) {
	if v, ok := get("WAVESPAN_BACKUP_BUCKET"); ok {
		c.Backup.DefaultDestination.Bucket = v
	}
	if v, ok := get("WAVESPAN_BACKUP_ENDPOINT"); ok {
		c.Backup.DefaultDestination.Endpoint = v
	}
	if v, ok := get("WAVESPAN_BACKUP_REGION"); ok {
		c.Backup.DefaultDestination.Region = v
	}
	if v, ok := get("WAVESPAN_BACKUP_PREFIX"); ok {
		c.Backup.DefaultDestination.Prefix = v
	}
	if v, ok := get("WAVESPAN_BACKUP_USE_SSL"); ok {
		c.Backup.DefaultDestination.UseSSL = v == "true" || v == "1"
	}
	if v, ok := get("WAVESPAN_BACKUP_USE_PATH_STYLE"); ok {
		c.Backup.DefaultDestination.UsePathStyle = v == "true" || v == "1"
	}
	if v, ok := get("WAVESPAN_BACKUP_ALLOW_INLINE_CREDS"); ok {
		c.Backup.AllowInlineDestinationCreds = v == "true" || v == "1"
	}
}

// applyBackupDefaults fills the default destination's credential env-var names when unset.
func (c *Config) applyBackupDefaults() {
	if c.Backup.DefaultDestination.AccessKeyEnv == "" {
		c.Backup.DefaultDestination.AccessKeyEnv = defaultBackupAccessKeyEnv
	}
	if c.Backup.DefaultDestination.SecretKeyEnv == "" {
		c.Backup.DefaultDestination.SecretKeyEnv = defaultBackupSecretKeyEnv
	}
}
