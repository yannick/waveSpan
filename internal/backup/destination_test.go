package backup

import (
	"strings"
	"testing"

	"github.com/yannick/wavespan/internal/config"
)

func testBackupConfig() config.BackupConfig {
	return config.BackupConfig{
		AllowInlineDestinationCreds: true,
		DefaultDestination: config.BackupDestination{
			Bucket: "default-bucket", Endpoint: "s3.default.net", Region: "de", UseSSL: true,
			AccessKeyEnv: "DEF_AK", SecretKeyEnv: "DEF_SK",
		},
		NamedDestinations: []config.BackupDestination{
			{Name: "cold", Bucket: "cold-bucket", Endpoint: "s3.cold.net", Region: "us", AccessKeyEnv: "COLD_AK", SecretKeyEnv: "COLD_SK"},
		},
	}
}

func TestResolveDestinationDefault(t *testing.T) {
	bc := testBackupConfig()
	env := map[string]string{"DEF_AK": "default-access", "DEF_SK": "default-secret"}
	get := func(k string) string { return env[k] }

	rd, desc, err := ResolveDestination(bc, DestinationSpec{}, get)
	if err != nil {
		t.Fatalf("resolve default: %v", err)
	}
	if rd.UseFS || rd.S3.Bucket != "default-bucket" || rd.S3.AccessKey != "default-access" || rd.S3.SecretKey != "default-secret" {
		t.Fatalf("resolved default = %+v, want S3 default-bucket with creds", rd)
	}
	// Descriptor carries only the env-var NAME, never the secret value.
	if desc.SecretName != "DEF_AK" {
		t.Fatalf("descriptor SecretName = %q, want the env ref DEF_AK", desc.SecretName)
	}
	assertNoSecret(t, desc, "default-access", "default-secret")
}

func TestResolveDestinationDefaultFS(t *testing.T) {
	bc := config.BackupConfig{} // no default bucket → FS fallback
	rd, _, err := ResolveDestination(bc, DestinationSpec{}, func(string) string { return "" })
	if err != nil {
		t.Fatalf("resolve fs default: %v", err)
	}
	if !rd.UseFS {
		t.Fatalf("empty default destination should resolve to FS, got %+v", rd)
	}
}

func TestResolveDestinationNamed(t *testing.T) {
	bc := testBackupConfig()
	env := map[string]string{"COLD_AK": "cold-access", "COLD_SK": "cold-secret"}
	get := func(k string) string { return env[k] }

	rd, desc, err := ResolveDestination(bc, DestinationSpec{Name: "cold"}, get)
	if err != nil {
		t.Fatalf("resolve named: %v", err)
	}
	if rd.S3.Bucket != "cold-bucket" || rd.S3.AccessKey != "cold-access" {
		t.Fatalf("named resolved = %+v, want cold-bucket with creds", rd)
	}
	if desc.Bucket != "cold-bucket" || desc.SecretName != "COLD_AK" {
		t.Fatalf("named descriptor = %+v", desc)
	}
	assertNoSecret(t, desc, "cold-access", "cold-secret")

	// Unknown named destination errors.
	if _, _, err := ResolveDestination(bc, DestinationSpec{Name: "ghost"}, get); err == nil {
		t.Fatalf("unknown named destination resolved, want error")
	}
}

// TestResolveDestinationNamedInNamedOnlyMode pins the two properties F3's stag deploy relies on: a named
// destination resolves its creds from its env refs even with inline creds DISABLED (named-only mode — the
// inline gate applies only to ad-hoc explicit destinations, never to pre-registered named ones), and its
// persisted descriptor carries Name (so the GC path re-resolves the alt bucket via storeForDescriptor's
// `d.Name != ""` branch) plus only the credential env-ref, never a raw key.
func TestResolveDestinationNamedInNamedOnlyMode(t *testing.T) {
	bc := testBackupConfig()
	bc.AllowInlineDestinationCreds = false // named-only mode
	env := map[string]string{"COLD_AK": "cold-access", "COLD_SK": "cold-secret"}

	rd, desc, err := ResolveDestination(bc, DestinationSpec{Name: "cold"}, func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("named resolution must work in named-only mode: %v", err)
	}
	if rd.S3.Bucket != "cold-bucket" || rd.S3.AccessKey != "cold-access" || rd.S3.SecretKey != "cold-secret" {
		t.Fatalf("named resolved = %+v, want cold-bucket with env-resolved creds", rd)
	}
	if desc.Name != "cold" {
		t.Fatalf("descriptor Name = %q, want \"cold\" (drives GC re-resolution via storeForDescriptor)", desc.Name)
	}
	assertNoSecret(t, desc, "cold-access", "cold-secret")
}

func TestResolveDestinationExplicitInline(t *testing.T) {
	bc := testBackupConfig()
	spec := DestinationSpec{
		Bucket: "adhoc", Endpoint: "s3.adhoc.net", Region: "fr",
		InlineAccessKey: "adhoc-access", InlineSecretKey: "TOP-SECRET-VALUE",
	}
	rd, desc, err := ResolveDestination(bc, spec, func(string) string { return "" })
	if err != nil {
		t.Fatalf("resolve explicit inline: %v", err)
	}
	// Transient creds carried in the resolved target.
	if rd.S3.Bucket != "adhoc" || rd.S3.SecretKey != "TOP-SECRET-VALUE" {
		t.Fatalf("explicit resolved = %+v, want adhoc with inline creds", rd)
	}
	// Descriptor records only the marker — never the raw secret.
	if desc.SecretName != "inline" {
		t.Fatalf("explicit descriptor SecretName = %q, want 'inline' marker", desc.SecretName)
	}
	assertNoSecret(t, desc, "adhoc-access", "TOP-SECRET-VALUE")

	// Named-only mode rejects inline creds.
	bc.AllowInlineDestinationCreds = false
	if _, _, err := ResolveDestination(bc, spec, func(string) string { return "" }); err == nil {
		t.Fatalf("inline creds accepted in named-only mode, want error")
	}
}

func TestResolveDestinationExplicitSecretRef(t *testing.T) {
	bc := testBackupConfig()
	env := map[string]string{"OPS_ACCESS_KEY": "ops-access", "OPS_SECRET_KEY": "ops-secret"}
	get := func(k string) string { return env[k] }
	spec := DestinationSpec{Bucket: "ops-bucket", Endpoint: "s3.ops.net", SecretRef: "OPS"}

	rd, desc, err := ResolveDestination(bc, spec, get)
	if err != nil {
		t.Fatalf("resolve explicit secret-ref: %v", err)
	}
	if rd.S3.AccessKey != "ops-access" || rd.S3.SecretKey != "ops-secret" {
		t.Fatalf("secret-ref creds not resolved: %+v", rd.S3)
	}
	if desc.SecretName != "OPS" {
		t.Fatalf("descriptor SecretName = %q, want the ref OPS", desc.SecretName)
	}
	assertNoSecret(t, desc, "ops-access", "ops-secret")
}

// assertNoSecret fails if any string field of the persisted descriptor contains a raw credential value.
func assertNoSecret(t *testing.T, desc Descriptor, secrets ...string) {
	t.Helper()
	fields := []string{desc.Name, desc.Bucket, desc.Prefix, desc.Region, desc.Endpoint, desc.SecretName}
	for _, f := range fields {
		for _, s := range secrets {
			if s != "" && strings.Contains(f, s) {
				t.Fatalf("descriptor field %q leaks secret %q", f, s)
			}
		}
	}
}
