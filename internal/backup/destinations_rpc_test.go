package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/yannick/wavespan/internal/config"
	"google.golang.org/protobuf/proto"
)

// TestListDestinationsCarriesDescriptorsNoSecrets: ListDestinations reports the default + named
// destinations' descriptor fields (bucket/prefix/region/endpoint) and NEVER a credential — the wire type
// has no credential field, and the credential env-var references must not leak into any value either.
func TestListDestinationsCarriesDescriptorsNoSecrets(t *testing.T) {
	c := NewCoordinator(Config{
		BackupCfg: config.BackupConfig{
			AllowInlineDestinationCreds: true,
			DefaultDestination: config.BackupDestination{
				Bucket: "def-bkt", Prefix: "p", Region: "de", Endpoint: "s3.def", UseSSL: true,
				AccessKeyEnv: "DEF_AK_ENV", SecretKeyEnv: "DEF_SK_ENV",
			},
			NamedDestinations: []config.BackupDestination{
				{Name: "alt", Bucket: "alt-bkt", Endpoint: "s3.alt", Region: "us", UsePathStyle: true,
					AccessKeyEnv: "ALT_AK_ENV", SecretKeyEnv: "ALT_SK_ENV"},
			},
		},
	})

	res, err := c.ListDestinations(context.Background())
	if err != nil {
		t.Fatalf("ListDestinations: %v", err)
	}
	if res.GetDefaultIsFs() {
		t.Fatal("default with a bucket must not be FS")
	}
	if d := res.GetDefaultDestination(); d.GetBucket() != "def-bkt" || d.GetEndpoint() != "s3.def" || d.GetRegion() != "de" || !d.GetUseSsl() {
		t.Fatalf("default descriptor = %+v", d)
	}
	if !res.GetAllowInlineCreds() {
		t.Fatal("allow_inline_creds must reflect config")
	}
	if len(res.GetNamed()) != 1 || res.GetNamed()[0].GetName() != "alt" || res.GetNamed()[0].GetBucket() != "alt-bkt" || !res.GetNamed()[0].GetUsePathStyle() {
		t.Fatalf("named = %+v", res.GetNamed())
	}
	// No secrets: not even the credential env-var NAMES may appear anywhere in the marshaled result.
	b, err := proto.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"DEF_AK_ENV", "DEF_SK_ENV", "ALT_AK_ENV", "ALT_SK_ENV"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("SECURITY: credential reference %q leaked into ListDestinations result", secret)
		}
	}
}

// TestListDestinationsDefaultFS: an empty default bucket → default_is_fs true (local FS fallback).
func TestListDestinationsDefaultFS(t *testing.T) {
	c := NewCoordinator(Config{BackupCfg: config.BackupConfig{}})
	res, err := c.ListDestinations(context.Background())
	if err != nil {
		t.Fatalf("ListDestinations: %v", err)
	}
	if !res.GetDefaultIsFs() {
		t.Fatal("empty default bucket must report default_is_fs = true")
	}
	if len(res.GetNamed()) != 0 {
		t.Fatalf("no named destinations expected, got %d", len(res.GetNamed()))
	}
}
