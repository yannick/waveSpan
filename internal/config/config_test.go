package config

import (
	"os"
	"path/filepath"
	"testing"
)

const validYAML = `
clusterId: dev
memberId: node1
storage:
  path: /var/lib/wavespan
  engine: wavesdb
membership:
  runtime: docker
  seeds: ["node1:7700", "node2:7700"]
replication:
  policyRef: local-cache-default
security:
  insecureDevMode: true
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeTemp(t, validYAML), nil)
	if err != nil {
		t.Fatalf("valid config failed to load: %v", err)
	}
	if cfg.ClusterID != "dev" || cfg.MemberID != "node1" {
		t.Fatalf("identity not parsed: %+v", cfg)
	}
	if cfg.Membership.Runtime != RuntimeDocker {
		t.Fatalf("runtime not parsed: %q", cfg.Membership.Runtime)
	}
	if len(cfg.Membership.Seeds) != 2 || cfg.Membership.Seeds[0] != "node1:7700" {
		t.Fatalf("seeds not parsed: %v", cfg.Membership.Seeds)
	}
	if !cfg.Security.InsecureDevMode {
		t.Fatalf("security.insecureDevMode not parsed")
	}
}

func TestDefaultsApplied(t *testing.T) {
	body := `
clusterId: dev
memberId: node1
membership:
  runtime: docker
  seeds: ["self:7700"]
`
	cfg, err := Load(writeTemp(t, body), nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.Engine != "wavesdb" {
		t.Fatalf("storage.engine default not applied: %q", cfg.Storage.Engine)
	}
	if cfg.Admin.Listen == "" {
		t.Fatalf("admin.listen default not applied")
	}
}

func TestEnvOverridesFile(t *testing.T) {
	env := map[string]string{
		"WAVESPAN_CLUSTER_ID":   "prod",
		"WAVESPAN_MEMBER_ID":    "nodeX",
		"WAVESPAN_SEEDS":        "a:7700,b:7700,c:7700",
		"WAVESPAN_ADMIN_LISTEN": ":9999",
	}
	cfg, err := Load(writeTemp(t, validYAML), env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterID != "prod" || cfg.MemberID != "nodeX" {
		t.Fatalf("env did not override identity: %+v", cfg)
	}
	if len(cfg.Membership.Seeds) != 3 || cfg.Membership.Seeds[2] != "c:7700" {
		t.Fatalf("WAVESPAN_SEEDS comma split failed: %v", cfg.Membership.Seeds)
	}
	if cfg.Admin.Listen != ":9999" {
		t.Fatalf("env admin listen override failed: %q", cfg.Admin.Listen)
	}
}

func TestKubernetesRuntimeFromEnvOnly(t *testing.T) {
	// No YAML file path: build entirely from env (kubernetes-style injection).
	env := map[string]string{
		"WAVESPAN_CLUSTER_ID": "prod-use1",
		"WAVESPAN_MEMBER_ID":  "wavespan-data-3",
		"WAVESPAN_RUNTIME":    "kubernetes",
	}
	cfg, err := Load("", env)
	if err != nil {
		t.Fatalf("kubernetes env config failed: %v", err)
	}
	if cfg.Membership.Runtime != RuntimeKubernetes {
		t.Fatalf("runtime not kubernetes: %q", cfg.Membership.Runtime)
	}
}

func TestSecurityTLSAndTuningParsed(t *testing.T) {
	body := `
clusterId: dev
memberId: node1
membership: {runtime: docker, seeds: ["s:7700"]}
security:
  certFile: /certs/tls.crt
  keyFile: /certs/tls.key
  caFile: /certs/ca.crt
  transport:
    maxIdleConnsPerHost: 128
    idleConnTimeoutSeconds: 900
`
	cfg, err := Load(writeTemp(t, body), nil)
	if err != nil {
		t.Fatalf("tls config failed to load: %v", err)
	}
	if cfg.Security.CertFile != "/certs/tls.crt" || cfg.Security.CAFile != "/certs/ca.crt" {
		t.Fatalf("tls material not parsed: %+v", cfg.Security)
	}
	if cfg.Security.Transport.MaxIdleConnsPerHost == nil || *cfg.Security.Transport.MaxIdleConnsPerHost != 128 {
		t.Fatalf("transport tuning not parsed: %+v", cfg.Security.Transport)
	}
	if cfg.Security.Transport.IdleConnTimeoutSeconds == nil || *cfg.Security.Transport.IdleConnTimeoutSeconds != 900 {
		t.Fatalf("idle timeout override not parsed: %+v", cfg.Security.Transport)
	}
}

func TestTLSCertEnvOverride(t *testing.T) {
	env := map[string]string{
		"WAVESPAN_TLS_CERT_FILE": "/run/tls.crt",
		"WAVESPAN_TLS_KEY_FILE":  "/run/tls.key",
		"WAVESPAN_TLS_CA_FILE":   "/run/ca.crt",
	}
	cfg, err := Load(writeTemp(t, validYAML), env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Security.CertFile != "/run/tls.crt" || cfg.Security.KeyFile != "/run/tls.key" || cfg.Security.CAFile != "/run/ca.crt" {
		t.Fatalf("tls env overrides failed: %+v", cfg.Security)
	}
}

func TestIncompleteTLSMaterialRejected(t *testing.T) {
	body := `
clusterId: dev
memberId: node1
membership: {runtime: docker, seeds: ["s:7700"]}
security:
  certFile: /certs/tls.crt
`
	if _, err := Load(writeTemp(t, body), nil); err == nil {
		t.Fatal("partial mTLS material (cert without key/ca) must fail validation")
	}
}

func TestFailFast(t *testing.T) {
	cases := map[string]string{
		"empty clusterId": `
memberId: node1
membership: {runtime: docker, seeds: ["s:7700"]}
`,
		"empty memberId": `
clusterId: dev
membership: {runtime: docker, seeds: ["s:7700"]}
`,
		"unknown runtime": `
clusterId: dev
memberId: node1
membership: {runtime: nomad, seeds: ["s:7700"]}
`,
		"docker without seeds": `
clusterId: dev
memberId: node1
membership: {runtime: docker, seeds: []}
`,
		"unknown storage engine": `
clusterId: dev
memberId: node1
storage: {engine: rocksdb}
membership: {runtime: docker, seeds: ["s:7700"]}
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeTemp(t, body), nil); err == nil {
				t.Fatalf("expected fail-fast error for %q, got nil", name)
			}
		})
	}
}
