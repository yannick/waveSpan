package config

import "testing"

func TestParsePeersAndPolicies(t *testing.T) {
	c := &Config{}
	env := map[string]string{
		"WAVESPAN_GLOBAL_MODE":              "active-active-async",
		"WAVESPAN_GLOBAL_PEERS":             "test-b@b1:7800, test-b@b2:7800",
		"WAVESPAN_KEEP_SIBLINGS_NAMESPACES": "siblings,notes",
	}
	c.applyGlobalEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })

	if !c.GlobalReplication.Enabled() {
		t.Fatal("global replication should be enabled")
	}
	if len(c.GlobalReplication.Peers) != 2 || c.GlobalReplication.Peers[0].ReplEndpoint != "b1:7800" {
		t.Fatalf("peers parsed wrong: %+v", c.GlobalReplication.Peers)
	}
	if c.ConflictPolicy("siblings") != "keep-siblings" {
		t.Fatalf("siblings namespace should use keep-siblings, got %q", c.ConflictPolicy("siblings"))
	}
	if c.ConflictPolicy("default") != "hlc-last-write-wins" {
		t.Fatalf("default namespace should use LWW, got %q", c.ConflictPolicy("default"))
	}
}
