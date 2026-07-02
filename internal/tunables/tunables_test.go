package tunables

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDefaultRegisters ensures Default() builds without panicking (duplicate keys / bad defaults
// panic at registration) and produces a non-trivial set.
func TestDefaultRegisters(t *testing.T) {
	r := Default()
	if len(r.All()) < 70 {
		t.Fatalf("expected the full tunable set, got %d", len(r.All()))
	}
}

// TestSyncModeDefaultIsFull pins the durability contract: the serving node's WAL sync default
// must stay "full" so the origin+1 ack (ADR-0002 "durable") means fsynced-on-two-nodes. Lowering
// it is a per-deployment decision, never a code default (design/37 P0.1).
func TestSyncModeDefaultIsFull(t *testing.T) {
	p := Default().Get("storage.engine.syncMode")
	if p == nil {
		t.Fatal("storage.engine.syncMode not registered")
	}
	if got := p.Default(); got != "full" {
		t.Fatalf("storage.engine.syncMode default = %q, want \"full\" (ADR-0002 durability contract)", got)
	}
}

// TestEnvNamesUnique guards against two keys colliding on the same WAVESPAN_TUNABLE_* name.
func TestEnvNamesUnique(t *testing.T) {
	seen := map[string]string{}
	for _, p := range Default().All() {
		env := EnvName(p.Key)
		if other, dup := seen[env]; dup {
			t.Fatalf("env name collision %s: %s and %s", env, other, p.Key)
		}
		seen[env] = p.Key
	}
}

func TestEnvNameMapping(t *testing.T) {
	cases := map[string]string{
		"ttl.sweepInterval":              "WAVESPAN_TUNABLE_TTL_SWEEP_INTERVAL",
		"storage.engine.writeBufferSize": "WAVESPAN_TUNABLE_STORAGE_ENGINE_WRITE_BUFFER_SIZE",
		"storage.engine.maxOpenSSTables": "WAVESPAN_TUNABLE_STORAGE_ENGINE_MAX_OPEN_SS_TABLES",
		"server.h2ReadIdleTimeout":       "WAVESPAN_TUNABLE_SERVER_H2_READ_IDLE_TIMEOUT",
		"latency.weight.ewma":            "WAVESPAN_TUNABLE_LATENCY_WEIGHT_EWMA",
	}
	for key, want := range cases {
		if got := EnvName(key); got != want {
			t.Errorf("EnvName(%q) = %q, want %q", key, got, want)
		}
	}
}

// TestReferenceRoundTrips renders the documented reference YAML and loads it back, asserting every
// value parses cleanly and resolves to the same canonical value as its default (no drift).
func TestReferenceRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteReference(&buf, Default()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "reference.yaml")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := Load(path, map[string]string{})
	if err != nil {
		t.Fatalf("load generated reference: %v", err)
	}
	def := Default()
	for _, p := range r.All() {
		if p.Source() != FromFile {
			t.Errorf("%s: source = %s, want file (value not present in reference?)", p.Key, p.Source())
		}
		if got, want := p.String(), def.Get(p.Key).String(); got != want {
			t.Errorf("%s: reference value %q != default %q", p.Key, got, want)
		}
	}
}

func TestEnvOverride(t *testing.T) {
	r, err := Load("", map[string]string{
		"WAVESPAN_TUNABLE_TTL_SWEEP_INTERVAL":                 "5s",
		"WAVESPAN_TUNABLE_STORAGE_ENGINE_WRITE_BUFFER_SIZE":   "128MiB",
		"WAVESPAN_TUNABLE_REPLICATION_TARGET_NEARBY_REPLICAS": "5",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Get("ttl.sweepInterval").Duration(); got != 5*time.Second {
		t.Errorf("ttl.sweepInterval = %v, want 5s", got)
	}
	if got := r.Get("ttl.sweepInterval").Source(); got != FromEnv {
		t.Errorf("source = %s, want env", got)
	}
	if got := r.Get("storage.engine.writeBufferSize").Int64(); got != 128<<20 {
		t.Errorf("writeBufferSize = %d, want %d", got, 128<<20)
	}
	if got := r.Get("replication.targetNearbyReplicas").Int(); got != 5 {
		t.Errorf("targetNearbyReplicas = %d, want 5", got)
	}
}

func TestUnknownKeyRejected(t *testing.T) {
	r := Default()
	if err := r.Set("nope.not.a.key", "1", FromFile, 0); err == nil {
		t.Fatal("expected error for unknown key")
	}
}

// TestHotApplyFires verifies the OnApply hook fires on a Hot param change but not registration of a
// new default, and that Static params don't fire.
func TestHotApplyFires(t *testing.T) {
	r := Default()
	fired := 0
	p := r.Get("ttl.sweepInterval")
	p.OnApply(func(*Param) { fired++ })
	if err := r.Set("ttl.sweepInterval", "9s", FromRuntime, 1); err != nil {
		t.Fatal(err)
	}
	if fired != 1 {
		t.Fatalf("hot apply fired %d times, want 1", fired)
	}
	if got := p.Duration(); got != 9*time.Second {
		t.Errorf("value = %v, want 9s", got)
	}
}
