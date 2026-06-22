package tunables

import (
	"path/filepath"
	"testing"
	"time"
)

func TestOverrideSetAppliesAndPersists(t *testing.T) {
	r := Default()
	var saved []Override
	o := NewOverrides(r, "node1", func(set []Override) { saved = set })

	v, restart, err := o.Set("ttl.sweepInterval", "5s", false)
	if err != nil {
		t.Fatal(err)
	}
	if restart {
		t.Error("ttl.sweepInterval is Hot, should not require restart")
	}
	if v == 0 {
		t.Error("expected non-zero version")
	}
	if got := r.Get("ttl.sweepInterval").Duration(); got != 5*time.Second {
		t.Errorf("value=%v want 5s", got)
	}
	if r.Get("ttl.sweepInterval").Source() != FromRuntime {
		t.Error("source should be runtime")
	}
	if len(saved) != 1 || saved[0].Key != "ttl.sweepInterval" {
		t.Errorf("persist not called with the override: %+v", saved)
	}

	// Static tunable reports requires-restart.
	_, restart, err = o.Set("storage.engine.writeBufferSize", "128MiB", false)
	if err != nil {
		t.Fatal(err)
	}
	if !restart {
		t.Error("storage.engine.writeBufferSize is Static, should require restart")
	}
}

func TestNodeLocalPin(t *testing.T) {
	r := Default()
	o := NewOverrides(r, "node1", nil)

	// node-local pin applies, is scoped "node", and is NOT advertised to gossip.
	if _, _, err := o.Set("ttl.batch", "42", true); err != nil {
		t.Fatal(err)
	}
	if got := r.Get("ttl.batch").Int(); got != 42 {
		t.Fatalf("local pin value = %d, want 42", got)
	}
	if o.Scope("ttl.batch") != "node" {
		t.Errorf("scope = %q, want node", o.Scope("ttl.batch"))
	}
	if len(o.GossipSet()) != 0 {
		t.Errorf("node-local pin must not be gossiped, got %d in GossipSet", len(o.GossipSet()))
	}

	// an incoming cluster delta (even newer) must NOT override a node-local pin.
	o.ApplyRemote([]Override{{Key: "ttl.batch", Value: "7", Version: 1 << 62, Origin: "node2"}})
	if got := r.Get("ttl.batch").Int(); got != 42 {
		t.Errorf("node-local pin should resist cluster delta, got %d", got)
	}

	// a cluster set (local=false) is gossiped and scoped "cluster".
	if _, _, err := o.Set("cache.idleTTL", "3m", false); err != nil {
		t.Fatal(err)
	}
	if o.Scope("cache.idleTTL") != "cluster" {
		t.Errorf("scope = %q, want cluster", o.Scope("cache.idleTTL"))
	}
	if len(o.GossipSet()) != 1 {
		t.Errorf("cluster override should be in GossipSet, got %d", len(o.GossipSet()))
	}
}

func TestOverrideLWWMerge(t *testing.T) {
	r := Default()
	o := NewOverrides(r, "node1", nil)

	// remote delta with a high version wins.
	o.ApplyRemote([]Override{{Key: "ttl.batch", Value: "100", Version: 50, Origin: "node2"}})
	if got := r.Get("ttl.batch").Int(); got != 100 {
		t.Fatalf("after remote apply = %d, want 100", got)
	}
	// an older delta is ignored.
	o.ApplyRemote([]Override{{Key: "ttl.batch", Value: "1", Version: 10, Origin: "node3"}})
	if got := r.Get("ttl.batch").Int(); got != 100 {
		t.Errorf("older delta should be ignored, got %d", got)
	}
	// a newer delta wins.
	o.ApplyRemote([]Override{{Key: "ttl.batch", Value: "7", Version: 99, Origin: "node3"}})
	if got := r.Get("ttl.batch").Int(); got != 7 {
		t.Errorf("newer delta should win, got %d", got)
	}
	// equal version, higher origin id wins.
	o.ApplyRemote([]Override{{Key: "ttl.batch", Value: "8", Version: 99, Origin: "node9"}})
	if got := r.Get("ttl.batch").Int(); got != 8 {
		t.Errorf("equal-version higher-origin should win, got %d", got)
	}
}

func TestOverrideSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config_overrides.json")
	r1 := Default()
	o1 := NewOverrides(r1, "node1", func(set []Override) {
		if err := SaveOverridesFile(path, set); err != nil {
			t.Fatal(err)
		}
	})
	if _, _, err := o1.Set("cache.idleTTL", "2m", false); err != nil {
		t.Fatal(err)
	}

	// reload into a fresh registry via the snapshot file.
	snap, err := LoadOverridesFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r2 := Default()
	o2 := NewOverrides(r2, "node1", nil)
	o2.LoadSnapshot(snap)
	if got := r2.Get("cache.idleTTL").Duration(); got != 2*time.Minute {
		t.Errorf("restored value = %v, want 2m", got)
	}
	if r2.Get("cache.idleTTL").Source() != FromRuntime {
		t.Error("restored override should be runtime-sourced")
	}
}

func TestLoadOverridesFileMissing(t *testing.T) {
	snap, err := LoadOverridesFile(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if len(snap) != 0 {
		t.Errorf("missing file should yield empty set, got %d", len(snap))
	}
}
