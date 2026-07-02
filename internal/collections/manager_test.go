package collections

import "testing"

// TestDefaultTunablesIntraRegionClock pins the staging-validated Raft clock (design/33 §3,
// design/37 P1.7): a 5ms base tick with the same absolute heartbeat (50ms) and election (500ms)
// timing as the old 50ms WAN clock. Out-of-the-box clusters must get the fast clock; WAN
// deployments opt out via WAVESPAN_COLLECTIONS_RTT_MS.
func TestDefaultTunablesIntraRegionClock(t *testing.T) {
	d := DefaultTunables()
	if d.RTTMillisecond != 5 {
		t.Fatalf("RTTMillisecond = %d, want 5 (intra-region clock, design/33)", d.RTTMillisecond)
	}
	if hb := d.HeartbeatRTT * d.RTTMillisecond; hb != 50 {
		t.Fatalf("heartbeat = %dms, want 50ms", hb)
	}
	if el := d.ElectionRTT * d.RTTMillisecond; el != 500 {
		t.Fatalf("election = %dms, want 500ms", el)
	}
	if d.MaxInMemLogSize != 0 {
		t.Fatalf("MaxInMemLogSize default = %d, want 0 (dragonboat unlimited)", d.MaxInMemLogSize)
	}
}

// TestShardConfigCarriesTunables ensures the per-shard dragonboat config actually receives the
// tunables (a knob that exists but isn't plumbed is how the slow clock shipped in the first place).
func TestShardConfigCarriesTunables(t *testing.T) {
	tun := Tunables{RTTMillisecond: 7, ElectionRTT: 20, HeartbeatRTT: 2, MaxInMemLogSize: 1 << 20}.withDefaults()
	m := &Manager{tun: tun}
	cfg := m.shardConfig(3, 9)
	if cfg.ElectionRTT != 20 || cfg.HeartbeatRTT != 2 {
		t.Fatalf("shard config clock = %d/%d, want 20/2", cfg.ElectionRTT, cfg.HeartbeatRTT)
	}
	if cfg.MaxInMemLogSize != 1<<20 {
		t.Fatalf("shard config MaxInMemLogSize = %d, want 1MiB", cfg.MaxInMemLogSize)
	}
}
