package storage

import (
	"testing"

	"wavesdb"
)

// TestSyncModePlumbing pins the syncMode string → engine mapping: "full" must reach the engine as
// SyncFull (per-group-commit fsync), and an unknown string must fail open to SyncNone rather than
// silently claiming durability it doesn't deliver (design/37 P0.1).
func TestSyncModePlumbing(t *testing.T) {
	cases := map[string]wavesdb.SyncMode{
		"full":     wavesdb.SyncFull,
		"FULL ":    wavesdb.SyncFull,
		"interval": wavesdb.SyncInterval,
		"none":     wavesdb.SyncNone,
		"bogus":    wavesdb.SyncNone,
	}
	for in, want := range cases {
		if got := (EngineOptions{SyncMode: in}).cfOptions().Sync; got != want {
			t.Fatalf("SyncMode %q -> %v, want %v", in, got, want)
		}
	}
}

func TestWavesdbStore(t *testing.T) {
	runConformance(t, func(t *testing.T) LocalStore {
		s, err := OpenWavesdb(t.TempDir())
		if err != nil {
			t.Fatalf("open wavesdb store: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
