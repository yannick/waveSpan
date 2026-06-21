package storage

import "testing"

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
