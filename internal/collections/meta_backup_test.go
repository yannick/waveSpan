package collections

import (
	"bytes"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// TestMetaBackupCatalog drives the meta SM's BackupIntent catalog ops directly (mirrors the meta SM test
// harness): opBackupPut stores a blob keyed by backup id, metaBackupGetQuery reads it back, the
// list query returns every blob, and opBackupDelete removes one without disturbing the others or the
// range directory (subData) living under a different sub-space.
func TestMetaBackupCatalog(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	s := newMetaSM(mem, 1)
	if _, err := s.Open(nil); err != nil {
		t.Fatalf("Open: %v", err)
	}

	blobA := []byte("intent-A-blob")
	blobB := []byte("intent-B-blob")

	// A range-directory put (subData) must coexist untouched with backup blobs (subBackup).
	put := func(idx uint64, c metaCommand) {
		if _, err := s.Update([]sm.Entry{{Index: idx, Cmd: encodeMetaCommand(c)}}); err != nil {
			t.Fatalf("Update %d: %v", idx, err)
		}
	}
	put(1, metaCommand{Op: opMetaPut, Start: []byte("a"), End: []byte("m"), ShardID: 7})
	put(2, metaCommand{Op: opBackupPut, Start: []byte("bk-a"), End: blobA})
	put(3, metaCommand{Op: opBackupPut, Start: []byte("bk-b"), End: blobB})

	// Get one blob.
	v, err := s.Lookup(metaBackupGetQuery{Key: []byte("bk-a")})
	if err != nil {
		t.Fatalf("Lookup get bk-a: %v", err)
	}
	if got, _ := v.([]byte); !bytes.Equal(got, blobA) {
		t.Fatalf("get bk-a = %q, want %q", got, blobA)
	}

	// Missing key -> nil blob.
	v, err = s.Lookup(metaBackupGetQuery{Key: []byte("bk-missing")})
	if err != nil {
		t.Fatalf("Lookup get missing: %v", err)
	}
	if got, _ := v.([]byte); got != nil {
		t.Fatalf("get missing = %q, want nil", got)
	}

	// List returns both.
	lv, err := s.Lookup(metaBackupListQuery{})
	if err != nil {
		t.Fatalf("Lookup list: %v", err)
	}
	m, _ := lv.(map[string][]byte)
	if len(m) != 2 || !bytes.Equal(m["bk-a"], blobA) || !bytes.Equal(m["bk-b"], blobB) {
		t.Fatalf("list = %v, want bk-a/bk-b blobs", m)
	}

	// The range directory is untouched by backup ops.
	rl, err := s.Lookup(metaListQuery{})
	if err != nil {
		t.Fatalf("Lookup ranges: %v", err)
	}
	if ranges, _ := rl.([]rangeEntry); len(ranges) != 1 || ranges[0].ShardID != 7 {
		t.Fatalf("ranges = %v, want one range -> shard 7", rl)
	}

	// Delete bk-a; bk-b survives.
	put(4, metaCommand{Op: opBackupDelete, Start: []byte("bk-a")})
	v, err = s.Lookup(metaBackupGetQuery{Key: []byte("bk-a")})
	if err != nil {
		t.Fatalf("Lookup get after delete: %v", err)
	}
	if got, _ := v.([]byte); got != nil {
		t.Fatalf("bk-a present after delete: %q", got)
	}
	lv, _ = s.Lookup(metaBackupListQuery{})
	if m, _ := lv.(map[string][]byte); len(m) != 1 || !bytes.Equal(m["bk-b"], blobB) {
		t.Fatalf("after delete list = %v, want only bk-b", lv)
	}
}
