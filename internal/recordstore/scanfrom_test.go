package recordstore

import (
	"fmt"
	"testing"
)

// TestScanRecordsFromPaginates verifies the bounded cursor scan returns every key exactly once
// across pages and reports nil cursor at the end (so the intra-AE sweep is incremental, not O(all)).
func TestScanRecordsFromPaginates(t *testing.T) {
	s := newTestStore(t)
	const n = 25
	for i := 0; i < n; i++ {
		putLocal(t, s, "default", []byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}

	seen := map[string]bool{}
	var cursor []byte
	pages := 0
	for {
		recs, next, err := s.ScanRecordsFrom("default", cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, r := range recs {
			k := string(r.GetLogicalKey())
			if seen[k] {
				t.Fatalf("key %q returned twice across pages", k)
			}
			seen[k] = true
		}
		if next == nil {
			break
		}
		cursor = next
		if pages > n {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct keys across pages, got %d", n, len(seen))
	}
	if pages < 2 {
		t.Fatalf("expected multiple bounded pages, got %d", pages)
	}
}
