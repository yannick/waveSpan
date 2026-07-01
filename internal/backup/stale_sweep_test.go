package backup

import (
	"context"
	"testing"
)

// splitMetaStore returns DELIBERATELY DIVERGENT catalogs for the linearizable (ListBlobs) and stale
// (ListBlobsStale) reads, so a test can prove which read path a caller takes.
type splitMetaStore struct {
	*fakeMetaStore // ListBlobs (linearizable) = the base catalog
	stale          map[string][]byte
}

func (s splitMetaStore) ListBlobsStale(context.Context) (map[string][]byte, error) {
	out := make(map[string][]byte, len(s.stale))
	for k, v := range s.stale {
		out[k] = v
	}
	return out, nil
}

// TestSweepScanUsesStaleUIUsesLinearizable pins the F1(b) routing: the UI/RPC enumeration (ListIntents)
// reads linearizable, while the lifecycle sweep enumeration (ListIntentsStale) reads stale — so the
// periodic sweep does not ReadIndex-wake the meta shard and an idle meta shard can quiesce. Correctness of
// the stale scan holds: it still enumerates the (best-effort) catalog view.
func TestSweepScanUsesStaleUIUsesLinearizable(t *testing.T) {
	ctx := context.Background()
	base := newFakeMetaStore()
	if err := PutIntent(ctx, base, &Intent{BackupID: "bk-linearizable", SchemaVersion: intentSchemaVersion}); err != nil {
		t.Fatal(err)
	}
	staleBlob, err := MarshalIntent(&Intent{BackupID: "bk-stale", SchemaVersion: intentSchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	store := splitMetaStore{fakeMetaStore: base, stale: map[string][]byte{"bk-stale": staleBlob}}

	// UI/RPC path (linearizable) sees the base catalog.
	lin, err := ListIntents(ctx, store)
	if err != nil || len(lin) != 1 || lin[0].BackupID != "bk-linearizable" {
		t.Fatalf("ListIntents (UI, linearizable) = %+v err %v, want [bk-linearizable]", lin, err)
	}
	// Sweep path (stale) sees the stale view — proving the sweep does not take the linearizable read.
	stale, err := ListIntentsStale(ctx, store)
	if err != nil || len(stale) != 1 || stale[0].BackupID != "bk-stale" {
		t.Fatalf("ListIntentsStale (sweep) = %+v err %v, want [bk-stale]", stale, err)
	}
}
