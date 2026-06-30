package backup

import (
	"bytes"
	"context"
	"encoding/gob"
	"reflect"
	"sort"
	"testing"
)

// TestIntentRoundTrip pins the durable codec: every field of a fully-populated BackupIntent survives
// Marshal→Unmarshal unchanged (including the Selector maps, the assignment plan, and per-node records).
func TestIntentRoundTrip(t *testing.T) {
	in := &BackupIntent{
		BackupID:           "bk-2026-0001",
		FrontierT:          1234567890,
		CaptureWallClockMs: 1719720000000,
		Selection:          Selector{Namespaces: Set("app", "logs"), Graphs: Set("g1")},
		Planes:             []Plane{PlaneLogical, PlanePhysical},
		Parent:             "bk-2026-0000",
		Destination:        Descriptor{Name: "primary", Bucket: "backups", Prefix: "wavespan/", Region: "us-east-1", UseSSL: true, SecretName: "s3-creds"},
		Status:             StatusRunning,
		Phase:              PhaseExport,
		LeaseDeadlineMs:    1719720600000,
		RetainUntilMs:      1722312000000,
		PerNode: []NodeRecord{
			{MemberID: "m1", Phase: PhaseExport, Objects: 7, Bytes: 4096, Done: true, SubManifestKey: "bk/nodes/m1/node.manifest.json", HeldRanges: []string{"a-m"}},
			{MemberID: "m2", Phase: PhasePrepare, HeldRanges: []string{"m-z"}},
		},
		Gaps:        []string{"x-y"},
		StartedMs:   1719720000000,
		FinishedMs:  0,
		Assignments: map[string]Selector{"m1": {Namespaces: Set("app")}, "m2": {Namespaces: Set("logs")}},
	}

	blob, err := MarshalIntent(in)
	if err != nil {
		t.Fatalf("MarshalIntent: %v", err)
	}
	got, err := UnmarshalIntent(blob)
	if err != nil {
		t.Fatalf("UnmarshalIntent: %v", err)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, in)
	}
}

// TestIntentForwardCompatible proves the gob codec tolerates a blob written by a newer node that carries
// a field this version does not know: the extra field is ignored, the known fields decode intact.
func TestIntentForwardCompatible(t *testing.T) {
	type backupIntentFuture struct {
		BackupID  string
		FrontierT int64
		Status    Status
		Phase     Phase
		NewField  string // a field a future version added; the current reader must ignore it
	}
	var buf bytes.Buffer
	future := backupIntentFuture{BackupID: "bk-future", FrontierT: 42, Status: StatusComplete, Phase: PhaseCommit, NewField: "from-the-future"}
	if err := gob.NewEncoder(&buf).Encode(&future); err != nil {
		t.Fatalf("encode future: %v", err)
	}
	got, err := UnmarshalIntent(buf.Bytes())
	if err != nil {
		t.Fatalf("UnmarshalIntent of future blob: %v", err)
	}
	if got.BackupID != "bk-future" || got.FrontierT != 42 || got.Status != StatusComplete || got.Phase != PhaseCommit {
		t.Fatalf("forward-compat decode lost known fields: %+v", got)
	}
}

// fakeMetaStore is an in-memory MetaStore for the helper-level tests.
type fakeMetaStore struct{ m map[string][]byte }

func newFakeMetaStore() *fakeMetaStore { return &fakeMetaStore{m: map[string][]byte{}} }

func (f *fakeMetaStore) PutBlob(_ context.Context, k string, b []byte) error {
	f.m[k] = append([]byte(nil), b...)
	return nil
}
func (f *fakeMetaStore) GetBlob(_ context.Context, k string) ([]byte, bool, error) {
	b, ok := f.m[k]
	return b, ok, nil
}
func (f *fakeMetaStore) ListBlobs(_ context.Context) (map[string][]byte, error) {
	out := make(map[string][]byte, len(f.m))
	for k, v := range f.m {
		out[k] = v
	}
	return out, nil
}
func (f *fakeMetaStore) DeleteBlob(_ context.Context, k string) error {
	delete(f.m, k)
	return nil
}

// TestIntentHelpers exercises Put/Get/List/Delete over a MetaStore.
func TestIntentHelpers(t *testing.T) {
	ctx := context.Background()
	store := newFakeMetaStore()

	if _, found, err := GetIntent(ctx, store, "missing"); err != nil || found {
		t.Fatalf("GetIntent(missing) = found %v err %v, want not-found nil", found, err)
	}

	a := &BackupIntent{BackupID: "bk-a", Status: StatusRunning, Phase: PhasePrepare}
	b := &BackupIntent{BackupID: "bk-b", Status: StatusComplete, Phase: PhaseCommit}
	if err := PutIntent(ctx, store, a); err != nil {
		t.Fatalf("PutIntent a: %v", err)
	}
	if err := PutIntent(ctx, store, b); err != nil {
		t.Fatalf("PutIntent b: %v", err)
	}

	got, found, err := GetIntent(ctx, store, "bk-a")
	if err != nil || !found || got.Status != StatusRunning {
		t.Fatalf("GetIntent(bk-a) = %+v found %v err %v", got, found, err)
	}

	list, err := ListIntents(ctx, store)
	if err != nil {
		t.Fatalf("ListIntents: %v", err)
	}
	ids := []string{}
	for _, in := range list {
		ids = append(ids, in.BackupID)
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(ids, []string{"bk-a", "bk-b"}) {
		t.Fatalf("ListIntents ids = %v, want [bk-a bk-b]", ids)
	}

	if err := DeleteIntent(ctx, store, "bk-a"); err != nil {
		t.Fatalf("DeleteIntent: %v", err)
	}
	if _, found, _ := GetIntent(ctx, store, "bk-a"); found {
		t.Fatalf("bk-a still present after delete")
	}
}
