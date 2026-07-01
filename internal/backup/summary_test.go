package backup

import (
	"context"
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// TestSummaryFromIntent_CarriesFieldsNoCreds: the compact summary carries planes, aggregated size,
// destination descriptor, retain-until, parent, and PARTIAL + gaps — and NEVER raw credentials.
func TestSummaryFromIntent_CarriesFieldsNoCreds(t *testing.T) {
	in := &Intent{
		BackupID:      "bk-1",
		Status:        StatusPartial,
		StartedMs:     1000,
		FinishedMs:    2000,
		Parent:        "bk-0",
		Planes:        []Plane{PlaneLogical, PlanePhysical},
		Destination:   Descriptor{Bucket: "my-bucket", Prefix: "pfx", Region: "de", Endpoint: "s3.example", SecretName: "s3-secret"},
		RetainUntilMs: 9999,
		Gaps:          []string{"collections-shard:5", "member:m3"},
		PerNode: []NodeRecord{
			{MemberID: "m1", Bytes: 100, Done: true},
			{MemberID: "m2", Bytes: 250, Done: true},
		},
	}
	s := summaryFromIntent(in)

	if s.GetBackupId() != "bk-1" || s.GetStatus() != wavespanv1.BackupStatus_BACKUP_PARTIAL {
		t.Fatalf("id/status = %s/%v", s.GetBackupId(), s.GetStatus())
	}
	if len(s.GetPlanes()) != 2 || s.GetPlanes()[0] != wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL {
		t.Fatalf("planes = %v", s.GetPlanes())
	}
	if s.GetSizeBytes() != 350 {
		t.Fatalf("size_bytes = %d, want 350 (100+250)", s.GetSizeBytes())
	}
	if s.GetParent() != "bk-0" {
		t.Fatalf("parent = %q", s.GetParent())
	}
	if s.GetRetainUntilMs() != 9999 {
		t.Fatalf("retain_until_ms = %d", s.GetRetainUntilMs())
	}
	if !s.GetPartial() || len(s.GetGaps()) != 2 {
		t.Fatalf("partial/gaps = %v/%v", s.GetPartial(), s.GetGaps())
	}
	d := s.GetDestination()
	if d == nil || d.GetBucket() != "my-bucket" || d.GetPrefix() != "pfx" || d.GetRegion() != "de" || d.GetEndpoint() != "s3.example" {
		t.Fatalf("destination descriptor = %+v", d)
	}
	// The credential REFERENCE (secret name) may travel; raw access/secret keys must NEVER be present.
	if cr := d.GetCredential(); cr != nil {
		if cr.GetAccessKey() != "" || cr.GetSecretKey() != "" {
			t.Fatalf("SECURITY: summary leaked raw credentials: %+v", cr)
		}
		if cr.GetSecretName() != "s3-secret" {
			t.Fatalf("credential ref = %q, want s3-secret", cr.GetSecretName())
		}
	}
}

// TestListBackups_PopulatesEnrichedFields: end-to-end — a completed backup's summary from ListBackups
// carries the aggregated size and planes (default logical), not just id/status/times.
func TestListBackups_PopulatesEnrichedFields(t *testing.T) {
	objStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	nodes := buildCluster(t, objStore, "m1", "m2")
	coord := newCoord(t, objStore, meta, nodes, fakeAssigner{assignments: map[string]Selector{"m1": {}, "m2": {}}})

	ctx := context.Background()
	if _, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{}); err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}
	list, err := coord.ListBackups(ctx)
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("list len = %d, want 1", len(list))
	}
	s := list[0]
	if len(s.GetPlanes()) != 1 || s.GetPlanes()[0] != wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL {
		t.Fatalf("planes = %v, want [LOGICAL]", s.GetPlanes())
	}
	if s.GetSizeBytes() <= 0 {
		t.Fatalf("size_bytes = %d, want > 0 (aggregated from per-node bytes)", s.GetSizeBytes())
	}
	if s.GetPartial() {
		t.Fatalf("a full 2-node backup must not be partial: gaps=%v", s.GetGaps())
	}
}
