package backup

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/config"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// TestCoordinatorAltDestination drives a backup at an explicit alternate destination: objects land in the
// ALT store (not the default), the persisted intent records the destination DESCRIPTOR (bucket + cred
// reference) but never the raw credentials, and the cluster.manifest is written to the alt store.
func TestCoordinatorAltDestination(t *testing.T) {
	ctx := context.Background()
	defaultStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	altStore, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := newFakeMetaStore()
	// Single node: alt-destination backups are restricted to single-node clusters in 3e (the multi-node
	// guard is exercised separately in TestCoordinatorAltDestinationMultiNodeRejected).
	nodes := buildCluster(t, defaultStore, "m1") // member store defaults to defaultStore

	// Creds live only in env; the opener maps the resolved destination to an FS store (no real S3 in tests).
	env := map[string]string{"OPS_ACCESS_KEY": "ops-access-SECRET", "OPS_SECRET_KEY": "ops-secret-SECRET"}
	openStore := func(rd ResolvedDestination) (ObjectStore, error) {
		if rd.UseFS {
			return defaultStore, nil
		}
		if rd.S3.Bucket == "alt-bucket" {
			// (creds would be passed to objstore.NewS3 here; for the test we just route to the alt FS store)
			return altStore, nil
		}
		return nil, fmt.Errorf("unexpected bucket %q", rd.S3.Bucket)
	}

	coord := NewCoordinator(Config{
		Self:      "m1",
		Meta:      meta,
		ObjStore:  defaultStore,
		Roster:    fakeRoster{live: []string{"m1"}},
		ClientFor: func(id string) (NodeClient, error) { return nodes[id], nil },
		Assigner:  AllExportAssigner{},
		BackupCfg: config.BackupConfig{AllowInlineDestinationCreds: true},
		Getenv:    func(k string) string { return env[k] },
		OpenStore: openStore,
	})

	spec := &wavespanv1.BackupSpec{
		Planes: []wavespanv1.BackupPlane{wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL},
		Destination: &wavespanv1.Destination{
			Bucket:     "alt-bucket",
			Endpoint:   "s3.alt.net",
			Credential: &wavespanv1.CredentialRef{SecretName: "OPS"},
		},
	}
	id, err := coord.BeginBackup(ctx, spec)
	if err != nil {
		t.Fatalf("BeginBackup: %v", err)
	}

	st, _ := coord.BackupStatus(ctx, id)
	if st.GetStatus() != wavespanv1.BackupStatus_BACKUP_COMPLETE {
		t.Fatalf("status = %v, want COMPLETE", st.GetStatus())
	}

	// Objects (cluster manifest + a node's CF object) land in the ALT store, not the default.
	if ok, _ := altStore.Exists(id + "/cluster.manifest.json"); !ok {
		t.Fatalf("cluster manifest not in the alt store")
	}
	if ok, _ := defaultStore.Exists(id + "/cluster.manifest.json"); ok {
		t.Fatalf("cluster manifest wrongly written to the default store")
	}
	if ok, _ := altStore.Exists(id + "/nodes/m1/cf/kv_data"); !ok {
		t.Fatalf("node CF object not in the alt store")
	}

	// The intent records the destination DESCRIPTOR (bucket + cred reference) but NO raw credentials.
	in, found, _ := GetIntent(ctx, meta, id)
	if !found || in.Destination.Bucket != "alt-bucket" || in.Destination.SecretName != "OPS" {
		t.Fatalf("intent destination = %+v, want alt-bucket + ref OPS", in.Destination)
	}
	blob, err := MarshalIntent(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"ops-access-SECRET", "ops-secret-SECRET"} {
		if bytes.Contains(blob, []byte(secret)) {
			t.Fatalf("persisted intent leaks credential %q", secret)
		}
	}
}

// TestCoordinatorAltDestinationMultiNodeRejected proves an alt destination is refused on a multi-node
// cluster (it would otherwise be silently incomplete — remote nodes export to their own default store),
// while the default destination is still allowed with multiple nodes.
func TestCoordinatorAltDestinationMultiNodeRejected(t *testing.T) {
	ctx := context.Background()
	defaultStore, _ := objstore.NewFS(t.TempDir())
	meta := newFakeMetaStore()
	nodes := buildCluster(t, defaultStore, "m1", "m2")
	coord := NewCoordinator(Config{
		Self:      "m1",
		Meta:      meta,
		ObjStore:  defaultStore,
		Roster:    fakeRoster{live: []string{"m1", "m2"}},
		ClientFor: func(id string) (NodeClient, error) { return nodes[id], nil },
		Assigner:  AllExportAssigner{},
		BackupCfg: config.BackupConfig{AllowInlineDestinationCreds: true},
		Getenv:    func(string) string { return "" },
	})

	// Alt (explicit) destination on a 2-node cluster → rejected.
	altSpec := &wavespanv1.BackupSpec{
		Planes:      []wavespanv1.BackupPlane{wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL},
		Destination: &wavespanv1.Destination{Bucket: "alt-bucket", Endpoint: "s3.alt.net", Credential: &wavespanv1.CredentialRef{SecretName: "OPS"}},
	}
	if _, err := coord.BeginBackup(ctx, altSpec); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("alt destination on multi-node = %v, want FailedPrecondition", err)
	}

	// Named destination on a 2-node cluster → also rejected.
	namedSpec := &wavespanv1.BackupSpec{Destination: &wavespanv1.Destination{Name: "cold"}}
	if _, err := coord.BeginBackup(ctx, namedSpec); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("named destination on multi-node = %v, want FailedPrecondition", err)
	}

	// Default destination (no destination in the spec) is unaffected by the guard.
	if _, err := coord.BeginBackup(ctx, &wavespanv1.BackupSpec{}); err != nil {
		t.Fatalf("default destination on multi-node rejected: %v", err)
	}
}
