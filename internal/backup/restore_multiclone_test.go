package backup

import (
	"io"
	"testing"

	"github.com/yannick/wavespan/internal/storage"
)

// readObject reads an object's full bytes (test helper).
func readObject(t *testing.T, objStore ObjectStore, key string) []byte {
	t.Helper()
	rc, err := objStore.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return b
}

// TestMultiCloneFromOneBackup forks two independent clones from one immutable logical backup (§5.1): both
// receive the data, each keeps its OWN storage identity, the two are independent (a write to one is not
// seen by the other), and the backup's objects are unmutated by the restores.
func TestMultiCloneFromOneBackup(t *testing.T) {
	objStore := seedLogicalBackup(t, "bk-fork", 4)

	// Snapshot a backup object before any restore, to prove immutability afterwards.
	manKey := ClusterManifestKey("bk-fork")
	nodeManKey := "bk-fork/nodes/m1/node.manifest.json"
	beforeMan := readObject(t, objStore, manKey)
	beforeNode := readObject(t, objStore, nodeManKey)

	clone := func(uuid string) storage.LocalStore {
		dst, err := storage.OpenWavesdb(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dst.Close() })
		mustPut(t, dst, storage.CFSys, []byte(storageIdentityKey), []byte(uuid))
		if err := RestoreBootstrapLogical(dst, objStore, "bk-fork", RestoreInfo{Clone: true}); err != nil {
			t.Fatalf("clone %s: %v", uuid, err)
		}
		return dst
	}

	c1 := clone("CLONE-1")
	c2 := clone("CLONE-2")

	// Both clones have the data.
	for i, c := range []storage.LocalStore{c1, c2} {
		if v, ok, _ := c.Get(storage.CFKVData, kvKey("app", "k1")); !ok || string(v) != "kvval" {
			t.Fatalf("clone %d missing kv data: ok=%v v=%q", i+1, ok, v)
		}
	}
	// Each kept its OWN identity.
	if v, _, _ := c1.Get(storage.CFSys, []byte(storageIdentityKey)); string(v) != "CLONE-1" {
		t.Fatalf("clone 1 identity = %q, want CLONE-1", v)
	}
	if v, _, _ := c2.Get(storage.CFSys, []byte(storageIdentityKey)); string(v) != "CLONE-2" {
		t.Fatalf("clone 2 identity = %q, want CLONE-2", v)
	}
	// Independent: a write to clone 1 is not visible in clone 2.
	mustPut(t, c1, storage.CFKVData, kvKey("app", "only1"), []byte("x"))
	if _, ok, _ := c2.Get(storage.CFKVData, kvKey("app", "only1")); ok {
		t.Fatal("clones are not independent: clone 2 saw clone 1's write")
	}

	// The backup objects are unmutated by the restores (immutable source).
	if string(readObject(t, objStore, manKey)) != string(beforeMan) {
		t.Fatal("cluster.manifest mutated by restore")
	}
	if string(readObject(t, objStore, nodeManKey)) != string(beforeNode) {
		t.Fatal("node sub-manifest mutated by restore")
	}
}
