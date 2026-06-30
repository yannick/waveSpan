package backup

import (
	"reflect"
	"testing"

	"wavesdb/objstore"
)

// TestResolveChain builds a base→incremental→incremental chain of cluster manifests and asserts the
// chain resolves base-first; a broken parent link and a missing root both surface as loud errors.
func TestResolveChain(t *testing.T) {
	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write := func(id, parent string) {
		cm := &ClusterManifest{FormatVersion: clusterManifestFormatVersion, BackupID: id, Parent: parent, Planes: []string{"physical"}, Status: "COMPLETE"}
		if err := WriteClusterManifest(store, cm); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
	}
	write("B0", "")
	write("B1", "B0")
	write("B2", "B1")

	chain, err := ResolveChain(store, "B2")
	if err != nil {
		t.Fatalf("ResolveChain(B2): %v", err)
	}
	if !reflect.DeepEqual(chain, []string{"B0", "B1", "B2"}) {
		t.Fatalf("chain = %v, want [B0 B1 B2]", chain)
	}

	// A single full backup resolves to just itself.
	if chain, err := ResolveChain(store, "B0"); err != nil || !reflect.DeepEqual(chain, []string{"B0"}) {
		t.Fatalf("ResolveChain(B0) = %v err %v, want [B0]", chain, err)
	}

	// A broken parent link is a loud error, naming the missing link.
	write("Bx", "ghost-parent")
	if _, err := ResolveChain(store, "Bx"); err == nil {
		t.Fatalf("ResolveChain over broken parent = nil err, want error")
	}

	// A missing root is a loud error.
	if _, err := ResolveChain(store, "does-not-exist"); err == nil {
		t.Fatalf("ResolveChain(missing) = nil err, want error")
	}
}

// TestResolveChainCycle guards the cycle branch: a parent pointer that loops back must surface a loud
// error rather than walking forever.
func TestResolveChainCycle(t *testing.T) {
	store, err := objstore.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	write := func(id, parent string) {
		cm := &ClusterManifest{FormatVersion: clusterManifestFormatVersion, BackupID: id, Parent: parent, Planes: []string{"physical"}, Status: "COMPLETE"}
		if err := WriteClusterManifest(store, cm); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
	}
	// C1 -> C2 -> C1 (a cycle).
	write("C1", "C2")
	write("C2", "C1")
	if _, err := ResolveChain(store, "C1"); err == nil {
		t.Fatalf("ResolveChain over a cycle = nil err, want loud error")
	}
}
