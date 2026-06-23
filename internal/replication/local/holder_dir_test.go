package local

import (
	"testing"

	"github.com/yannick/wavespan/internal/version"
)

func TestHolderDirectoryResolvesWithoutBroadcast(t *testing.T) {
	d := NewHolderDirectory("node1")
	v := version.Version{HLCPhysicalMs: 1}
	d.RecordHolder("default", []byte("k"), "node1", v)
	d.RecordHolder("default", []byte("k"), "node2", v)

	// a get miss on node3 can resolve holders from the directory — no broadcast needed
	holders := d.Holders("default", []byte("k"))
	if len(holders) != 2 || holders[0] != "node1" || holders[1] != "node2" {
		t.Fatalf("directory should list both holders sorted: %v", holders)
	}
	if d.Count("default", []byte("k")) != 2 {
		t.Fatalf("count mismatch")
	}
	// a different key resolves to nothing (no false positives)
	if len(d.Holders("default", []byte("other"))) != 0 {
		t.Fatal("unknown key should resolve to no holders")
	}
}

func TestHolderDirectoryRemoveAndKeysHeldBy(t *testing.T) {
	d := NewHolderDirectory("node1")
	v := version.Version{HLCPhysicalMs: 1}
	d.RecordHolder("ns", []byte("a"), "node2", v)
	d.RecordHolder("ns", []byte("b"), "node2", v)
	d.RecordHolder("ns", []byte("a"), "node3", v)

	held := d.keysHeldBy("node2")
	if len(held) != 2 {
		t.Fatalf("node2 should hold 2 keys, got %d", len(held))
	}
	d.RemoveHolder("ns", []byte("a"), "node2")
	if d.Count("ns", []byte("a")) != 1 {
		t.Fatalf("after removing node2, key a should have 1 holder")
	}
	for _, h := range d.Holders("ns", []byte("a")) {
		if h == "node2" {
			t.Fatal("node2 should be gone from key a")
		}
	}
}
