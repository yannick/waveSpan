package vector

import "testing"

func TestCollectionOfKey(t *testing.T) {
	cases := map[string][]byte{
		"raw":    rawKey("coll", "v1"),
		"meta":   metaKey("coll", "v1"),
		"attach": attachKey("node1", "coll", "v1"),
	}
	for name, k := range cases {
		c, ok := CollectionOfKey(k)
		if !ok || c != "coll" {
			t.Fatalf("%s: coll=%q ok=%v, want coll/true", name, c, ok)
		}
	}
	if _, ok := CollectionOfKey([]byte{'z', 'z', 0x01, 'c'}); ok {
		t.Fatal("unknown prefix must decode ok=false")
	}
	if _, ok := CollectionOfKey([]byte{'v'}); ok {
		t.Fatal("short key must decode ok=false")
	}
}

func TestCollectionOfKeyNeverPanicsOnOverflow(t *testing.T) {
	// uvarint(2^63) — a length prefix that overflows a lossy int() cast. For "va"
	// the overflow is in the first (nodeID) chunk, for "vr"/"vm" in the collection.
	overflow := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	for _, pfx := range []string{pfxRaw, pfxMeta, pfxAttach} {
		k := append([]byte(pfx), overflow...)
		if _, ok := CollectionOfKey(k); ok {
			t.Fatalf("overflow length after %s must decode ok=false (and not panic)", pfx)
		}
	}
}
