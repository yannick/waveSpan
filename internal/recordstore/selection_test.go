package recordstore

import (
	"testing"

	"github.com/yannick/wavespan/internal/version"
)

func TestNamespaceOfKey(t *testing.T) {
	cases := map[string][]byte{
		"dataKey":   dataKey("ns", []byte("k"), version.Version{}),
		"latestKey": latestKey("ns", []byte("k")),
		"ttlKey":    ttlKey(1719000000000, "ns", []byte("k")),
	}
	for name, k := range cases {
		ns, ok := NamespaceOfKey(k)
		if !ok || ns != "ns" {
			t.Fatalf("%s: ns=%q ok=%v, want ns/true", name, ns, ok)
		}
	}
	if _, ok := NamespaceOfKey(nil); ok {
		t.Fatal("empty key must decode ok=false")
	}
	if _, ok := NamespaceOfKey([]byte{0xff, 0x01}); ok {
		t.Fatal("short ttl-sentinel key must decode ok=false")
	}
}

// overflowUvarint is uvarint(2^63): a length prefix big enough that a lossy
// int() cast goes negative and would slice out of range (the never-panic bug).
var overflowUvarint = []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}

func TestNamespaceOfKeyNeverPanicsOnOverflow(t *testing.T) {
	// Non-sentinel path: leading length prefix overflows.
	if _, ok := NamespaceOfKey(overflowUvarint); ok {
		t.Fatal("overflow ns length must decode ok=false (and not panic)")
	}
	// TTL-sentinel path: ns length after 0xff + 8-byte bucket overflows.
	k := append([]byte{0xff}, make([]byte, 8)...)
	k = append(k, overflowUvarint...)
	if _, ok := NamespaceOfKey(k); ok {
		t.Fatal("overflow ttl ns length must decode ok=false (and not panic)")
	}
}
