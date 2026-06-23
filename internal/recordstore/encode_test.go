package recordstore

import (
	"bytes"
	"sort"
	"testing"

	"github.com/yannick/wavespan/internal/version"
)

func TestLatestKeyPreservesUserKeyOrder(t *testing.T) {
	// within one namespace, latest keys must sort in raw user-key order (M6 scans depend on it)
	users := [][]byte{[]byte("a"), []byte("ab"), []byte("b"), []byte("ba"), []byte("c")}
	keys := make([][]byte, len(users))
	for i, u := range users {
		keys[i] = latestKey("ns", u)
	}
	sorted := append([][]byte(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })
	for i := range keys {
		if !bytes.Equal(keys[i], sorted[i]) {
			t.Fatalf("latest keys not in user-key order at %d: %q", i, users[i])
		}
	}
}

func TestNamespaceIsolationInLatestKeys(t *testing.T) {
	a := latestKey("ns1", []byte("x"))
	b := latestKey("ns2", []byte("x"))
	if bytes.Equal(a, b) {
		t.Fatal("same key in different namespaces must encode differently")
	}
	// namespacePrefix must prefix every latest key in that namespace
	p := namespacePrefix("ns1")
	if !bytes.HasPrefix(a, p) {
		t.Fatal("namespacePrefix should prefix the namespace's latest keys")
	}
	if bytes.HasPrefix(b, p) {
		t.Fatal("namespacePrefix should not match another namespace (length-prefixed ns)")
	}
}

func TestDataKeyGroupsVersionsAndIsUnique(t *testing.T) {
	v1 := version.Version{HLCPhysicalMs: 100, WriterClusterID: "c", WriterMemberID: "m", WriterSequence: 1}
	v2 := version.Version{HLCPhysicalMs: 200, WriterClusterID: "c", WriterMemberID: "m", WriterSequence: 2}
	k1 := dataKey("ns", []byte("key"), v1)
	k2 := dataKey("ns", []byte("key"), v2)
	if bytes.Equal(k1, k2) {
		t.Fatal("different versions must produce different data keys")
	}
	prefix := dataKeyPrefix("ns", []byte("key"))
	if !bytes.HasPrefix(k1, prefix) || !bytes.HasPrefix(k2, prefix) {
		t.Fatal("all versions of a key must share the data-key prefix")
	}
	// a different user key must not share the prefix (length-prefixed user key)
	other := dataKey("ns", []byte("ke"), v1)
	if bytes.HasPrefix(other, prefix) {
		t.Fatal("different user key must not match the data-key prefix")
	}
}
