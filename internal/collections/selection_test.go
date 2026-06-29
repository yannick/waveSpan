package collections

import (
	"encoding/binary"
	"testing"
)

func TestNamespaceCollectionOfKey(t *testing.T) {
	be8 := func(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

	// A subData row under a real data shard decodes (ns,coll).
	key := append(be8(ShardForKey([]byte("ns"), []byte("c"), 4)), subData)
	key = appendChunk(key, []byte("ns"))
	key = appendChunk(key, []byte("c"))
	key = append(key, []byte("rest")...)
	if ns, coll, ok := NamespaceCollectionOfKey(key); !ok || ns != "ns" || coll != "c" {
		t.Fatalf("subData: ns=%q coll=%q ok=%v want ns/c/true", ns, coll, ok)
	}

	// subBudExp index row (body=suffix[9:]) decodes the same (ns,coll).
	bud := append(be8(ShardForKey([]byte("ns"), []byte("c"), 4)), subBudExp)
	bud = append(bud, be8(123)...)
	bud = appendChunk(bud, []byte("ns"))
	bud = appendChunk(bud, []byte("c"))
	bud = append(bud, []byte("lease")...)
	if ns, coll, ok := NamespaceCollectionOfKey(bud); !ok || ns != "ns" || coll != "c" {
		t.Fatalf("subBudExp: ns=%q coll=%q ok=%v want ns/c/true", ns, coll, ok)
	}

	// Meta shard (shardID < FirstDataShard) → ok=false.
	if _, _, ok := NamespaceCollectionOfKey(append(be8(MetaShardID), subData)); ok {
		t.Fatal("meta-shard key must decode ok=false")
	}
	// Shard-local bookkeeping (subMeta) on a data shard → ok=false.
	meta := append(be8(ShardForKey([]byte("ns"), []byte("c"), 4)), subMeta)
	meta = append(meta, []byte("applied")...)
	if _, _, ok := NamespaceCollectionOfKey(meta); ok {
		t.Fatal("subMeta bookkeeping must decode ok=false")
	}
	// Too-short key → ok=false.
	if _, _, ok := NamespaceCollectionOfKey([]byte{0x00, 0x01}); ok {
		t.Fatal("short key must decode ok=false")
	}
}
