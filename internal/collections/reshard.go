package collections

import (
	"encoding/binary"
	"fmt"
)

// RerouteSuffix decides the target shard for a CFReplData key suffix (the bytes after the
// 8-byte shard prefix) under a new shard count newN. Collection rows that embed (ns,coll) are
// re-routed; shard-local bookkeeping (subMeta applied-index, subDedup window, subDedupRing) is
// dropped (keep=false, nil err); an unrecognized sub-prefix returns an error so re-shard fails
// loudly rather than dropping or misplacing data.
func RerouteSuffix(suffix []byte, newN uint64) (shardID uint64, keep bool, err error) {
	if len(suffix) == 0 {
		return 0, false, fmt.Errorf("collections: empty CFReplData suffix")
	}
	switch suffix[0] {
	case subMeta, subDedup, subDedupRing:
		// Shard-local bookkeeping with no (ns,coll): the applied raft index and the
		// idempotency dedup window/ring. Not re-routable; the target shards rebuild them
		// fresh (mirrors how the dropped applied index is re-established).
		return 0, false, nil
	case subData, subTTL, subBudExp, subBudTombGC:
		// Routable: handled below.
	default:
		return 0, false, fmt.Errorf("collections: cannot re-route unknown CFReplData sub-prefix %#x", suffix[0])
	}
	ns, coll, ok := nsCollOfSuffix(suffix)
	if !ok {
		return 0, false, fmt.Errorf("collections: cannot decode (ns,coll) from %#x CFReplData suffix", suffix[0])
	}
	return ShardForKey(ns, coll, newN), true, nil
}

// nsCollOfSuffix extracts (ns,coll) from a routable CFReplData key suffix. The body
// holding chunk(ns)||chunk(coll) follows the sub-prefix byte for subData (suffix[1:])
// and the sub-prefix byte plus an 8-byte timestamp for the shard-level due indexes
// subTTL/subBudExp/subBudTombGC (suffix[9:]). It returns ok=false for bookkeeping or
// unknown sub-prefixes, short suffixes, or undecodable chunks — never panics.
func nsCollOfSuffix(suffix []byte) (ns, coll []byte, ok bool) {
	if len(suffix) == 0 {
		return nil, nil, false
	}
	var body []byte
	switch suffix[0] {
	case subData:
		body = suffix[1:]
	case subTTL, subBudExp, subBudTombGC:
		if len(suffix) < 9 {
			return nil, nil, false
		}
		body = suffix[9:] // skip sub-prefix byte + 8-byte timestamp
	default:
		return nil, nil, false
	}
	ns, rest, err := takeChunk(body)
	if err != nil {
		return nil, nil, false
	}
	coll, _, err = takeChunk(rest)
	if err != nil {
		return nil, nil, false
	}
	return ns, coll, true
}

// NamespaceCollectionOfKey decodes the (ns,coll) a full CFReplData key belongs to,
// for partial-backup selection. It returns ok=false for keys with no collection
// identity: keys shorter than the 8-byte shard prefix, meta/system-shard keys
// (shardID < FirstDataShard), and shard-local bookkeeping or unknown sub-prefixes.
func NamespaceCollectionOfKey(key []byte) (ns, coll string, ok bool) {
	if len(key) < 8 {
		return "", "", false
	}
	if binary.BigEndian.Uint64(key[:8]) < FirstDataShard {
		return "", "", false
	}
	nsb, collb, ok := nsCollOfSuffix(key[8:])
	if !ok {
		return "", "", false
	}
	return string(nsb), string(collb), true
}
