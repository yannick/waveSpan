package collections

import "fmt"

// RerouteSuffix decides the target shard for a CFReplData key suffix (the bytes after the
// 8-byte shard prefix) under a new shard count newN. Collection rows that embed (ns,coll) are
// re-routed; shard-local bookkeeping (subMeta applied-index, subDedup window, subDedupRing) is
// dropped (keep=false, nil err); an unrecognized sub-prefix returns an error so re-shard fails
// loudly rather than dropping or misplacing data.
func RerouteSuffix(suffix []byte, newN uint64) (shardID uint64, keep bool, err error) {
	if len(suffix) == 0 {
		return 0, false, fmt.Errorf("collections: empty CFReplData suffix")
	}
	var body []byte
	switch suffix[0] {
	case subMeta, subDedup, subDedupRing:
		// Shard-local bookkeeping with no (ns,coll): the applied raft index and the
		// idempotency dedup window/ring. Not re-routable; the target shards rebuild them
		// fresh (mirrors how the dropped applied index is re-established).
		return 0, false, nil
	case subData:
		body = suffix[1:]
	case subTTL, subBudExp, subBudTombGC:
		if len(suffix) < 9 {
			return 0, false, fmt.Errorf("collections: short %#x suffix", suffix[0])
		}
		body = suffix[9:] // skip sub-prefix byte + 8-byte timestamp
	default:
		return 0, false, fmt.Errorf("collections: cannot re-route unknown CFReplData sub-prefix %#x", suffix[0])
	}
	ns, rest, err := takeChunk(body)
	if err != nil {
		return 0, false, fmt.Errorf("collections: reroute decode ns: %w", err)
	}
	coll, _, err := takeChunk(rest)
	if err != nil {
		return 0, false, fmt.Errorf("collections: reroute decode coll: %w", err)
	}
	return ShardForKey(ns, coll, newN), true, nil
}
