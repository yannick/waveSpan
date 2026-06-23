package vector

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// Ring returns the top-n members for a bucket by rendezvous (highest-random-weight) hashing. HRW is
// deterministic from the member set — every node computes the same holders without coordination —
// and moves only ~1/N of buckets when a member joins or leaves (minimal rebalancing). It is the
// affinity placement + routing primitive for vector buckets (design/29 Phase 3): all vectors in a
// bucket land on the same small node-set, so a kNN query reaches just those nodes.
func Ring(bucketKey string, memberIDs []string, n int) []string {
	if n <= 0 || len(memberIDs) == 0 {
		return nil
	}
	type scored struct {
		id    string
		score uint64
	}
	ss := make([]scored, len(memberIDs))
	for i, id := range memberIDs {
		ss[i] = scored{id, hrw(bucketKey, id)}
	}
	sort.Slice(ss, func(a, b int) bool {
		if ss[a].score != ss[b].score {
			return ss[a].score > ss[b].score
		}
		return ss[a].id < ss[b].id // stable tie-break
	})
	if n > len(ss) {
		n = len(ss)
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = ss[i].id
	}
	return out
}

func hrw(bucketKey, member string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(bucketKey))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(member))
	return h.Sum64()
}

// BucketKey is the stable string identifying a (collection, quantizer-version, bucket) tuple for HRW.
func BucketKey(collection string, qver, bucket uint32) string {
	return collection + "/" + strconv.FormatUint(uint64(qver), 10) + "/" + strconv.FormatUint(uint64(bucket), 10)
}
