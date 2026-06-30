package collections

import (
	"encoding/binary"
	"testing"
)

func be8(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

func dataSuffix(ns, coll string) []byte {
	// subData || chunk(ns) || chunk(coll) || <rest>
	s := []byte{subData}
	s = appendChunk(s, []byte(ns))
	s = appendChunk(s, []byte(coll))
	return append(s, []byte("rest")...)
}

func TestRerouteSuffix(t *testing.T) {
	const newN = 8
	want := ShardForKey([]byte("ns1"), []byte("c1"), newN)

	// subData row re-routes by (ns,coll).
	id, keep, err := RerouteSuffix(dataSuffix("ns1", "c1"), newN)
	if err != nil || !keep || id != want {
		t.Fatalf("subData reroute: id=%d keep=%v err=%v want id=%d", id, keep, err, want)
	}

	// subBudExp index row (subBudExp || be8(reclaim) || chunk(ns) || chunk(coll) || leaseID) re-routes the same.
	exp := append([]byte{subBudExp}, be8(123)...)
	exp = appendChunk(exp, []byte("ns1"))
	exp = appendChunk(exp, []byte("c1"))
	exp = append(exp, []byte("lease")...)
	id2, keep2, err2 := RerouteSuffix(exp, newN)
	if err2 != nil || !keep2 || id2 != want {
		t.Fatalf("subBudExp reroute: id=%d keep=%v err=%v want id=%d", id2, keep2, err2, want)
	}

	// Shard-local bookkeeping is dropped: applied index + dedup window/ring (no (ns,coll)).
	for _, sp := range []byte{subMeta, subDedup, subDedupRing} {
		if _, keep, err := RerouteSuffix([]byte{sp, 'a'}, newN); err != nil || keep {
			t.Fatalf("sub-prefix %#x should drop: keep=%v err=%v", sp, keep, err)
		}
	}

	// A genuinely unused sub-prefix is a loud error (never silently dropped or misplaced).
	if _, _, err4 := RerouteSuffix([]byte{0x7f, 'x'}, newN); err4 == nil {
		t.Fatal("unknown sub-prefix must return an error")
	}
}
