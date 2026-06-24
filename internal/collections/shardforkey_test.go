package collections

import (
	"fmt"
	"testing"
)

// TestShardForKeyMatchesHashDirectory asserts the exported pure routing function ShardForKey agrees
// with HashDirectory.ShardFor for every (ns,coll) over a range of data-shard counts. This is the
// contract that lets a shard-aware client route identically to the server: if the two ever diverge,
// writes land on the wrong shard's leader.
func TestShardForKeyMatchesHashDirectory(t *testing.T) {
	for _, n := range []uint64{1, 2, 3, 4, 8, 16} {
		dir := NewHashDirectory(n)
		for i := 0; i < 500; i++ {
			ns := []byte(fmt.Sprintf("ns/%d", i%7))
			coll := []byte(fmt.Sprintf("col/%d", i))
			got := ShardForKey(ns, coll, n)
			want := dir.ShardFor(ns, coll)
			if got != want {
				t.Fatalf("n=%d ns=%q coll=%q: ShardForKey=%d ShardFor=%d", n, ns, coll, got, want)
			}
			if got < FirstDataShard || got >= FirstDataShard+n {
				t.Fatalf("n=%d: shard %d out of range [%d,%d)", n, got, FirstDataShard, FirstDataShard+n)
			}
		}
	}
}

// TestShardForKeyClampsDataShards confirms dataShards < 1 is clamped to a single shard (FirstDataShard).
func TestShardForKeyClampsDataShards(t *testing.T) {
	if got := ShardForKey([]byte("a"), []byte("b"), 0); got != FirstDataShard {
		t.Fatalf("dataShards=0: got shard %d want %d", got, FirstDataShard)
	}
}
