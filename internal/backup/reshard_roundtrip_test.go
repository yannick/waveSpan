package backup

import (
	"testing"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/storage"
	"wavesdb/objstore"
)

// CFReplData sub-prefix bytes used only by this round-trip test (mirrored from
// package collections, like subDataByte/subMetaByte in logical_restore_test.go).
const (
	subDedupByte  byte = 0x03 // collections.subDedup
	subBudExpByte byte = 0x05 // collections.subBudExp
)

// budExpSuffix builds a budget shard-level auto-reclaim index suffix:
// subBudExp || be8(reclaimMs) || chunk(ns) || chunk(coll) || leaseID.
func budExpSuffix(ns, coll, lease string, reclaimMs uint64) []byte {
	s := append([]byte{subBudExpByte}, be8(reclaimMs)...)
	s = append(s, chunkBE([]byte(ns))...)
	s = append(s, chunkBE([]byte(coll))...)
	return append(s, []byte(lease)...)
}

func budExpKey(ns, coll, lease string, reclaimMs, n uint64) []byte {
	return append(be8(collections.ShardForKey([]byte(ns), []byte(coll), n)), budExpSuffix(ns, coll, lease, reclaimMs)...)
}

// TestReshardRoundTripN4toN8 proves a realistic re-shard: several collections,
// each with a data row and a budget secondary-index row, plus shard-local
// bookkeeping (subMeta, subDedup), restored from a source N=4 layout into a target
// N=8 cluster. Every collection row lands under its N=8 shard with value+suffix
// intact, at least one collection actually moves shard, and the shard-local rows
// are dropped.
func TestReshardRoundTripN4toN8(t *testing.T) {
	const srcN, dstN = 4, 8

	src, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = src.Close() })

	pairs := []struct{ ns, coll string }{
		{"tenantA", "orders"},
		{"tenantA", "users"},
		{"tenantB", "events"},
		{"tenantB", "sessions"},
		{"tenantC", "metrics"},
		{"tenantC", "logs"},
	}

	moved := 0
	for _, p := range pairs {
		// Data row.
		mustPut(t, src, storage.CFReplData, replDataKey(p.ns, p.coll, "doc", srcN), []byte("data:"+p.ns+"/"+p.coll))
		// Budget auto-reclaim index row.
		mustPut(t, src, storage.CFReplData, budExpKey(p.ns, p.coll, "lease9", 555, srcN), []byte("budidx:"+p.ns+"/"+p.coll))
		if collections.ShardForKey([]byte(p.ns), []byte(p.coll), srcN) != collections.ShardForKey([]byte(p.ns), []byte(p.coll), dstN) {
			moved++
		}
	}

	// Shard-local bookkeeping a real cluster would hold: applied index + a dedup row.
	metaKey := append(be8(5), subMetaByte)
	metaKey = append(metaKey, []byte("applied")...)
	mustPut(t, src, storage.CFReplData, metaKey, []byte("123"))
	dedupKey := append(be8(5), subDedupByte)
	dedupKey = append(dedupKey, []byte("idem-token-xyz")...)
	mustPut(t, src, storage.CFReplData, dedupKey, []byte("seen"))

	store, _ := objstore.NewFS(t.TempDir())
	if _, err := ExportLogical(src, store, "bk", DefaultRegistry(), 1719000000000); err != nil {
		t.Fatal(err)
	}

	dst, _ := storage.OpenWavesdb(t.TempDir())
	t.Cleanup(func() { _ = dst.Close() })
	if err := RestoreLogical(dst, store, "bk", DefaultRegistry(), RestoreInfo{CollectionsDataShards: dstN}); err != nil {
		t.Fatal(err)
	}

	if moved == 0 {
		t.Fatal("test is a no-op: no (ns,coll) changed shard between N=4 and N=8")
	}

	for _, p := range pairs {
		// (a) data + budget index rows present under the new N=8 shard, verbatim.
		if v, ok, _ := dst.Get(storage.CFReplData, replDataKey(p.ns, p.coll, "doc", dstN)); !ok || string(v) != "data:"+p.ns+"/"+p.coll {
			t.Fatalf("%s/%s data row missing at N=8: ok=%v v=%q", p.ns, p.coll, ok, v)
		}
		if v, ok, _ := dst.Get(storage.CFReplData, budExpKey(p.ns, p.coll, "lease9", 555, dstN)); !ok || string(v) != "budidx:"+p.ns+"/"+p.coll {
			t.Fatalf("%s/%s budget index row missing at N=8: ok=%v v=%q", p.ns, p.coll, ok, v)
		}
		// (a cont.) absent under the old N=4 shard when the shard actually changed.
		if collections.ShardForKey([]byte(p.ns), []byte(p.coll), srcN) != collections.ShardForKey([]byte(p.ns), []byte(p.coll), dstN) {
			if _, ok, _ := dst.Get(storage.CFReplData, replDataKey(p.ns, p.coll, "doc", srcN)); ok {
				t.Fatalf("%s/%s data row should not remain under old N=4 shard", p.ns, p.coll)
			}
			if _, ok, _ := dst.Get(storage.CFReplData, budExpKey(p.ns, p.coll, "lease9", 555, srcN)); ok {
				t.Fatalf("%s/%s budget index row should not remain under old N=4 shard", p.ns, p.coll)
			}
		}
	}

	// (c) shard-local bookkeeping dropped.
	if _, ok, _ := dst.Get(storage.CFReplData, metaKey); ok {
		t.Fatal("subMeta applied-index row must be dropped on re-shard")
	}
	if _, ok, _ := dst.Get(storage.CFReplData, dedupKey); ok {
		t.Fatal("subDedup row must be dropped on re-shard")
	}
}
