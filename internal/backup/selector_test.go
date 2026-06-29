package backup

import (
	"encoding/binary"
	"testing"

	"github.com/yannick/wavespan/internal/graph"
	"github.com/yannick/wavespan/internal/storage"
)

// lpUV appends uvarint(len(s))||s — the recordstore/graph/vector chunk encoding
// (collections uses uint32be via chunkBE instead).
func lpUV(dst []byte, s string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(s)))
	dst = append(dst, tmp[:n]...)
	return append(dst, s...)
}

// kvKey builds a CFKVMeta latest-pointer key (lenPrefix(ns)||userKey).
func kvKey(ns, k string) []byte { return append(lpUV(nil, ns), []byte(k)...) }

// vrKey builds a CFVectorRaw key ("vr"||lp(coll)||lp(vec)).
func vrKey(coll, vec string) []byte { return lpUV(lpUV([]byte("vr"), coll), vec) }

func TestSelectorMatchers(t *testing.T) {
	if !(Selector{}).IsEmpty() {
		t.Fatal("zero Selector must be empty")
	}
	sel := Selector{
		Namespaces:        Set("ns1"),
		Graphs:            Set("g1"),
		VectorCollections: Set("c1"),
	}
	if sel.IsEmpty() {
		t.Fatal("populated Selector must not be empty")
	}

	reg := DefaultRegistry()
	owner := map[storage.ColumnFamily]Contributor{}
	for _, c := range reg.Contributors() {
		for _, s := range c.CFs() {
			owner[s.CF] = c
		}
	}
	sel1 := func(cf storage.ColumnFamily, key []byte) bool { return owner[cf].Selects(cf, key, sel) }

	// KV: ns1 included, ns2 excluded.
	if !sel1(storage.CFKVData, kvKey("ns1", "k")) {
		t.Fatal("kv ns1 should be selected")
	}
	if sel1(storage.CFKVData, kvKey("ns2", "k")) {
		t.Fatal("kv ns2 should be excluded")
	}
	// Collections: ns1 included, ns2 excluded.
	if !sel1(storage.CFReplData, replDataKey("ns1", "cX", "row", 4)) {
		t.Fatal("collections ns1 should be selected")
	}
	if sel1(storage.CFReplData, replDataKey("ns2", "cX", "row", 4)) {
		t.Fatal("collections ns2 should be excluded")
	}
	// Graph: g1 included, g2 excluded.
	if !sel1(storage.CFGraphData, graph.NodeKey("g1", "n")) {
		t.Fatal("graph g1 should be selected")
	}
	if sel1(storage.CFGraphData, graph.NodeKey("g2", "n")) {
		t.Fatal("graph g2 should be excluded")
	}
	// Vector: c1 included, c2 excluded.
	if !sel1(storage.CFVectorRaw, vrKey("c1", "v")) {
		t.Fatal("vector c1 should be selected")
	}
	if sel1(storage.CFVectorRaw, vrKey("c2", "v")) {
		t.Fatal("vector c2 should be excluded")
	}
	// CFSys is always included regardless of selector.
	if !sel1(storage.CFSys, []byte("/sys/anything")) {
		t.Fatal("CFSys must always be selected")
	}
}
