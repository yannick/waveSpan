package graph

import (
	"bytes"
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

func intVal(i int64) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_IntValue{IntValue: i}}
}
func strVal(s string) *wavespanv1.Value {
	return &wavespanv1.Value{Value: &wavespanv1.Value_StringValue{StringValue: s}}
}

func TestOutAdjKeyGroupingAndTypeSeek(t *testing.T) {
	k1 := OutAdjKey("g", "a", "FOLLOWS", "b", "e1")
	k2 := OutAdjKey("g", "a", "FOLLOWS", "c", "e2")
	k3 := OutAdjKey("g", "a", "LIKES", "b", "e3")
	// all outgoing edges of a node share the src prefix (enables ScanOutgoing)
	srcPfx := OutAdjPrefix("g", "a")
	for _, k := range [][]byte{k1, k2, k3} {
		if !bytes.HasPrefix(k, srcPfx) {
			t.Fatalf("key %x missing src prefix", k)
		}
	}
	// within a type, edges sort by destination (FOLLOWS->b before FOLLOWS->c)
	if bytes.Compare(k1, k2) >= 0 {
		t.Fatal("FOLLOWS->b should sort before FOLLOWS->c")
	}
	// a type-specific prefix isolates exactly that relationship type
	followsPfx := append(OutAdjPrefix("g", "a"), lp(nil, "FOLLOWS")...)
	if !bytes.HasPrefix(k1, followsPfx) {
		t.Fatal("FOLLOWS edge should carry the FOLLOWS type prefix")
	}
	if bytes.HasPrefix(k3, followsPfx) {
		t.Fatal("LIKES edge must not match the FOLLOWS type prefix")
	}
}

func TestOrderedIntValueIsByteComparable(t *testing.T) {
	// negative < zero < positive under the sign-flipped encoding
	neg := EncodeOrderedValue(intVal(-5))
	zero := EncodeOrderedValue(intVal(0))
	pos := EncodeOrderedValue(intVal(30))
	big := EncodeOrderedValue(intVal(100))
	if bytes.Compare(neg, zero) >= 0 || bytes.Compare(zero, pos) >= 0 || bytes.Compare(pos, big) >= 0 {
		t.Fatalf("int ordering broken: %x %x %x %x", neg, zero, pos, big)
	}
}

func TestPropKeyPrefixSeek(t *testing.T) {
	// equality prefix matches its own key, and different values differ
	k30 := PropKey("g", "User", "age", intVal(30), "n1")
	pfx30 := propValuePrefix("g", "User", "age", intVal(30))
	if !bytes.HasPrefix(k30, pfx30) {
		t.Fatal("prop key should carry its value prefix")
	}
	k40 := PropKey("g", "User", "age", intVal(40), "n1")
	if bytes.Compare(k30, k40) >= 0 {
		t.Fatal("age 30 should sort before age 40")
	}
}

func TestRecordEncodeDecode(t *testing.T) {
	n := &wavespanv1.NodeRecord{
		GraphId: "g", NodeId: "n1", Labels: []string{"User"},
		Properties: map[string]*wavespanv1.Value{"name": strVal("alice"), "age": intVal(30)},
		Version:    &wavespanv1.Version{HlcPhysicalMs: 100, WriterMemberId: "m1"},
	}
	b, err := EncodeNode(n)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeNode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(n, got) {
		t.Fatalf("node round-trip mismatch:\n%v\n%v", n, got)
	}

	e := &wavespanv1.EdgeRecord{GraphId: "g", EdgeId: "e1", StartNode: "n1", EndNode: "n2", Type: "FOLLOWS", Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	eb, _ := EncodeEdge(e)
	ge, err := DecodeEdge(eb)
	if err != nil || !proto.Equal(e, ge) {
		t.Fatalf("edge round-trip mismatch: %v %v", e, ge)
	}
}
