// Package graph implements WaveSpan's property-graph storage over wavesdb (design/07): node/edge
// records in CFGraphData, derived label/property/adjacency entries in CFGraphIndex, and an atomic
// single-Txn mutation batch on the coordinator pod.
package graph

import (
	"encoding/binary"
	"math"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Key prefixes within a column family. CFGraphData holds nodes ("n") and edges-by-id ("e");
// CFGraphIndex holds labels ("l"), property index ("p"), and adjacency ("ao"/"ai").
const (
	pfxNode   = "n"
	pfxEdge   = "e"
	pfxLabel  = "l"
	pfxProp   = "p"
	pfxOutAdj = "ao"
	pfxInAdj  = "ai"
)

func lp(dst []byte, s string) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(s)))
	dst = append(dst, tmp[:n]...)
	return append(dst, s...)
}

// prefixEnd returns the smallest key strictly greater than all keys with the given prefix.
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func decodeLP(b []byte) (string, []byte) {
	n, sz := binary.Uvarint(b)
	// Unsigned compare (no lossy int() cast): a length in (2^63, 2^64) would go
	// negative as an int and slip past a signed guard, panicking the slice below.
	if sz <= 0 || n > uint64(len(b)-sz) {
		return "", nil
	}
	return string(b[sz : sz+int(n)]), b[sz+int(n):]
}

// NodePrefix is the scan prefix for all node records of a graph (CFGraphData).
func NodePrefix(graph string) []byte { return lp([]byte(pfxNode), graph) }

// EdgePrefix is the scan prefix for all edge records of a graph (CFGraphData).
func EdgePrefix(graph string) []byte { return lp([]byte(pfxEdge), graph) }

// NodeKey is the CFGraphData key for a node record.
func NodeKey(graph, nodeID string) []byte {
	out := lp([]byte(pfxNode), graph)
	return lp(out, nodeID)
}

// EdgeKey is the CFGraphData key for an edge record (by-id lookup).
func EdgeKey(graph, edgeID string) []byte {
	out := lp([]byte(pfxEdge), graph)
	return lp(out, edgeID)
}

// LabelKey is the CFGraphIndex key for a (label -> node) index entry.
func LabelKey(graph, label, nodeID string) []byte {
	out := lp([]byte(pfxLabel), graph)
	out = lp(out, label)
	return lp(out, nodeID)
}

// LabelPrefix is the scan prefix for all nodes carrying a label.
func LabelPrefix(graph, label string) []byte {
	out := lp([]byte(pfxLabel), graph)
	return lp(out, label)
}

// PropKey is the CFGraphIndex key for a (label, prop, value -> node) index entry. The value is
// order-preserving so range seeks work; nodeID is the raw suffix.
func PropKey(graph, label, prop string, val *wavespanv1.Value, nodeID string) []byte {
	out := propValuePrefix(graph, label, prop, val)
	return append(out, nodeID...)
}

// propValuePrefix is PropKey without the node id (an equality-seek prefix).
func propValuePrefix(graph, label, prop string, val *wavespanv1.Value) []byte {
	out := lp([]byte(pfxProp), graph)
	out = lp(out, label)
	out = lp(out, prop)
	return append(out, EncodeOrderedValue(val)...)
}

// PropTypePrefix is the scan prefix for a (label, prop) of a given value type, for range seeks.
func PropTypePrefix(graph, label, prop string, typeTag byte) []byte {
	out := lp([]byte(pfxProp), graph)
	out = lp(out, label)
	out = lp(out, prop)
	return append(out, typeTag)
}

// OutAdjKey is the CFGraphIndex key for an outgoing-adjacency entry; sorts grouped by (src,type,dst).
func OutAdjKey(graph, src, edgeType, dst, edgeID string) []byte {
	out := lp([]byte(pfxOutAdj), graph)
	out = lp(out, src)
	out = lp(out, edgeType)
	out = lp(out, dst)
	return lp(out, edgeID)
}

// OutAdjPrefix is the scan prefix for all outgoing edges of a node.
func OutAdjPrefix(graph, src string) []byte {
	out := lp([]byte(pfxOutAdj), graph)
	return lp(out, src)
}

// InAdjKey is the CFGraphIndex key for an incoming-adjacency entry; sorts grouped by (dst,type,src).
func InAdjKey(graph, dst, edgeType, src, edgeID string) []byte {
	out := lp([]byte(pfxInAdj), graph)
	out = lp(out, dst)
	out = lp(out, edgeType)
	out = lp(out, src)
	return lp(out, edgeID)
}

// InAdjPrefix is the scan prefix for all incoming edges of a node.
func InAdjPrefix(graph, dst string) []byte {
	out := lp([]byte(pfxInAdj), graph)
	return lp(out, dst)
}

// Ordered-value type tags (sort values into per-type groups).
const (
	tagNull   byte = 0
	tagBool   byte = 1
	tagInt    byte = 2
	tagDouble byte = 3
	tagString byte = 4
	tagBytes  byte = 5
)

// EncodeOrderedValue encodes a property Value into order-preserving bytes for the property index:
// a type tag, then a byte-comparable payload (sign-flipped big-endian numbers; raw strings/bytes).
func EncodeOrderedValue(v *wavespanv1.Value) []byte {
	switch b := v.GetValue().(type) {
	case *wavespanv1.Value_BoolValue:
		if b.BoolValue {
			return []byte{tagBool, 1}
		}
		return []byte{tagBool, 0}
	case *wavespanv1.Value_IntValue:
		out := []byte{tagInt}
		var u [8]byte
		binary.BigEndian.PutUint64(u[:], uint64(b.IntValue)^0x8000000000000000) // flip sign for order
		return append(out, u[:]...)
	case *wavespanv1.Value_DoubleValue:
		out := []byte{tagDouble}
		bits := math.Float64bits(b.DoubleValue)
		if bits&0x8000000000000000 != 0 {
			bits = ^bits // negative: flip all bits
		} else {
			bits |= 0x8000000000000000 // positive: flip sign bit
		}
		var u [8]byte
		binary.BigEndian.PutUint64(u[:], bits)
		return append(out, u[:]...)
	case *wavespanv1.Value_StringValue:
		return append([]byte{tagString}, b.StringValue...)
	case *wavespanv1.Value_BytesValue:
		return append([]byte{tagBytes}, b.BytesValue...)
	default:
		return []byte{tagNull}
	}
}

// TypeTag returns the ordered-value type tag for a value (for range-seek prefixes).
func TypeTag(v *wavespanv1.Value) byte { return EncodeOrderedValue(v)[0] }
