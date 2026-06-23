package graph

import (
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// EncodeNode marshals a node record for storage in CFGraphData.
func EncodeNode(n *wavespanv1.NodeRecord) ([]byte, error) { return proto.Marshal(n) }

// DecodeNode unmarshals a node record.
func DecodeNode(b []byte) (*wavespanv1.NodeRecord, error) {
	n := &wavespanv1.NodeRecord{}
	if err := proto.Unmarshal(b, n); err != nil {
		return nil, err
	}
	return n, nil
}

// EncodeEdge marshals an edge record for storage in CFGraphData.
func EncodeEdge(e *wavespanv1.EdgeRecord) ([]byte, error) { return proto.Marshal(e) }

// DecodeEdge unmarshals an edge record.
func DecodeEdge(b []byte) (*wavespanv1.EdgeRecord, error) {
	e := &wavespanv1.EdgeRecord{}
	if err := proto.Unmarshal(b, e); err != nil {
		return nil, err
	}
	return e, nil
}
