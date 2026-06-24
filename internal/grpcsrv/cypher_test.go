package grpcsrv

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Cypher must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.CypherServer = (*Cypher)(nil)

func TestNewCypherNonNil(t *testing.T) {
	a := NewCypher(nil, "c1", "m1", nil)
	if a == nil {
		t.Fatal("NewCypher returned nil")
	}
}
