package grpcsrv

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// GlobalReplication must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.GlobalReplicationServer = (*GlobalReplication)(nil)

func TestNewGlobalReplicationNonNil(t *testing.T) {
	a := NewGlobalReplication(nil, nil, nil)
	if a == nil {
		t.Fatal("NewGlobalReplication returned nil")
	}
}
