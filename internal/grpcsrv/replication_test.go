package grpcsrv

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Replication must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.ReplicationServiceServer = (*Replication)(nil)

func TestNewReplicationNonNil(t *testing.T) {
	a := NewReplication(nil, nil, "self", "127.0.0.1:9000", nil)
	if a == nil {
		t.Fatal("NewReplication returned nil")
	}
}
