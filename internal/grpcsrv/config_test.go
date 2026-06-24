package grpcsrv

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Config must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.ConfigServiceServer = (*Config)(nil)

func TestNewConfigNonNil(t *testing.T) {
	a := NewConfig(nil, nil, "cluster", "member")
	if a == nil {
		t.Fatal("NewConfig returned nil")
	}
}
