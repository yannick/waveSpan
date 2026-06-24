package grpcsrv

import (
	"testing"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Vector must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.VectorServiceServer = (*Vector)(nil)

func TestNewVectorNonNil(t *testing.T) {
	a := NewVector(nil, nil)
	if a == nil {
		t.Fatal("NewVector returned nil")
	}
}
