package grpcsrv

import (
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// KV must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.KvServiceServer = (*KV)(nil)

func TestNewKVNonNil(t *testing.T) {
	a := NewKV(nil, nil, nil, membership.Member{})
	if a == nil {
		t.Fatal("NewKV returned nil")
	}
}
