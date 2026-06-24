package grpcsrv

import (
	"testing"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Collections must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.CollectionServiceServer = (*Collections)(nil)

func TestNewCollectionsNonNil(t *testing.T) {
	a := NewCollections(collections.NewService(nil, membership.Member{}))
	if a == nil {
		t.Fatal("NewCollections returned nil")
	}
}
