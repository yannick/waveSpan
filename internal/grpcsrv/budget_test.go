package grpcsrv

import (
	"testing"

	"github.com/yannick/wavespan/internal/collections"
	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Budget must satisfy the generated gRPC server interface (compile-time guard).
var _ wavespanv1.BudgetServiceServer = (*Budget)(nil)

func TestNewBudgetNonNil(t *testing.T) {
	a := NewBudget(collections.NewService(nil, membership.Member{}))
	if a == nil {
		t.Fatal("NewBudget returned nil")
	}
}
