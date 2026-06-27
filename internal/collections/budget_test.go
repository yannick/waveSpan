package collections

import "testing"

func TestBudgetOpKindsAndType(t *testing.T) {
	// New opKinds are distinct and contiguous after opBatch(15).
	ops := []opKind{opBudInit, opBudGrant, opBudReport, opBudReturn}
	seen := map[opKind]bool{}
	for _, o := range ops {
		if o <= opBatch {
			t.Fatalf("op %d must be > opBatch(%d)", o, opBatch)
		}
		if seen[o] {
			t.Fatalf("duplicate opKind %d", o)
		}
		seen[o] = true
	}
	// typeForOp maps all four budget ops to typeBudget.
	for _, o := range ops {
		if got := typeForOp(o); got != typeBudget {
			t.Fatalf("typeForOp(%d) = %d, want typeBudget(%d)", o, got, typeBudget)
		}
	}
}
