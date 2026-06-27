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

func TestPoolRecRoundTrip(t *testing.T) {
	p := poolRec{Cap: 500_000_000, Available: 400_000_000, LeasedOut: 90_000_000, Spent: 10_000_000, Epoch: 3, Mode: modeStrict, Rate: 0, Burst: 500_000_000}
	got, err := decodePool(encodePool(p))
	if err != nil {
		t.Fatalf("decodePool: %v", err)
	}
	if got != p {
		t.Fatalf("round-trip = %+v, want %+v", got, p)
	}
}

func TestLeaseRecRoundTrip(t *testing.T) {
	l := leaseRec{Holder: []byte("node-7"), Amount: 600_000, Spent: 250_000, Epoch: 3}
	got, err := decodeLease(encodeLease(l))
	if err != nil {
		t.Fatalf("decodeLease: %v", err)
	}
	if string(got.Holder) != string(l.Holder) || got.Amount != l.Amount || got.Spent != l.Spent || got.Epoch != l.Epoch {
		t.Fatalf("round-trip = %+v, want %+v", got, l)
	}
}
