package collections

import (
	"context"
	"testing"
	"time"
)

// dragonboat's SyncPropose/SyncRead require the context to carry a deadline (they return
// ErrDeadlineNotSet otherwise). ensureDeadline guarantees one so a deadline-less client (e.g. the
// admin-port UI write) does not get spuriously treated as a leadership failure and forwarded.
func TestEnsureDeadlineAddsDefaultWhenMissing(t *testing.T) {
	ctx, cancel := ensureDeadline(context.Background())
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("ensureDeadline did not add a deadline to a deadline-less context")
	}
	d := time.Until(dl)
	if d <= 0 || d > defaultProposeTimeout+time.Second {
		t.Fatalf("default deadline = %v, want ~%v in the future", d, defaultProposeTimeout)
	}
}

func TestEnsureDeadlinePreservesExisting(t *testing.T) {
	want := time.Now().Add(123 * time.Millisecond)
	parent, pcancel := context.WithDeadline(context.Background(), want)
	defer pcancel()
	ctx, cancel := ensureDeadline(parent)
	defer cancel()
	got, ok := ctx.Deadline()
	if !ok {
		t.Fatal("existing deadline was dropped")
	}
	if !got.Equal(want) {
		t.Fatalf("deadline overridden: got %v, want %v", got, want)
	}
}
