package benchengine

import (
	"context"
	"testing"
	"time"
)

func TestWithDeadlineAddsWhenAbsent(t *testing.T) {
	ctx, cancel := withDeadline(context.Background(), 5*time.Second)
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("withDeadline must add a deadline when the context has none")
	}
	if d := time.Until(dl); d <= 0 || d > 6*time.Second {
		t.Fatalf("deadline = %v from now, want ~5s", d)
	}
}

func TestWithDeadlinePreservesExisting(t *testing.T) {
	base, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, cancel2 := withDeadline(base, time.Hour)
	defer cancel2()
	dl, _ := got.Deadline()
	if d := time.Until(dl); d > 3*time.Second {
		t.Fatalf("withDeadline must preserve the existing (shorter) deadline, got %v from now", d)
	}
}
