package global

import (
	"testing"

	"github.com/cwire/wavespan/internal/storage"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

func mut(seqHint int, ns string, key string) *wavespanv1.GlobalMutation {
	return &wavespanv1.GlobalMutation{
		Id:        &wavespanv1.GlobalMutationId{ClusterId: "test-a", MemberId: "a1", WriterSequence: uint64(seqHint)},
		Namespace: ns, Key: []byte(key), Partition: Partition(ns, []byte(key)),
		Record: &wavespanv1.StoredRecord{LogicalKey: []byte(key), Namespace: ns},
	}
}

func TestOutLogAppendIterate(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	l := NewOutLog(mem, 0)

	// append several mutations to the same partition (fixed key -> fixed partition)
	for i := 1; i <= 5; i++ {
		if err := l.Append("test-b", mut(i, "default", "k"), false); err != nil {
			t.Fatal(err)
		}
	}
	part := Partition("default", []byte("k"))
	entries, err := l.IterateFrom("test-b", part, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.Seq != uint64(i+1) {
			t.Fatalf("entries out of order: seq[%d]=%d", i, e.Seq)
		}
	}
	// iterate from a cursor
	tail, _ := l.IterateFrom("test-b", part, 3, 0)
	if len(tail) != 2 || tail[0].Seq != 4 {
		t.Fatalf("iterate-from cursor wrong: %+v", tail)
	}
}

func TestOutLogCheckpointGatesCompaction(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	l := NewOutLog(mem, 50) // tiny budget

	part := Partition("default", []byte("k"))
	for i := 1; i <= 10; i++ {
		if err := l.Append("test-b", mut(i, "default", "k"), false); err != nil {
			t.Fatalf("default append must not fail over budget: %v", err)
		}
	}
	// nothing dropped before a checkpoint advances, even over budget
	if n, _ := l.CompactBelowCheckpoint("test-b", part); n != 0 {
		t.Fatalf("no checkpoint yet -> nothing compactable, compacted %d", n)
	}
	if entries, _ := l.IterateFrom("test-b", part, 0, 0); len(entries) != 10 {
		t.Fatal("entries dropped before checkpoint")
	}
	// after a checkpoint, entries at/below it are compactable; above are retained
	l.Checkpoint("test-b", part, 6)
	n, _ := l.CompactBelowCheckpoint("test-b", part)
	if n != 6 {
		t.Fatalf("expected 6 entries compacted, got %d", n)
	}
	rest, _ := l.IterateFrom("test-b", part, 0, 0)
	if len(rest) != 4 || rest[0].Seq != 7 {
		t.Fatalf("entries above checkpoint should remain: %+v", rest)
	}
}

func TestOutLogBackpressureForGlobalDurability(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	l := NewOutLog(mem, 30)

	// fill past budget on the default path (never blocks)
	for i := 1; i <= 5; i++ {
		if err := l.Append("test-b", mut(i, "default", "k"), false); err != nil {
			t.Fatalf("default path must keep appending: %v", err)
		}
	}
	// a globalDurabilityRequired caller now hits backpressure
	if err := l.Append("test-b", mut(6, "default", "k"), true); err != ErrOutLogFull {
		t.Fatalf("globalDurabilityRequired over budget should return ErrOutLogFull, got %v", err)
	}
	// after the peer catches up (checkpoint) and we compact, the required write succeeds
	part := Partition("default", []byte("k"))
	l.Checkpoint("test-b", part, 5)
	if _, err := l.CompactBelowCheckpoint("test-b", part); err != nil {
		t.Fatal(err)
	}
	if err := l.Append("test-b", mut(6, "default", "k"), true); err != nil {
		t.Fatalf("after draining, required write should succeed: %v", err)
	}
}
