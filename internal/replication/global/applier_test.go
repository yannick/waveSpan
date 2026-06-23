package global

import (
	"testing"

	"github.com/yannick/wavespan/internal/conflict"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/storage"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func newRecStore(t *testing.T, member string) *recordstore.Store {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	return recordstore.NewStore(mem, "dev", member, version.NewClock(nil, 500), version.NewSequencer(0))
}

func gmut(cluster, member string, seq uint64, phys uint64, ns, key, value string, tombstone bool, expires *int64) *wavespanv1.GlobalMutation {
	v := version.Version{HLCPhysicalMs: phys, WriterClusterID: cluster, WriterMemberID: member, WriterSequence: seq}
	rec := &wavespanv1.StoredRecord{LogicalKey: []byte(key), Namespace: ns, Version: v.ToProto(), Tombstone: tombstone, ExpiresAtUnixMs: expires}
	if !tombstone {
		rec.Value = &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte(value)}}
	}
	return &wavespanv1.GlobalMutation{
		Id:        &wavespanv1.GlobalMutationId{ClusterId: cluster, MemberId: member, WriterSequence: seq},
		Namespace: ns, Key: []byte(key), Record: rec, Partition: Partition(ns, []byte(key)),
	}
}

func TestApplyIdempotent(t *testing.T) {
	s := newRecStore(t, "b1")
	a := NewApplier(s, conflict.NewRegistry(), nil)
	m := gmut("test-a", "a1", 1, 100, "default", "k", "v", false, nil)

	applied, err := a.Apply(m)
	if err != nil || !applied {
		t.Fatalf("first apply should be new: %v %v", applied, err)
	}
	applied2, err := a.Apply(m) // replay
	if err != nil || applied2 {
		t.Fatalf("replay should be a no-op: %v %v", applied2, err)
	}
	if out, _ := s.Get("default", []byte("k")); !out.Found || string(out.Value) != "v" {
		t.Fatalf("value not applied: %+v", out)
	}
}

func TestApplyKeepSiblingsStoresBoth(t *testing.T) {
	s := newRecStore(t, "b1")
	a := NewApplier(s, conflict.NewRegistry(), func(string) string { return conflict.PolicyKeepSiblings })

	// two concurrent writes from different clusters to the same key
	if _, err := a.Apply(gmut("test-a", "a1", 1, 100, "default", "k", "from-a", false, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(gmut("test-b", "b1", 1, 100, "default", "k", "from-b", false, nil)); err != nil {
		t.Fatal(err)
	}
	// the latest pointer should report siblings present
	out, _ := s.Get("default", []byte("k"))
	if out.ConflictNone {
		t.Fatalf("keep-siblings should record SIBLINGS_PRESENT: %+v", out)
	}
}

func TestApplyDefaultLWWKeepsWinner(t *testing.T) {
	s := newRecStore(t, "b1")
	a := NewApplier(s, conflict.NewRegistry(), nil) // default LWW
	if _, err := a.Apply(gmut("test-a", "a1", 1, 100, "default", "k", "lo", false, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(gmut("test-b", "b1", 1, 200, "default", "k", "hi", false, nil)); err != nil {
		t.Fatal(err)
	}
	if out, _ := s.Get("default", []byte("k")); string(out.Value) != "hi" {
		t.Fatalf("LWW should keep the higher-HLC value, got %q", out.Value)
	}
}

func TestApplyTTLUsesOriginExpiry(t *testing.T) {
	s := newRecStore(t, "b1")
	a := NewApplier(s, conflict.NewRegistry(), nil)
	origin := int64(9_999_999_999)
	if _, err := a.Apply(gmut("test-a", "a1", 1, 100, "default", "k", "v", false, &origin)); err != nil {
		t.Fatal(err)
	}
	out, _ := s.Get("default", []byte("k"))
	if out.ExpiresAtMs == nil || *out.ExpiresAtMs != origin {
		t.Fatalf("applied expiry must equal origin expiry, got %v want %d", out.ExpiresAtMs, origin)
	}
}

// TestApplyOnApplyHookSpreads: a successful inbound apply fires the onApply hook (so the receiving
// node can fanout an everywhere namespace), and a replay does not fire it again.
func TestApplyOnApplyHookSpreads(t *testing.T) {
	s := newRecStore(t, "b1")
	a := NewApplier(s, conflict.NewRegistry(), nil)
	var fired [][]byte
	a.SetOnApply(func(_ string, key []byte, _ *wavespanv1.StoredRecord) { fired = append(fired, key) })

	m := gmut("test-a", "a1", 1, 100, "ref", "k", "v", false, nil)
	if _, err := a.Apply(m); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(m); err != nil { // replay
		t.Fatal(err)
	}
	if len(fired) != 1 || string(fired[0]) != "k" {
		t.Fatalf("onApply should fire once for a new apply, got %d", len(fired))
	}
}
