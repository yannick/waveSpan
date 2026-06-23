package conflict

import (
	"testing"

	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

func rec(phys uint64, cluster, member string, seq uint64, value string, tombstone bool) *wavespanv1.StoredRecord {
	v := version.Version{HLCPhysicalMs: phys, WriterClusterID: cluster, WriterMemberID: member, WriterSequence: seq}
	r := &wavespanv1.StoredRecord{LogicalKey: []byte("k"), Namespace: "default", Version: v.ToProto(), Tombstone: tombstone}
	if !tombstone {
		r.Value = &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte(value)}}
	}
	return r
}

func TestHLCLWWHigherVersionWinsDeterministically(t *testing.T) {
	r := HLCLastWriteWins{}
	a := rec(100, "A", "m1", 1, "a", false)
	b := rec(200, "B", "m2", 1, "b", false)
	// higher HLC wins regardless of argument order
	if got := r.Resolve([]*wavespanv1.StoredRecord{a}, b); got.Kind != KindWinner || string(got.Record.GetValue().GetInline()) != "b" {
		t.Fatalf("higher HLC should win: %+v", got)
	}
	if got := r.Resolve([]*wavespanv1.StoredRecord{b}, a); got.Kind != KindWinner || string(got.Record.GetValue().GetInline()) != "b" {
		t.Fatalf("winner must be order-independent: %+v", got)
	}
	// equal HLC -> deterministic tie-break by writer cluster then member
	c1 := rec(100, "A", "m1", 9, "c1", false)
	c2 := rec(100, "B", "m1", 9, "c2", false)
	if got := r.Resolve([]*wavespanv1.StoredRecord{c1}, c2); string(got.Record.GetValue().GetInline()) != "c2" {
		t.Fatalf("equal HLC should tie-break to higher cluster id: %+v", got)
	}
}

func TestHLCLWWTombstoneWinsIfVersionWins(t *testing.T) {
	r := HLCLastWriteWins{}
	value := rec(100, "A", "m1", 1, "v", false)
	tombHi := rec(300, "A", "m1", 2, "", true)
	if got := r.Resolve([]*wavespanv1.StoredRecord{value}, tombHi); got.Kind != KindTombstone {
		t.Fatalf("higher-version tombstone should win: %+v", got)
	}
	tombLo := rec(50, "A", "m1", 0, "", true)
	if got := r.Resolve([]*wavespanv1.StoredRecord{value}, tombLo); got.Kind != KindWinner || string(got.Record.GetValue().GetInline()) != "v" {
		t.Fatalf("lower-version tombstone must lose to the live value: %+v", got)
	}
}

func TestKeepSiblingsConcurrentWritersReturnBoth(t *testing.T) {
	r := KeepSiblings{}
	a := rec(100, "A", "m1", 1, "a", false)
	b := rec(100, "B", "m2", 1, "b", false) // different cluster -> concurrent
	got := r.Resolve([]*wavespanv1.StoredRecord{a}, b)
	if got.Kind != KindSiblings || len(got.Siblings) != 2 {
		t.Fatalf("concurrent writers should yield 2 siblings: %+v", got)
	}
}

func TestKeepSiblingsSameWriterSuccessorCollapses(t *testing.T) {
	r := KeepSiblings{}
	a := rec(100, "A", "m1", 1, "a", false)
	a2 := rec(200, "A", "m1", 2, "a2", false) // same writer, later -> successor
	got := r.Resolve([]*wavespanv1.StoredRecord{a}, a2)
	if got.Kind != KindWinner || string(got.Record.GetValue().GetInline()) != "a2" {
		t.Fatalf("same-writer successor should collapse to a single winner: %+v", got)
	}
}

func TestRegistryDefaultsToLWW(t *testing.T) {
	reg := NewRegistry()
	if _, ok := reg.Resolver(PolicyHLCLastWriteWins).(HLCLastWriteWins); !ok {
		t.Fatal("hlc-last-write-wins should resolve to HLCLastWriteWins")
	}
	if _, ok := reg.Resolver(PolicyKeepSiblings).(KeepSiblings); !ok {
		t.Fatal("keep-siblings should resolve to KeepSiblings")
	}
	// unknown policy falls back to LWW (never panics)
	if _, ok := reg.Resolver("crdt-or-set").(HLCLastWriteWins); !ok {
		t.Fatal("unknown policy should fall back to LWW, not a deferred panic resolver")
	}
}
