package runner

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cwire/wavespan/internal/version"
)

func TestHistoryRecordsOpsAndFaults(t *testing.T) {
	h := &History{Seed: 42}
	h.Append(Op{Kind: OpPut, Key: "a", Value: "1", RequestID: "r1", StartMs: 10, EndMs: 12, Ack: true,
		WriterVersion: &version.Version{HLCPhysicalMs: 100, WriterMemberID: "n1"}})
	h.AppendFault(Fault{Kind: "partition-halves", Targets: []string{"n2"}, StartMs: 20, EndMs: 40})

	b, err := h.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ops) != 1 || got.Ops[0].Key != "a" || !got.Ops[0].Ack {
		t.Fatalf("op did not round-trip: %+v", got.Ops)
	}
	if got.Ops[0].WriterVersion == nil || got.Ops[0].WriterVersion.HLCPhysicalMs != 100 {
		t.Fatal("writer version lost in round-trip")
	}
	if len(got.Faults) != 1 || got.Faults[0].Kind != "partition-halves" {
		t.Fatalf("fault did not round-trip: %+v", got.Faults)
	}
	if got.HealedAtMs() != 40 {
		t.Fatalf("healed-at = %d, want 40", got.HealedAtMs())
	}
}

func TestSeedDeterministic(t *testing.T) {
	members := []string{"n1", "n2", "n3"}
	kinds := []string{"kill", "partition-halves", "latency"}
	key := func(s *Schedule) string {
		var b strings.Builder
		for _, n := range s.Nemeses {
			fmt.Fprintf(&b, "%s|%v|%d|%d;", n.Kind, n.Targets, n.StartMs, n.DurationMs)
		}
		return b.String()
	}
	a := NewSchedule(7, 100, kinds, members)
	b := NewSchedule(7, 100, kinds, members)
	if key(a) != key(b) {
		t.Fatalf("same seed diverged:\n%s\n%s", key(a), key(b))
	}
	if key(a) == key(NewSchedule(8, 100, kinds, members)) {
		t.Fatal("different seeds produced identical schedules")
	}
}

func TestForensicDumpShape(t *testing.T) {
	h := &History{Seed: 99}
	h.Append(Op{Kind: OpGet, Key: "k", Value: "v1", ServedBy: "n1", Ack: true, StartMs: 100,
		ObservedVersion: &version.Version{HLCPhysicalMs: 5, WriterMemberID: "n1"}})
	h.Append(Op{Kind: OpGet, Key: "k", Value: "v2", ServedBy: "n2", Ack: true, StartMs: 100,
		ObservedVersion: &version.Version{HLCPhysicalMs: 6, WriterMemberID: "n2"}})
	h.AppendFault(Fault{Kind: "partition-halves", Targets: []string{"n2"}, StartMs: 10, EndMs: 50})

	out := Dump(h, Violation{Property: "convergence", Detail: "replicas disagree", Key: "k", Window: [2]int64{100, 100}})
	for _, want := range []string{"convergence", "seed: 99", "per-replica", "n1", "n2", "partition-halves"} {
		if !strings.Contains(out, want) {
			t.Fatalf("dump missing %q:\n%s", want, out)
		}
	}
}
