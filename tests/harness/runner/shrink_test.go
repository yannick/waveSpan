package runner

import (
	"os"
	"testing"

	"github.com/yannick/wavespan/internal/version"
)

func TestShrinkProducesMinimalRepro(t *testing.T) {
	// a history with two unrelated keys; only key "bad" violates a "two distinct post-heal reads"
	// property. Shrinking should drop everything except the two offending reads.
	h := &History{Seed: 1}
	h.AppendFault(Fault{Kind: "partition-halves", StartMs: 10, EndMs: 50})
	h.AppendFault(Fault{Kind: "latency", StartMs: 5, EndMs: 20}) // unrelated, should be droppable
	for i := 0; i < 10; i++ {
		h.Append(Op{Kind: OpPut, Key: "noise", Value: "x", Ack: true, WriterVersion: &version.Version{HLCPhysicalMs: uint64(i)}})
	}
	h.Append(Op{Kind: OpGet, Key: "bad", Value: "1", Ack: true, ServedBy: "n1", StartMs: 100})
	h.Append(Op{Kind: OpGet, Key: "bad", Value: "2", Ack: true, ServedBy: "n2", StartMs: 100})

	// the "violation": >1 distinct post-heal value for some key
	fails := func(hh *History) bool {
		vals := map[string]bool{}
		for _, r := range hh.PostHealReads("bad") {
			vals[r.Value] = true
		}
		return len(vals) > 1
	}
	if !fails(h) {
		t.Fatal("setup history should fail")
	}
	minH := Shrink(h, fails)
	if !fails(minH) {
		t.Fatal("shrunk history must still reproduce the violation")
	}
	if len(minH.Ops) != 2 {
		t.Fatalf("shrinker should keep only the 2 offending reads, kept %d ops", len(minH.Ops))
	}
	// neither fault is needed to reproduce this property (with no fault HealedAtMs=0, so the reads
	// are still post-heal) — the shrinker correctly drops both.
	if len(minH.Faults) != 0 {
		t.Fatalf("shrinker should drop the unneeded faults, kept %+v", minH.Faults)
	}

	// emit a standalone repro and confirm it round-trips
	dir := t.TempDir()
	path, err := EmitRepro(minH, "demo", dir)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	replayed, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if !fails(replayed) {
		t.Fatal("emitted repro must replay the violation deterministically")
	}
}
