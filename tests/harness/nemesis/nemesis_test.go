package nemesis

import (
	"testing"

	"github.com/yannick/wavespan/tests/harness/runner"
)

func TestNemesisStartStopHeal(t *testing.T) {
	h := &runner.History{}
	injected, healed := false, false
	n := New("node-kill", func([]string) { injected = true }, func([]string) { healed = true })

	n.Start(h, []string{"n2"}, 100)
	if !injected || len(h.Faults) != 1 || h.Faults[0].Kind != "node-kill" || h.Faults[0].EndMs != 0 {
		t.Fatalf("start should inject + record an open fault: injected=%v faults=%+v", injected, h.Faults)
	}
	n.Stop(h, 300)
	if !healed || h.Faults[0].EndMs != 300 {
		t.Fatalf("stop should heal + close the fault: healed=%v fault=%+v", healed, h.Faults[0])
	}
	if h.HealedAtMs() != 300 {
		t.Fatalf("history should be healed at 300, got %d", h.HealedAtMs())
	}
}

func TestNemesisComposition(t *testing.T) {
	h := &runner.History{}
	kill := New("node-kill", nil, nil)
	latency := New("latency", nil, nil)
	comp := NewCompose("kill+latency", kill, latency)

	comp.Start(h, []string{"n2"}, 100)
	comp.Stop(h, 200)
	if len(h.Faults) != 2 {
		t.Fatalf("composition should record both faults, got %d", len(h.Faults))
	}
	kinds := map[string]bool{}
	for _, f := range h.Faults {
		kinds[f.Kind] = true
		if f.EndMs != 200 {
			t.Fatalf("both faults should be healed at 200: %+v", f)
		}
	}
	if !kinds["node-kill"] || !kinds["latency"] {
		t.Fatalf("both nemeses should appear on the timeline: %v", kinds)
	}
}

func TestKillOriginAfterAckWindow(t *testing.T) {
	// the durability nemesis kills the origin within a bounded window after a write ack.
	h := &runner.History{}
	ackMs := int64(1000)
	n := New("kill-origin-after-ack", nil, nil)
	n.Start(h, []string{"origin-node"}, ackMs+50) // within 50ms of the ack
	if h.Faults[0].StartMs-ackMs > 200 {
		t.Fatalf("kill should fall within the bounded post-ack window, gap=%dms", h.Faults[0].StartMs-ackMs)
	}
}
