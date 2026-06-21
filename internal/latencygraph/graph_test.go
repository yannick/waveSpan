package latencygraph

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestEWMAConvergesAndP95Tracks(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(1000, 0)
	// steady 10ms samples
	for i := 0; i < 50; i++ {
		g.AddSample("p1", 10*time.Millisecond, true, base.Add(time.Duration(i)*time.Second))
	}
	e, _ := g.Edge("p1")
	if e.EWMARttMs < 9.5 || e.EWMARttMs > 10.5 {
		t.Fatalf("EWMA should converge to ~10ms, got %.2f", e.EWMARttMs)
	}
	if e.P95RttMs < 9.5 || e.P95RttMs > 10.5 {
		t.Fatalf("p95 of constant 10ms should be ~10ms, got %.2f", e.P95RttMs)
	}
	if e.SampleCount != 50 {
		t.Fatalf("sample count = %d want 50", e.SampleCount)
	}
}

func TestP95IgnoresMajorityForTailLatency(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(2000, 0)
	// 90 fast samples, 10 slow: p95 should reflect the tail, not the median
	for i := 0; i < 90; i++ {
		g.AddSample("p1", 5*time.Millisecond, true, base.Add(time.Duration(i)*time.Millisecond))
	}
	for i := 90; i < 100; i++ {
		g.AddSample("p1", 200*time.Millisecond, true, base.Add(time.Duration(i)*time.Millisecond))
	}
	e, _ := g.Edge("p1")
	if e.P95RttMs < 50 {
		t.Fatalf("p95 should reflect the slow tail, got %.2f", e.P95RttMs)
	}
}

func TestPacketLossRisesOnFailures(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(3000, 0)
	g.AddSample("p1", 10*time.Millisecond, true, base)
	loss0, _ := g.Edge("p1")
	for i := 1; i <= 10; i++ {
		g.AddSample("p1", 0, false, base.Add(time.Duration(i)*time.Second))
	}
	loss1, _ := g.Edge("p1")
	if loss1.PacketLoss <= loss0.PacketLoss {
		t.Fatalf("packet loss should rise after failures: %.3f -> %.3f", loss0.PacketLoss, loss1.PacketLoss)
	}
}

func TestEdgeExpiry(t *testing.T) {
	g := New(Config{EWMAAlpha: 0.2, RefMaxRttMs: 50, EdgeExpiry: 10 * time.Second, RecentWindow: 16, Weights: DefaultScoreWeights()})
	base := time.Unix(4000, 0)
	g.AddSample("p1", 10*time.Millisecond, true, base)
	g.AddSample("p2", 10*time.Millisecond, true, base.Add(20*time.Second))
	// expire as of base+25s: p1 (last sample at base) is stale, p2 is fresh
	g.Expire(base.Add(25 * time.Second))
	if _, ok := g.Edge("p1"); ok {
		t.Fatal("stale edge p1 should be expired")
	}
	if _, ok := g.Edge("p2"); !ok {
		t.Fatal("fresh edge p2 should survive")
	}
}

func TestScoreInjectedLatencyRaisesScore(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(5000, 0)
	g.AddSample("fast", 2*time.Millisecond, true, base)
	g.AddSample("slow", 2*time.Millisecond, true, base)

	fast0 := g.Score("fast", 0, 0, 0)
	slow0 := g.Score("slow", 0, 0, 0)
	if fast0 != slow0 {
		t.Fatalf("equal-latency peers should score equally: %.4f vs %.4f", fast0, slow0)
	}

	// inject +100ms on slow
	for i := 1; i <= 20; i++ {
		g.AddSample("slow", 100*time.Millisecond, true, base.Add(time.Duration(i)*time.Second))
	}
	if g.Score("slow", 0, 0, 0) <= g.Score("fast", 0, 0, 0) {
		t.Fatal("injecting latency must raise the peer's score (worse)")
	}
}

func TestScoreDeterministic(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(6000, 0)
	g.AddSample("p1", 7*time.Millisecond, true, base)
	a := g.Score("p1", 0.1, 0.2, 0.4)
	b := g.Score("p1", 0.1, 0.2, 0.4)
	if a != b {
		t.Fatalf("score not deterministic: %.6f != %.6f", a, b)
	}
}

func TestTopologyPenaltyTable(t *testing.T) {
	cases := []struct {
		name                       string
		sz, sr, sg, pz, pr, pg     string
		want                       float64
	}{
		{"same zone", "a", "r", "g", "a", "r", "g", 0},
		{"same region diff zone", "a", "r", "g", "b", "r", "g", 0.2},
		{"same geo diff region", "a", "r1", "g", "a", "r2", "g", 0.4},
		{"diff geo", "a", "r", "g1", "a", "r", "g2", 1.0},
		{"unknown", "", "", "", "a", "r", "g", 0.5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := TopologyPenalty(c.sz, c.sr, c.sg, c.pz, c.pr, c.pg); got != c.want {
				t.Fatalf("penalty = %.2f want %.2f", got, c.want)
			}
		})
	}
}

func TestProberRecordsSamples(t *testing.T) {
	g := New(DefaultConfig())
	base := time.Unix(7000, 0)
	now := base
	calls := 0
	p := NewProber(g, func(_ context.Context, _ string) (time.Duration, error) {
		calls++
		if calls == 2 {
			return 0, errors.New("timeout")
		}
		return 5 * time.Millisecond, nil
	}, func() time.Time { now = now.Add(time.Second); return now })

	if !p.Probe(context.Background(), "p1", "p1:7700") {
		t.Fatal("first probe should succeed")
	}
	if p.Probe(context.Background(), "p1", "p1:7700") {
		t.Fatal("second probe should report failure")
	}
	e, ok := g.Edge("p1")
	if !ok || e.SampleCount != 2 {
		t.Fatalf("prober should have recorded 2 samples, got %+v", e)
	}
}
