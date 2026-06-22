package profile

import (
	"bytes"
	"testing"

	"github.com/google/pprof/profile"
)

// buildCPU constructs a tiny CPU profile: sample1 has stack A<-B (leaf A), sample2 has leaf B.
func buildCPU(t *testing.T) []byte {
	t.Helper()
	fnA := &profile.Function{ID: 1, Name: "pkg.A"}
	fnB := &profile.Function{ID: 2, Name: "pkg.B"}
	locA := &profile.Location{ID: 1, Line: []profile.Line{{Function: fnA}}}
	locB := &profile.Location{ID: 2, Line: []profile.Line{{Function: fnB}}}
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}, {Type: "cpu", Unit: "nanoseconds"}},
		Sample: []*profile.Sample{
			{Location: []*profile.Location{locA, locB}, Value: []int64{1, 100}}, // A is leaf; B is up the stack
			{Location: []*profile.Location{locB}, Value: []int64{1, 50}},        // B is leaf
		},
		Function: []*profile.Function{fnA, fnB},
		Location: []*profile.Location{locA, locB},
	}
	var buf bytes.Buffer
	if err := p.Write(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestAnalyzeFlatVsCum(t *testing.T) {
	a, err := Analyze("cpu", buildCPU(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Unit != "nanoseconds" {
		t.Fatalf("expected the cpu/nanoseconds column, got unit %q", a.Unit)
	}
	if a.Total != 150 {
		t.Fatalf("total should be 100+50=150, got %d", a.Total)
	}
	// cum: B appears in both stacks -> 150; A only in the first -> 100. B should rank first by cum.
	if a.Cum[0].Function != "pkg.B" || a.Cum[0].Cum != 150 {
		t.Fatalf("cum leader should be B=150, got %s=%d", a.Cum[0].Function, a.Cum[0].Cum)
	}
	// flat: A is the leaf of 100, B the leaf of 50 -> A leads flat.
	if a.Flat[0].Function != "pkg.A" || a.Flat[0].Flat != 100 {
		t.Fatalf("flat leader should be A=100, got %s=%d", a.Flat[0].Function, a.Flat[0].Flat)
	}
}

func TestBuildReportAggregatesNodes(t *testing.T) {
	raw := buildCPU(t)
	cpuRaw := map[string][]byte{"node1": raw, "node2": raw} // same profile on 2 nodes
	rep := BuildReport("kv test", []string{"node1", "node2"}, 10, cpuRaw, nil)
	if len(rep.Sections) != 1 || rep.Sections[0].Kind != "cpu" {
		t.Fatalf("expected one cpu section, got %+v", rep.Sections)
	}
	// aggregated total is doubled (two nodes), and the report renders without panicking.
	if rep.Sections[0].Total != 300 {
		t.Fatalf("aggregate total should be 2x150=300, got %d", rep.Sections[0].Total)
	}
	if md := rep.Markdown(); !bytes.Contains([]byte(md), []byte("performance breakdown")) {
		t.Fatal("markdown should render a heading")
	}
	if len(rep.Sections[0].Notes) == 0 {
		t.Fatal("expected interpretation notes")
	}
}
