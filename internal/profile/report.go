package profile

import (
	"fmt"
	"sort"
	"strings"
)

// Section is one profile dimension aggregated across the cluster.
type Section struct {
	Kind    string
	Title   string
	Explain string
	Unit    string
	Total   int64
	Agg     []FuncStat // full aggregate across nodes, sorted by cum desc
	App     []FuncStat // application/storage/RPC frames only (the actionable view)
	FwShare float64    // fraction of leaf cost in Go runtime / HTTP plumbing
	PerNode map[string]*Analysis
	Notes   []string
}

// Report is the full cross-node performance breakdown.
type Report struct {
	Bench      string
	Nodes      []string
	CPUSeconds int
	Sections   []Section
}

type sectionSpec struct{ kind, title, explain string }

var sectionOrder = []sectionSpec{
	{"cpu", "CPU — where on-CPU time goes", "Sampled on-CPU work. High cumulative % = code paths burning CPU."},
	{"block", "Latency — where REQUEST goroutines BLOCK (off-CPU)", "Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted."},
	{"mutex", "Lock contention (request path)", "Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded."},
	{"alloc", "Allocations (GC pressure)", "Bytes allocated since start. Heavy allocation drives GC, which adds tail latency."},
	{"goroutine", "Goroutine concurrency snapshot", "Where goroutines were parked at capture time — corroborates the blocking story."},
}

// focusByKind isolates request-path blocking/contention from idle background loops by keeping only
// samples whose stack passes through a request handler.
var focusByKind = map[string][]string{
	"block": {"ServeHTTP"},
	"mutex": {"ServeHTTP"},
}

// BuildReport analyzes the captured profiles and aggregates them across nodes.
func BuildReport(benchDesc string, nodes []string, cpuSeconds int, cpuRaw map[string][]byte, snapRaw map[string]map[string][]byte) *Report {
	r := &Report{Bench: benchDesc, Nodes: nodes, CPUSeconds: cpuSeconds}
	for _, spec := range sectionOrder {
		perNode := map[string]*Analysis{}
		for _, node := range nodes {
			var raw []byte
			if spec.kind == "cpu" {
				raw = cpuRaw[node]
			} else if snapRaw[node] != nil {
				raw = snapRaw[node][spec.kind]
			}
			if len(raw) == 0 {
				continue
			}
			if a, err := Analyze(spec.kind, raw, focusByKind[spec.kind]); err == nil {
				perNode[node] = a
			}
		}
		if len(perNode) == 0 {
			continue
		}
		agg, total, unit := aggregate(perNode)
		sec := Section{
			Kind: spec.kind, Title: spec.title, Explain: spec.explain, Unit: unit,
			Total: total, Agg: agg, App: filterApp(agg), FwShare: frameworkFlatShare(agg, total), PerNode: perNode,
		}
		notesSrc := sec.App
		if len(notesSrc) == 0 {
			notesSrc = agg // nothing matched the app filter (e.g. a synthetic profile) — annotate the raw top
		}
		sec.Notes = notesFor(spec.kind, notesSrc, total, unit)
		r.Sections = append(r.Sections, sec)
	}
	return r
}

func aggregate(perNode map[string]*Analysis) (agg []FuncStat, total int64, unit string) {
	accum := map[string]*FuncStat{}
	get := func(name string) *FuncStat {
		s, ok := accum[name]
		if !ok {
			s = &FuncStat{Function: name}
			accum[name] = s
		}
		return s
	}
	for _, a := range perNode {
		total += a.Total
		unit = a.Unit
		for _, fs := range a.Cum {
			get(fs.Function).Cum += fs.Cum
		}
		for _, fs := range a.Flat {
			get(fs.Function).Flat += fs.Flat
		}
	}
	out := make([]FuncStat, 0, len(accum))
	for _, s := range accum {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cum != out[j].Cum {
			return out[i].Cum > out[j].Cum
		}
		return out[i].Function < out[j].Function
	})
	return out, total, unit
}

// Markdown renders the full breakdown.
func (r *Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# WaveSpan performance breakdown\n\n")
	fmt.Fprintf(&b, "**Workload:** %s\n\n", r.Bench)
	fmt.Fprintf(&b, "**Nodes profiled:** %s · **CPU profile window:** %ds\n\n", strings.Join(r.Nodes, ", "), r.CPUSeconds)
	b.WriteString("> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.\n\n")

	for _, sec := range r.Sections {
		fmt.Fprintf(&b, "## %s\n\n%s\n\n", sec.Title, sec.Explain)
		fmt.Fprintf(&b, "Total sampled: **%s** across %d node(s). Go runtime / HTTP-framework plumbing accounts for **%.0f%%** of leaf cost; the application/storage/RPC frames below are the rest.\n\n",
			human(sec.Unit, sec.Total), len(sec.PerNode), 100*sec.FwShare)
		if len(sec.Notes) > 0 {
			for _, n := range sec.Notes {
				fmt.Fprintf(&b, "- %s\n", n)
			}
			b.WriteString("\n")
		}
		b.WriteString("Top application / storage / RPC frames (cum = time spent in this function + everything it calls):\n\n")
		b.WriteString("| # | function | cum | cum % | flat % |\n|---|---|--:|--:|--:|\n")
		rows := topN(sec.App, 12)
		if len(rows) == 0 {
			rows = topN(sec.Agg, 8) // fall back to the raw view if nothing matched the app filter
		}
		for i, fs := range rows {
			fmt.Fprintf(&b, "| %d | `%s` | %s | %s | %s |\n",
				i+1, shortFn(fs.Function), human(sec.Unit, fs.Cum), pct(fs.Cum, sec.Total), pct(fs.Flat, sec.Total))
		}
		b.WriteString("\n")
	}
	b.WriteString("---\n_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._\n")
	return b.String()
}

// Console renders a short summary (top 3 per section).
func (r *Report) Console() string {
	var b strings.Builder
	fmt.Fprintf(&b, "WaveSpan performance breakdown — %s\n", r.Bench)
	for _, sec := range r.Sections {
		fmt.Fprintf(&b, "\n[%s]  total=%s  (runtime/framework %.0f%%)\n", strings.ToUpper(sec.Kind), human(sec.Unit, sec.Total), 100*sec.FwShare)
		rows := topN(sec.App, 4)
		if len(rows) == 0 {
			rows = topN(sec.Agg, 4)
		}
		for i, fs := range rows {
			fmt.Fprintf(&b, "  %d. %-48s %6s cum\n", i+1, shortFn(fs.Function), pct(fs.Cum, sec.Total))
		}
		for _, n := range sec.Notes {
			fmt.Fprintf(&b, "  → %s\n", stripMarkdown(n))
		}
	}
	return b.String()
}

func topN(s []FuncStat, n int) []FuncStat {
	if len(s) > n {
		return s[:n]
	}
	return s
}
