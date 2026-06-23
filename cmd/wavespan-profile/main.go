// Command wavespan-profile captures Go runtime profiles (CPU, heap, block, mutex, goroutine) from
// every node WHILE a benchmark workload runs, then prints + writes a cross-node breakdown of where
// CPU, allocations, and — most importantly — latency (off-CPU blocking) go.
//
// The nodes must run with WAVESPAN_PROFILING_ENABLED=true (and ideally WAVESPAN_BLOCK_PROFILE_RATE /
// WAVESPAN_MUTEX_PROFILE_FRACTION) so /debug/pprof is served on the admin port. See
// docker/docker-compose.profile.yaml.
//
//	wavespan-profile --nodes node1=localhost:7921,node2=localhost:7922,node3=localhost:7923 \
//	  --data localhost:7831 --workload kv --seconds 20 --concurrency 32 --out perf-report
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yannick/wavespan/internal/bench"
	"github.com/yannick/wavespan/internal/profile"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wavespan-profile:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("wavespan-profile", flag.ContinueOnError)
	nodesArg := fs.String("nodes", "", "comma list of name=adminAddr (pprof endpoints), e.g. node1=localhost:7921,node2=localhost:7922")
	data := fs.String("data", "localhost:7831", "data-port address the workload drives")
	workload := fs.String("workload", "kv", "workload: kv | query")
	seconds := fs.Int("seconds", 20, "profile window / workload duration (s)")
	conc := fs.Int("concurrency", 32, "workload concurrency")
	keys := fs.Int("keys", 5000, "kv: key space")
	readRatio := fs.Float64("read-ratio", 0.7, "kv: read fraction")
	queriesDir := fs.String("queries", "bench/queries", "query: .cypher folder")
	graph := fs.String("graph", "g", "query: graph id")
	out := fs.String("out", "perf-report", "output directory for the report")
	if err := fs.Parse(args); err != nil {
		return err
	}
	nodes, err := parseNodes(*nodesArg)
	if err != nil {
		return err
	}
	ctx := context.Background()

	// Preflight: profiling must be enabled on every node.
	for _, n := range nodes {
		if !profile.Reachable(ctx, n) {
			return fmt.Errorf("node %s (%s) has no /debug/pprof — start it with WAVESPAN_PROFILING_ENABLED=true", n.Name, n.AdminAddr)
		}
	}
	fmt.Printf("profiling %d node(s) for %ds while running the %q workload against %s...\n", len(nodes), *seconds, *workload, *data)

	// Run the workload concurrently with the CPU capture so the profile covers it.
	desc := fmt.Sprintf("%s @ concurrency=%d for %ds", *workload, *conc, *seconds)
	wlDone := make(chan string, 1)
	go func() {
		wlDone <- runWorkload(*workload, *data, *seconds, *conc, *keys, *readRatio, *queriesDir, *graph)
	}()

	cpuRaw := profile.CaptureCPU(ctx, nodes, *seconds)
	wlSummary := <-wlDone
	snapRaw := profile.CaptureSnapshots(ctx, nodes)

	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.Name
	}
	report := profile.BuildReport(desc, names, *seconds, cpuRaw, snapRaw)

	// Console: the client-side latency numbers, then the server-side breakdown.
	fmt.Printf("\n--- client-side workload result ---\n%s\n", wlSummary)
	fmt.Println(report.Console())

	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	// Save the raw profiles too, so they can be opened with `go tool pprof perf-report/<file>`.
	saveRaw(*out, "cpu", cpuRaw)
	for node, kinds := range snapRaw {
		for kind, raw := range kinds {
			_ = os.WriteFile(filepath.Join(*out, fmt.Sprintf("%s.%s.pb.gz", node, kind)), raw, 0o644) //nolint:gosec,errcheck
		}
	}
	mdPath := filepath.Join(*out, "breakdown.md")
	md := "<!-- client-side workload result\n" + wlSummary + "\n-->\n\n" + report.Markdown()
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil { //nolint:gosec // report is not sensitive
		return err
	}
	warnEmptyProfiles(report)
	fmt.Printf("\nwrote %s\n", mdPath)
	return nil
}

// runWorkload drives the chosen workload for ~seconds and returns its client-side latency summary.
func runWorkload(kind, addr string, seconds, conc, keys int, readRatio float64, queriesDir, graph string) string {
	dur := time.Duration(seconds) * time.Second
	switch kind {
	case "kv":
		res := bench.RunKV(addr, bench.KVOptions{Concurrency: conc, Keys: keys, ReadRatio: readRatio, Duration: dur})
		return res.Get.Report("kv-get", dur) + "\n" + res.Put.Report("kv-put", dur)
	case "query":
		queries, err := bench.LoadQueries(queriesDir)
		if err != nil || len(queries) == 0 {
			return fmt.Sprintf("query workload unavailable: %v", err)
		}
		per := dur / time.Duration(len(queries)) // fit the whole suite in the window
		var b strings.Builder
		for _, r := range bench.RunQueries(addr, graph, queries, conc, per) {
			b.WriteString(r.Lat.Report(r.Name, per) + "\n")
		}
		return strings.TrimRight(b.String(), "\n")
	default:
		return "unknown workload " + kind
	}
}

func saveRaw(dir, kind string, byNode map[string][]byte) {
	for node, raw := range byNode {
		_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%s.pb.gz", node, kind)), raw, 0o644) //nolint:gosec,errcheck
	}
}

func parseNodes(s string) ([]profile.Node, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("--nodes is required (name=adminAddr,...)")
	}
	var nodes []profile.Node
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad --nodes entry %q (want name=host:port)", part)
		}
		nodes = append(nodes, profile.Node{Name: kv[0], AdminAddr: kv[1]})
	}
	return nodes, nil
}

func warnEmptyProfiles(r *profile.Report) {
	have := map[string]bool{}
	for _, s := range r.Sections {
		have[s.Kind] = true
	}
	if !have["block"] {
		fmt.Println("note: no block profile — set WAVESPAN_BLOCK_PROFILE_RATE on the nodes to see where latency blocks.")
	}
	if !have["mutex"] {
		fmt.Println("note: no mutex profile — set WAVESPAN_MUTEX_PROFILE_FRACTION on the nodes to see lock contention.")
	}
}
