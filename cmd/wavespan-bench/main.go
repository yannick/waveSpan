// Command wavespan-bench is a load + benchmark client for WaveSpan. It bulk-loads test data (KV +
// a social graph) and replays a folder of Cypher queries under concurrency, reporting throughput
// and p50/p95/p99 latency per query. The workloads live in internal/bench so wavespan-profile drives
// identical traffic while profiling.
//
//	wavespan-bench load  --addr localhost:7811 --users 2000 --follows 6000 --kv 5000
//	wavespan-bench query --addr localhost:7811 --queries bench/queries --concurrency 16 --duration 30s
//	wavespan-bench kv    --addr localhost:7811 --concurrency 32 --duration 20s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cwire/wavespan/internal/bench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "load":
		err = loadCmd(os.Args[2:])
	case "query":
		err = queryCmd(os.Args[2:])
	case "kv":
		err = kvCmd(os.Args[2:])
	case "multiget":
		err = multigetCmd(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "wavespan-bench:", err)
		os.Exit(1)
	}
}

func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	graph := fs.String("graph", "g", "graph id")
	users := fs.Int("users", 2000, "User nodes to create")
	follows := fs.Int("follows", 6000, "FOLLOWS edges to create")
	kv := fs.Int("kv", 5000, "KV keys to write")
	conc := fs.Int("concurrency", 16, "loader concurrency")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res := bench.Load(context.Background(), *addr, bench.LoadOptions{
		Graph: *graph, Users: *users, Follows: *follows, KV: *kv, Concurrency: *conc,
	}, func(s string) { fmt.Println(s) })
	fmt.Printf("load complete in %s\n", res.Elapsed.Round(time.Millisecond))
	return nil
}

func queryCmd(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	dir := fs.String("queries", "bench/queries", "directory of .cypher query files")
	graph := fs.String("graph", "g", "graph id")
	conc := fs.Int("concurrency", 16, "concurrent clients per query")
	dur := fs.Duration("duration", 15*time.Second, "duration per query")
	if err := fs.Parse(args); err != nil {
		return err
	}
	queries, err := bench.LoadQueries(*dir)
	if err != nil {
		return err
	}
	if len(queries) == 0 {
		return fmt.Errorf("no .cypher files in %s", *dir)
	}
	fmt.Printf("# cypher benchmark: %d queries, concurrency=%d, duration=%s each\n", len(queries), *conc, *dur)
	for _, r := range bench.RunQueries(*addr, *graph, queries, *conc, *dur) {
		fmt.Println(r.Lat.Report(r.Name, *dur))
	}
	return nil
}

func kvCmd(args []string) error {
	fs := flag.NewFlagSet("kv", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	conc := fs.Int("concurrency", 32, "concurrent clients")
	dur := fs.Duration("duration", 15*time.Second, "duration")
	keys := fs.Int("keys", 10000, "key space size")
	readRatio := fs.Float64("read-ratio", 0.5, "fraction of ops that are reads")
	namespace := fs.String("namespace", "default", "namespace to target")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res := bench.RunKV(*addr, bench.KVOptions{Concurrency: *conc, Keys: *keys, ReadRatio: *readRatio, Duration: *dur, Namespace: *namespace})
	fmt.Printf("# KV benchmark: concurrency=%d, duration=%s, read-ratio=%.0f%%\n", *conc, *dur, *readRatio*100)
	fmt.Println(res.Get.Report("kv-get", *dur))
	fmt.Println(res.Put.Report("kv-put", *dur))
	return nil
}

func multigetCmd(args []string) error {
	fs := flag.NewFlagSet("multiget", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	conc := fs.Int("concurrency", 32, "concurrent clients")
	dur := fs.Duration("duration", 15*time.Second, "duration")
	keys := fs.Int("keys", 10000, "key space size")
	batch := fs.Int("batch", 100, "keys per MultiGet RPC")
	if err := fs.Parse(args); err != nil {
		return err
	}
	res := bench.RunMultiGet(*addr, bench.MultiGetOptions{Concurrency: *conc, Keys: *keys, Batch: *batch, Duration: *dur})
	keysPerSec := float64(res.KeysGot) / dur.Seconds()
	fmt.Printf("# MultiGet benchmark: concurrency=%d, duration=%s, batch=%d\n", *conc, *dur, *batch)
	fmt.Println(res.Lat.Report("multiget-rpc", *dur))
	fmt.Printf("%-28s keys=%-9d %12.0f keys/s\n", "effective-read-throughput", res.KeysGot, keysPerSec)
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: wavespan-bench <command>

commands:
  load   --addr host:port [--users N] [--follows N] [--kv N] [--graph g]   bulk-load KV + a social graph
  query  --addr host:port --queries DIR [--concurrency N] [--duration D] [--graph g]   replay Cypher queries, report latency
  kv     --addr host:port [--concurrency N] [--duration D] [--read-ratio R]   KV put/get load test
`)
}
