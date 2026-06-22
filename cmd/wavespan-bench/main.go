// Command wavespan-bench is a load + benchmark client for WaveSpan. It bulk-loads test data (KV +
// a social graph) and replays a folder of Cypher queries under concurrency, reporting throughput
// and p50/p95/p99 latency per query. It talks to a data pod over Connect (default :7800).
//
//	wavespan-bench load  --addr localhost:7811 --users 2000 --follows 6000 --kv 5000
//	wavespan-bench query --addr localhost:7811 --queries bench/queries --concurrency 16 --duration 30s
//	wavespan-bench kv    --addr localhost:7811 --concurrency 32 --duration 20s
package main

import (
	"fmt"
	"os"
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

func usage() {
	fmt.Fprint(os.Stderr, `usage: wavespan-bench <command>

commands:
  load   --addr host:port [--users N] [--follows N] [--kv N] [--graph g]   bulk-load KV + a social graph
  query  --addr host:port --queries DIR [--concurrency N] [--duration D] [--graph g]   replay Cypher queries, report latency
  kv     --addr host:port [--concurrency N] [--duration D]   KV put/get load test
`)
}
