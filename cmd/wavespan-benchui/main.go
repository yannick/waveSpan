// Command wavespan-benchui is the standalone benchmarking web UI server. It drives WaveSpan
// benchmarks against a cluster and serves the dashboard (embedded SPA). Binds 127.0.0.1 by default;
// pass --addr 0.0.0.0:PORT to expose it.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/yannick/wavespan/internal/benchui"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8088", "HTTP listen address")
	dataAddr := flag.String("data-addr", os.Getenv("WAVESPAN_BENCH_DATA_ADDR"), "default cluster data address pre-filled in the UI (env WAVESPAN_BENCH_DATA_ADDR)")
	adminAddr := flag.String("admin-addr", os.Getenv("WAVESPAN_BENCH_ADMIN_ADDR"), "default node admin address pre-filled in the UI (env WAVESPAN_BENCH_ADMIN_ADDR)")
	flag.Parse()
	srv := benchui.New(benchui.Options{Addr: *addr, DefaultDataAddr: *dataAddr, DefaultAdminAddr: *adminAddr})
	log.Printf("wavespan-benchui listening on http://%s", *addr)
	if err := http.ListenAndServe(*addr, srv.Handler()); err != nil { //nolint:gosec // local benchmarking tool
		log.Fatalf("wavespan-benchui: %v", err)
	}
}
