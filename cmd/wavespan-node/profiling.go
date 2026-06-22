package main

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
)

// enableProfiling wires Go's runtime profilers onto the admin mux when WAVESPAN_PROFILING_ENABLED is
// set, so wavespan-profile can capture CPU/heap/block/mutex/goroutine profiles from every node during
// a benchmark run. Block and mutex profiling are OFF by default (they add runtime overhead); set
// WAVESPAN_BLOCK_PROFILE_RATE (nanoseconds; 10000 = sample ~1 blocking event per 10µs of delay) and
// WAVESPAN_MUTEX_PROFILE_FRACTION (1-in-N contended locks; 100 is a good start) to enable them.
//
// Latency lives in OFF-CPU waits — lock contention, fsync, network — so the block and mutex profiles
// are what reveal where requests stall; the CPU profile alone only shows on-CPU work.
func enableProfiling(mux *http.ServeMux, logger *slog.Logger) {
	if !envBoolDefault("WAVESPAN_PROFILING_ENABLED", false) {
		return
	}
	if rate := envIntDefault("WAVESPAN_BLOCK_PROFILE_RATE", 0); rate > 0 {
		runtime.SetBlockProfileRate(rate)
	}
	if frac := envIntDefault("WAVESPAN_MUTEX_PROFILE_FRACTION", 0); frac > 0 {
		runtime.SetMutexProfileFraction(frac)
	}
	mux.HandleFunc("/debug/pprof/", pprof.Index) // serves heap, goroutine, block, mutex, allocs, ...
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	logger.Info("profiling enabled on admin port",
		"block_rate", envIntDefault("WAVESPAN_BLOCK_PROFILE_RATE", 0),
		"mutex_fraction", envIntDefault("WAVESPAN_MUTEX_PROFILE_FRACTION", 0))
}

func envBoolDefault(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func envIntDefault(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
