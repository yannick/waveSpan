// Package bench holds the WaveSpan load + benchmark workloads (bulk load, Cypher query replay, KV
// load) as a reusable library so both the wavespan-bench CLI and wavespan-profile drive identical
// traffic — the profiler runs these in-process while capturing pprof from every node.
package bench

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func kvClient(addr string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(rpcopts.H2CClient(), "http://"+addr)
}

func cypherClient(addr string) wavespanv1connect.CypherClient {
	return wavespanv1connect.NewCypherClient(rpcopts.H2CClient(), "http://"+addr)
}

// Latencies accumulates op latencies for percentile reporting.
type Latencies struct {
	mu   sync.Mutex
	lats []time.Duration
	errs int
}

// Add records one successful op latency.
func (l *Latencies) Add(d time.Duration) {
	l.mu.Lock()
	l.lats = append(l.lats, d)
	l.mu.Unlock()
}

// AddErr records one failed op.
func (l *Latencies) AddErr() {
	l.mu.Lock()
	l.errs++
	l.mu.Unlock()
}

// Count returns the number of successful ops.
func (l *Latencies) Count() int { l.mu.Lock(); defer l.mu.Unlock(); return len(l.lats) }

func (l *Latencies) percentile(q float64) time.Duration {
	if len(l.lats) == 0 {
		return 0
	}
	i := int(float64(len(l.lats)) * q)
	if i >= len(l.lats) {
		i = len(l.lats) - 1
	}
	return l.lats[i]
}

// Report sorts and renders one line of throughput + percentiles.
func (l *Latencies) Report(label string, wall time.Duration) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	sort.Slice(l.lats, func(i, j int) bool { return l.lats[i] < l.lats[j] })
	tput := float64(len(l.lats)) / wall.Seconds()
	return fmt.Sprintf("%-28s ops=%-7d errs=%-4d %8.0f/s  p50=%-9s p95=%-9s p99=%-9s",
		label, len(l.lats), l.errs, tput,
		l.percentile(0.50).Round(time.Microsecond), l.percentile(0.95).Round(time.Microsecond), l.percentile(0.99).Round(time.Microsecond))
}
