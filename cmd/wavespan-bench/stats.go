package main

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func kvClient(addr string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(http.DefaultClient, "http://"+addr)
}

func cypherClient(addr string) wavespanv1connect.CypherClient {
	return wavespanv1connect.NewCypherClient(http.DefaultClient, "http://"+addr)
}

// latencies accumulates op latencies for percentile reporting.
type latencies struct {
	mu   sync.Mutex
	lats []time.Duration
	errs int
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.lats = append(l.lats, d)
	l.mu.Unlock()
}

func (l *latencies) addErr() {
	l.mu.Lock()
	l.errs++
	l.mu.Unlock()
}

func (l *latencies) percentile(q float64) time.Duration {
	if len(l.lats) == 0 {
		return 0
	}
	i := int(float64(len(l.lats)) * q)
	if i >= len(l.lats) {
		i = len(l.lats) - 1
	}
	return l.lats[i]
}

// report sorts and renders one line of throughput + percentiles.
func (l *latencies) report(label string, wall time.Duration) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	sort.Slice(l.lats, func(i, j int) bool { return l.lats[i] < l.lats[j] })
	tput := float64(len(l.lats)) / wall.Seconds()
	return fmt.Sprintf("%-28s ops=%-7d errs=%-4d %8.0f/s  p50=%-9s p95=%-9s p99=%-9s",
		label, len(l.lats), l.errs, tput,
		l.percentile(0.50).Round(time.Microsecond), l.percentile(0.95).Round(time.Microsecond), l.percentile(0.99).Round(time.Microsecond))
}
