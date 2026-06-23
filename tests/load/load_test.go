//go:build load

// Package load drives a sustained KV workload against the local compose cluster and reports
// throughput + latency percentiles (M12 deliverable). Run with:
//
//	make docker-up
//	go test -tags load ./tests/load -run Load -v
package load

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

func kvClient(port string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(http.DefaultClient, "http://localhost:"+port)
}

func TestLoadKVThroughputLatency(t *testing.T) {
	const (
		port              = "7811"
		concurrency       = 16
		duration          = 10 * time.Second
		latencyP99Ceiling = 250 * time.Millisecond
	)
	client := kvClient(port)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var ops int64
	var errs int64
	var mu sync.Mutex
	var lats []time.Duration

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for ctx.Err() == nil {
				key := fmt.Sprintf("load/%d/%d", id, i)
				start := time.Now()
				_, err := client.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
					Namespace: "default", Key: []byte(key), Value: []byte("v"), RequireOriginPlusOne: true,
				}))
				lat := time.Since(start)
				if err != nil {
					atomic.AddInt64(&errs, 1)
				} else {
					atomic.AddInt64(&ops, 1)
					mu.Lock()
					lats = append(lats, lat)
					mu.Unlock()
				}
				i++
			}
		}(w)
	}
	wg.Wait()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p := func(q float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[int(float64(len(lats))*q)%len(lats)]
	}
	tput := float64(ops) / duration.Seconds()
	t.Logf("KV load: ops=%d errs=%d throughput=%.0f/s p50=%s p95=%s p99=%s", ops, errs, tput, p(0.50), p(0.95), p(0.99))
	if errs > 0 {
		t.Fatalf("load test saw %d errors", errs)
	}
	if p(0.99) > latencyP99Ceiling {
		t.Fatalf("p99 latency %s exceeds ceiling %s", p(0.99), latencyP99Ceiling)
	}
}
