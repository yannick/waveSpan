package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// MultiGetOptions configures the batched-read load test.
type MultiGetOptions struct {
	Concurrency int
	Keys        int // key space
	Batch       int // keys per MultiGet RPC
	Duration    time.Duration
}

// MultiGetResult reports per-RPC latency and the effective key throughput (ops × batch).
type MultiGetResult struct {
	Lat     *Latencies
	KeysGot int64
}

// RunMultiGet hammers MultiGet, each RPC fetching Batch random keys; the headline number is keys/s.
func RunMultiGet(addr string, opt MultiGetOptions) *MultiGetResult {
	client := KVClient(addr)
	res := &MultiGetResult{Lat: &Latencies{}}
	var keys atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), opt.Duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < opt.Concurrency; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			batch := make([][]byte, opt.Batch)
			for ctx.Err() == nil {
				for i := range batch {
					batch[i] = []byte(fmt.Sprintf("bench/%d", rng.Intn(opt.Keys)))
				}
				start := time.Now()
				err := OpMultiGet(ctx, client, "default", batch)
				switch {
				case err == nil:
					res.Lat.Add(time.Since(start))
					keys.Add(int64(opt.Batch))
				case ctx.Err() == nil:
					res.Lat.AddErr()
				}
			}
		}(w)
	}
	wg.Wait()
	res.KeysGot = keys.Load()
	return res
}
