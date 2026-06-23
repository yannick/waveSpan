package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// KVOptions configures the KV load test.
type KVOptions struct {
	Namespace   string
	Concurrency int
	Keys        int
	ReadRatio   float64
	Duration    time.Duration
}

// KVResult holds the separate get/put latencies.
type KVResult struct {
	Get *Latencies
	Put *Latencies
}

// RunKV runs a mixed put/get load test.
func RunKV(addr string, opt KVOptions) *KVResult {
	ns := opt.Namespace
	if ns == "" {
		ns = "default"
	}
	client := kvClient(addr)
	res := &KVResult{Get: &Latencies{}, Put: &Latencies{}}
	ctx, cancel := context.WithTimeout(context.Background(), opt.Duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < opt.Concurrency; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for ctx.Err() == nil {
				key := fmt.Sprintf("bench/%d", rng.Intn(opt.Keys))
				start := time.Now()
				if rng.Float64() < opt.ReadRatio {
					_, err := client.Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)}))
					switch {
					case err == nil:
						res.Get.Add(time.Since(start))
					case ctx.Err() == nil:
						res.Get.AddErr()
					}
				} else {
					_, err := client.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
						Namespace: ns, Key: []byte(key), Value: []byte("v"), RequireOriginPlusOne: true,
					}))
					switch {
					case err == nil:
						res.Put.Add(time.Since(start))
					case ctx.Err() == nil:
						res.Put.AddErr()
					}
				}
			}
		}(w)
	}
	wg.Wait()
	return res
}
