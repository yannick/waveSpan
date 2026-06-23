package bench

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// LoadOptions configures the bulk loader.
type LoadOptions struct {
	Graph              string
	Users, Follows, KV int
	Concurrency        int
}

// LoadResult reports how much was loaded and how long it took.
type LoadResult struct {
	Users, Follows, KV int
	Elapsed            time.Duration
}

// Load bulk-loads a social graph (User nodes + FOLLOWS edges) and KV keys for benchmarking.
func Load(ctx context.Context, addr string, opt LoadOptions, progress func(string)) LoadResult {
	cy := CypherClient(addr)
	kvc := KVClient(addr)
	cities := []string{"NYC", "SF", "LA", "SEA", "AUS"}
	if progress == nil {
		progress = func(string) {}
	}
	start := time.Now()
	res := LoadResult{}

	var nodes int64
	runPool(opt.Concurrency, opt.Users, func(i int) {
		if OpCreateNode(ctx, cy, opt.Graph, i, cities[i%len(cities)]) == nil {
			atomic.AddInt64(&nodes, 1)
		}
	})
	res.Users = int(nodes)
	progress(fmt.Sprintf("loaded %d/%d User nodes", res.Users, opt.Users))

	var edges int64
	runPool(opt.Concurrency, opt.Follows, func(i int) {
		a := i % opt.Users
		b := (i*7 + 1) % opt.Users
		if a == b {
			return
		}
		if OpCreateEdge(ctx, cy, opt.Graph, a, b) == nil {
			atomic.AddInt64(&edges, 1)
		}
	})
	res.Follows = int(edges)
	progress(fmt.Sprintf("loaded %d FOLLOWS edges", res.Follows))

	var keys int64
	runPool(opt.Concurrency, opt.KV, func(i int) {
		if OpKVWrite(ctx, kvc, "default", fmt.Sprintf("bench/%d", i), []byte(fmt.Sprintf("value-%d", i))) == nil {
			atomic.AddInt64(&keys, 1)
		}
	})
	res.KV = int(keys)
	progress(fmt.Sprintf("loaded %d/%d KV keys", res.KV, opt.KV))

	res.Elapsed = time.Since(start)
	return res
}

// runPool runs fn(0..n-1) across `workers` goroutines.
func runPool(workers, n int, fn func(int)) {
	if workers < 1 {
		workers = 1
	}
	jobs := make(chan int, workers*2)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				fn(i)
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}
