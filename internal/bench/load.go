package bench

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
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
	cy := cypherClient(addr)
	kvc := kvClient(addr)
	cities := []string{"NYC", "SF", "LA", "SEA", "AUS"}
	if progress == nil {
		progress = func(string) {}
	}
	start := time.Now()
	res := LoadResult{}

	var nodes int64
	runPool(opt.Concurrency, opt.Users, func(i int) {
		q := fmt.Sprintf("CREATE (:User {id:'user-%d', name:'User %d', age:%d, city:'%s'})", i, i, 18+i%60, cities[i%len(cities)])
		if execCypher(ctx, cy, opt.Graph, q) {
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
		q := fmt.Sprintf("MATCH (a:User {id:'user-%d'}), (b:User {id:'user-%d'}) CREATE (a)-[:FOLLOWS]->(b)", a, b)
		if execCypher(ctx, cy, opt.Graph, q) {
			atomic.AddInt64(&edges, 1)
		}
	})
	res.Follows = int(edges)
	progress(fmt.Sprintf("loaded %d FOLLOWS edges", res.Follows))

	var keys int64
	runPool(opt.Concurrency, opt.KV, func(i int) {
		_, err := kvc.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
			Namespace: "default", Key: []byte(fmt.Sprintf("bench/%d", i)), Value: []byte(fmt.Sprintf("value-%d", i)), RequireOriginPlusOne: true,
		}))
		if err == nil {
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

// execCypher runs a write query, draining the stream, returning success.
func execCypher(ctx context.Context, cy wavespanv1connect.CypherClient, graph, q string) bool {
	stream, err := cy.Query(ctx, connect.NewRequest(&wavespanv1.CypherRequest{GraphId: graph, Query: q}))
	if err != nil {
		return false
	}
	for stream.Receive() { //nolint:revive // drain
	}
	return stream.Err() == nil
}
