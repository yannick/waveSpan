package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// loadCmd bulk-loads a social graph (User nodes + FOLLOWS edges) and KV keys for benchmarking.
func loadCmd(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	graph := fs.String("graph", "g", "graph id")
	users := fs.Int("users", 2000, "User nodes to create")
	follows := fs.Int("follows", 6000, "FOLLOWS edges to create")
	kv := fs.Int("kv", 5000, "KV keys to write")
	conc := fs.Int("concurrency", 16, "loader concurrency")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	cy := cypherClient(*addr)
	kvc := kvClient(*addr)
	cities := []string{"NYC", "SF", "LA", "SEA", "AUS"}

	start := time.Now()
	var created int64

	// nodes
	runPool(*conc, *users, func(i int) {
		q := fmt.Sprintf("CREATE (:User {id:'user-%d', name:'User %d', age:%d, city:'%s'})", i, i, 18+i%60, cities[i%len(cities)])
		if execCypher(ctx, cy, *graph, q) {
			atomic.AddInt64(&created, 1)
		}
	})
	fmt.Printf("loaded %d/%d User nodes\n", created, *users)

	// edges: user-i FOLLOWS user-(i*7+1 mod users)
	created = 0
	runPool(*conc, *follows, func(i int) {
		a := i % *users
		b := (i*7 + 1) % *users
		if a == b {
			return
		}
		q := fmt.Sprintf("MATCH (a:User {id:'user-%d'}), (b:User {id:'user-%d'}) CREATE (a)-[:FOLLOWS]->(b)", a, b)
		if execCypher(ctx, cy, *graph, q) {
			atomic.AddInt64(&created, 1)
		}
	})
	fmt.Printf("loaded %d FOLLOWS edges\n", created)

	// KV
	created = 0
	runPool(*conc, *kv, func(i int) {
		_, err := kvc.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
			Namespace: "default", Key: []byte(fmt.Sprintf("bench/%d", i)), Value: []byte(fmt.Sprintf("value-%d", i)), RequireOriginPlusOne: true,
		}))
		if err == nil {
			atomic.AddInt64(&created, 1)
		}
	})
	fmt.Printf("loaded %d/%d KV keys\n", created, *kv)
	fmt.Printf("load complete in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// runPool runs fn(0..n-1) across `workers` goroutines.
func runPool(workers, n int, fn func(int)) {
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
