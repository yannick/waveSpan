package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// kvCmd runs a mixed KV put/get load test, reporting throughput + latency for each.
func kvCmd(args []string) error {
	fs := flag.NewFlagSet("kv", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	conc := fs.Int("concurrency", 32, "concurrent clients")
	dur := fs.Duration("duration", 15*time.Second, "duration")
	keyspace := fs.Int("keys", 10000, "key space size")
	readRatio := fs.Float64("read-ratio", 0.5, "fraction of ops that are reads")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := kvClient(*addr)
	puts, gets := &latencies{}, &latencies{}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for ctx.Err() == nil {
				key := fmt.Sprintf("bench/%d", rng.Intn(*keyspace))
				start := time.Now()
				if rng.Float64() < *readRatio {
					_, err := client.Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: "default", Key: []byte(key)}))
					switch {
					case err == nil:
						gets.add(time.Since(start))
					case ctx.Err() == nil:
						gets.addErr()
					}
				} else {
					_, err := client.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
						Namespace: "default", Key: []byte(key), Value: []byte("v"), RequireOriginPlusOne: true,
					}))
					switch {
					case err == nil:
						puts.add(time.Since(start))
					case ctx.Err() == nil:
						puts.addErr()
					}
				}
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("# KV benchmark: concurrency=%d, duration=%s, read-ratio=%.0f%%\n", *conc, *dur, *readRatio*100)
	fmt.Println(gets.report("kv-get", *dur))
	fmt.Println(puts.report("kv-put", *dur))
	return nil
}
