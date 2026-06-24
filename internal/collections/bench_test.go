package collections

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yannick/wavespan/internal/storage"
)

// benchShard stands up a single-node shard for benchmarks. There is no network, so these measure the
// local Raft commit path (propose → apply → reply), not replication latency.
func benchShard(b *testing.B) *Collections {
	b.Helper()
	mem := storage.NewMemStore()
	b.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(b)
	m := newMgr(b, b.TempDir(), addr, mem)
	if err := m.StartShard(1, 1, map[uint64]string{1: addr}, false); err != nil {
		b.Fatalf("StartShard: %v", err)
	}
	b.Cleanup(m.Stop)
	c := New(m, SingleShardDirectory(1))
	waitReady(b, c)
	return c
}

// BenchmarkHIncrBy measures an atomic integer counter increment (one committed Raft entry).
func BenchmarkHIncrBy(b *testing.B) {
	c := benchShard(b)
	ns, coll, field := []byte("bench"), []byte("ctr"), []byte("n")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.HIncrBy(ctx, ns, coll, field, 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHIncrByFloat measures an atomic float counter increment.
func BenchmarkHIncrByFloat(b *testing.B) {
	c := benchShard(b)
	ns, coll, field := []byte("bench"), []byte("ctr"), []byte("r")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.HIncrByFloat(ctx, ns, coll, field, 1.5); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSAdd is a single-write baseline to compare the counter and bulk paths against.
func BenchmarkSAdd(b *testing.B) {
	c := benchShard(b)
	ns, coll := []byte("bench"), []byte("s")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.SAdd(ctx, ns, coll, []byte(strconv.Itoa(i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentSAdd is the lever-visible write benchmark (design/32 §7): N goroutines each issue
// SAdds concurrently, so the batching proposer (QW2) can coalesce many in-flight proposals into few
// Raft entries — a throughput the single-writer BenchmarkSAdd structurally cannot show (it measures
// serial commit latency, batch depth 1). Run with -cpu to vary writer concurrency, e.g.:
//
//	go test -run x -bench ConcurrentSAdd -benchmem -cpu 1,8,64 ./internal/collections
//
// b.N total ops are spread across b.RunParallel's goroutines; ns/op is per-op wall time under load.
func BenchmarkConcurrentSAdd(b *testing.B) {
	c := benchShard(b)
	ns, coll := []byte("bench"), []byte("conc")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var ctr uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			if _, err := c.SAdd(ctx, ns, coll, []byte(strconv.FormatUint(i, 10))); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkConcurrentSAddSharded combines QW2 (batching) with D1 (N hash-routed data shards): writers
// spread across shards by key, so multiple Raft groups apply in parallel AND each group still batches.
func BenchmarkConcurrentSAddSharded(b *testing.B) {
	const shards = 4
	mem := storage.NewMemStore()
	b.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(b)
	m := newMgr(b, b.TempDir(), addr, mem)
	for i := uint64(0); i < shards; i++ {
		if err := m.StartShard(firstDataShard+i, 1, map[uint64]string{1: addr}, false); err != nil {
			b.Fatalf("StartShard: %v", err)
		}
	}
	b.Cleanup(m.Stop)
	c := New(m, NewHashDirectory(shards))
	// Warm every shard's leader.
	wctx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
	var wg sync.WaitGroup
	for i := uint64(0); i < shards; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			for {
				if _, err := c.SAdd(wctx, []byte("warm"), []byte(fmt.Sprintf("s%d", i)), []byte("x")); err == nil {
					return
				}
				if wctx.Err() != nil {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()
	wcancel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var ctr uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&ctr, 1)
			key := strconv.FormatUint(i, 10)
			if _, err := c.SAdd(ctx, []byte("bench"), []byte(key), []byte(key)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkBulkRemove measures removing one member from a named list of 10 collections (the fan-out is
// 10 proposes; each iteration re-seeds outside the timer).
func BenchmarkBulkRemove(b *testing.B) {
	c := benchShard(b)
	ns := []byte("bench")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const k = 10
	colls := make([][]byte, k)
	for i := range colls {
		colls[i] = []byte("c" + strconv.Itoa(i))
	}
	member := [][]byte{[]byte("m")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, coll := range colls {
			if _, err := c.SAdd(ctx, ns, coll, member[0]); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		res, err := c.BulkRemove(ctx, ns, colls, member)
		if err != nil {
			b.Fatal(err)
		}
		for _, e := range res {
			if e.Err != nil {
				b.Fatal(e.Err)
			}
		}
	}
}
