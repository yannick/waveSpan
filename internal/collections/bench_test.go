package collections

import (
	"context"
	"strconv"
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
