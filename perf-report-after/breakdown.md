<!-- client-side workload result
kv-get                       ops=53554   errs=17       2678/s  p50=2.841ms   p95=26.874ms  p99=48.611ms 
kv-put                       ops=23133   errs=11       1157/s  p50=4.039ms   p95=30.004ms  p99=51.729ms 
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **30.56s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **94%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **15.67s** (51.3% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 15.67s | 51.3% | 0.1% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 11.21s | 36.7% | 0.1% |
| 3 | `connect.(*connectUnaryMarshaler).Marshal` | 10.29s | 33.7% | 0.1% |
| 4 | `connect.(*connectUnaryHandlerConn).Send` | 9.33s | 30.5% | 0.0% |
| 5 | `connect.(*errorTranslatingHandlerConnCloser).Send` | 9.33s | 30.5% | 0.0% |
| 6 | `connect.(*compressionPool).Compress` | 8.81s | 28.8% | 0.0% |
| 7 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 5.56s | 18.2% | 0.0% |
| 8 | `connect.(*compressionPool).putCompressor` | 5.03s | 16.5% | 0.0% |
| 9 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 5.01s | 16.4% | 0.1% |
| 10 | `syscall.RawSyscall6` | 4.99s | 16.3% | 0.0% |
| 11 | `syscall.Syscall` | 4.85s | 15.9% | 0.0% |
| 12 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 4.44s | 14.5% | 0.0% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **87.00s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **26.99s** (31.0% of the sampled block).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 26.99s | 31.0% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 25.64s | 29.5% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 25.62s | 29.4% | 0.0% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 25.62s | 29.4% | 0.0% |
| 5 | `kv.(*Coordinator).Put` | 25.62s | 29.4% | 0.0% |
| 6 | `kv.(*Coordinator).write` | 25.62s | 29.4% | 0.0% |
| 7 | `kv.(*Service).Put` | 25.62s | 29.4% | 0.0% |
| 8 | `wavespanv1connect.KvServiceHandler.Put-fm` | 25.62s | 29.4% | 0.0% |
| 9 | `kv.(*Coordinator).replicateMinAck` | 22.82s | 26.2% | 0.0% |
| 10 | `local.(*ConnectReplicator).StoreReplica` | 22.61s | 26.0% | 0.0% |
| 11 | `connect.(*Client[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45]).CallUnary` | 22.61s | 26.0% | 0.0% |
| 12 | `connect.NewClient[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45].func1` | 22.61s | 26.0% | 0.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **8.28s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **8.28s** (100.0% of the sampled mutex).
- contention in the **record store** write path — commits may share one lock.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 8.28s | 100.0% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 6.38s | 77.2% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 5.43s | 65.6% | 0.0% |
| 4 | `recordstore.(*Store).Apply` | 5.38s | 65.1% | 0.0% |
| 5 | `storage.(*WavesdbStore).Batch` | 5.38s | 65.1% | 0.0% |
| 6 | `wavesdb.(*Txn).Commit` | 5.38s | 65.1% | 0.0% |
| 7 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 5.16s | 62.4% | 0.0% |
| 8 | `kv.(*Coordinator).Put` | 5.16s | 62.4% | 0.0% |
| 9 | `kv.(*Coordinator).write` | 5.16s | 62.4% | 0.0% |
| 10 | `kv.(*Service).Put` | 5.16s | 62.4% | 0.0% |
| 11 | `wavespanv1connect.KvServiceHandler.Put-fm` | 3.90s | 47.1% | 0.0% |
| 12 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 1.89s | 22.8% | 0.0% |

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **1.90GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **67%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **1.34GB** (70.4% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 1.34GB | 70.4% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 899.6MB | 46.1% | 0.0% |
| 3 | `connect.(*connectUnaryMarshaler).Marshal` | 619.1MB | 31.7% | 0.0% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 528.8MB | 27.1% | 0.1% |
| 5 | `connect.(*connectUnaryHandlerConn).Send` | 511.6MB | 26.2% | 0.0% |
| 6 | `connect.(*errorTranslatingHandlerConnCloser).Send` | 511.6MB | 26.2% | 0.0% |
| 7 | `connect.(*compressionPool).Compress` | 458.6MB | 23.5% | 0.0% |
| 8 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 431.2MB | 22.1% | 0.0% |
| 9 | `kv.(*Service).Put` | 431.2MB | 22.1% | 0.1% |
| 10 | `kv.(*Coordinator).Put` | 415.7MB | 21.3% | 0.0% |
| 11 | `kv.(*Coordinator).write` | 415.7MB | 21.3% | 0.0% |
| 12 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 414.6MB | 21.3% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **216** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `wavesdb.(*DB).flushWorker` at **12** (5.6% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `wavesdb.(*DB).flushWorker` | 12 | 5.6% | 0.0% |
| 2 | `wavesdb.(*DB).compactionWorker` | 6 | 2.8% | 0.0% |
| 3 | `cache.(*Evictor).Run` | 3 | 1.4% | 0.0% |
| 4 | `membership.(*Service).Run` | 3 | 1.4% | 0.0% |
| 5 | `local.(*Fanout).Run` | 3 | 1.4% | 0.0% |
| 6 | `local.(*IntraAntiEntropy).Run` | 3 | 1.4% | 0.0% |
| 7 | `local.(*RepairEngine).Run` | 3 | 1.4% | 0.0% |
| 8 | `ttl.(*Sweeper).Run` | 3 | 1.4% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
