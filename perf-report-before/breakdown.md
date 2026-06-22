<!-- client-side workload result
kv-get                       ops=53654   errs=62       2683/s  p50=3.229ms   p95=30.138ms  p99=52.484ms 
kv-put                       ops=23210   errs=31       1160/s  p50=4.576ms   p95=33.324ms  p99=55.545ms 
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **34.43s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **92%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **16.92s** (49.1% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 16.92s | 49.1% | 0.1% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 11.56s | 33.6% | 0.0% |
| 3 | `connect.(*connectUnaryMarshaler).Marshal` | 10.33s | 30.0% | 0.0% |
| 4 | `connect.(*errorTranslatingHandlerConnCloser).Send` | 9.08s | 26.4% | 0.0% |
| 5 | `connect.(*connectUnaryHandlerConn).Send` | 9.06s | 26.3% | 0.0% |
| 6 | `connect.(*compressionPool).Compress` | 8.53s | 24.8% | 0.0% |
| 7 | `syscall.RawSyscall6` | 6.57s | 19.1% | 0.0% |
| 8 | `syscall.Syscall` | 6.36s | 18.5% | 0.0% |
| 9 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 5.69s | 16.5% | 0.1% |
| 10 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 5.30s | 15.4% | 0.0% |
| 11 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 5.06s | 14.7% | 0.0% |
| 12 | `connect.(*compressionPool).putCompressor` | 4.78s | 13.9% | 0.0% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **87.37s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **27.35s** (31.3% of the sampled block).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 27.35s | 31.3% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 25.90s | 29.6% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 25.84s | 29.6% | 0.0% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 25.84s | 29.6% | 0.0% |
| 5 | `kv.(*Coordinator).Put` | 25.84s | 29.6% | 0.0% |
| 6 | `kv.(*Coordinator).write` | 25.84s | 29.6% | 0.0% |
| 7 | `kv.(*Service).Put` | 25.84s | 29.6% | 0.0% |
| 8 | `wavespanv1connect.KvServiceHandler.Put-fm` | 25.84s | 29.6% | 0.0% |
| 9 | `kv.(*Coordinator).replicateMinAck` | 24.12s | 27.6% | 0.0% |
| 10 | `local.(*ConnectReplicator).StoreReplica` | 23.92s | 27.4% | 0.0% |
| 11 | `connect.(*Client[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45]).CallUnary` | 23.92s | 27.4% | 0.0% |
| 12 | `connect.NewClient[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45].func1` | 23.92s | 27.4% | 0.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **13.81s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **13.81s** (100.0% of the sampled mutex).
- contention in the **record store** write path — commits may share one lock.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 13.81s | 100.0% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 10.12s | 73.3% | 0.0% |
| 3 | `connect.(*connectUnaryUnmarshaler).Unmarshal` | 6.16s | 44.6% | 0.0% |
| 4 | `connect.(*connectUnaryUnmarshaler).UnmarshalFunc` | 6.16s | 44.6% | 0.0% |
| 5 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 6.01s | 43.5% | 0.0% |
| 6 | `connect.(*connectUnaryHandlerConn).Receive` | 5.65s | 40.9% | 0.0% |
| 7 | `connect.(*errorTranslatingHandlerConnCloser).Receive` | 5.65s | 40.9% | 0.0% |
| 8 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 5.00s | 36.2% | 0.0% |
| 9 | `kv.(*Coordinator).Put` | 5.00s | 36.2% | 0.0% |
| 10 | `kv.(*Coordinator).write` | 5.00s | 36.2% | 0.0% |
| 11 | `kv.(*Service).Put` | 5.00s | 36.2% | 0.0% |
| 12 | `recordstore.(*Store).Apply` | 3.79s | 27.4% | 0.0% |

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **2.47GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **25%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **2.16GB** (87.5% of the sampled alloc).
- **Hottest leaf: `wavesdb.(*Txn).dup` at 1.54GB flat (62.4%)** — the actual allocation happens here, not just in callers above it.
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 2.16GB | 87.5% | 0.0% |
| 2 | `recordstore.(*Store).Apply` | 1.60GB | 65.1% | 0.5% |
| 3 | `storage.(*WavesdbStore).Batch` | 1.57GB | 63.8% | 0.0% |
| 4 | `wavesdb.(*Txn).Put` | 1.55GB | 62.9% | 0.0% |
| 5 | `wavesdb.(*Txn).dup` | 1.54GB | 62.4% | 62.4% |
| 6 | `wavespanv1connect.NewKvServiceHandler.func1` | 1.27GB | 51.6% | 0.0% |
| 7 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 1.13GB | 45.9% | 0.1% |
| 8 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 1.07GB | 43.4% | 0.0% |
| 9 | `kv.(*Service).Put` | 1.07GB | 43.4% | 0.1% |
| 10 | `kv.(*Coordinator).Put` | 1.07GB | 43.3% | 0.0% |
| 11 | `kv.(*Coordinator).write` | 1.07GB | 43.3% | 0.0% |
| 12 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 872.0MB | 34.5% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **205** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **99%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `wavesdb.(*DB).flushWorker` at **12** (5.9% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `wavesdb.(*DB).flushWorker` | 12 | 5.9% | 0.0% |
| 2 | `wavesdb.(*DB).compactionWorker` | 6 | 2.9% | 0.0% |
| 3 | `cache.(*Evictor).Run` | 3 | 1.5% | 0.0% |
| 4 | `membership.(*Service).Run` | 3 | 1.5% | 0.0% |
| 5 | `local.(*Fanout).Run` | 3 | 1.5% | 0.0% |
| 6 | `local.(*IntraAntiEntropy).Run` | 3 | 1.5% | 0.0% |
| 7 | `local.(*RepairEngine).Run` | 3 | 1.5% | 0.0% |
| 8 | `ttl.(*Sweeper).Run` | 3 | 1.5% | 0.0% |
| 9 | `syscall.Read` | 2 | 1.0% | 0.0% |
| 10 | `syscall.Syscall` | 2 | 1.0% | 1.0% |
| 11 | `syscall.read` | 2 | 1.0% | 0.5% |
| 12 | `syscall.SetNonblock` | 1 | 0.5% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
