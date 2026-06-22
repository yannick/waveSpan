<!-- client-side workload result
kv-get                       ops=55072   errs=665      2754/s  p50=2.364ms   p95=26.572ms  p99=52.339ms 
kv-put                       ops=23787   errs=273      1189/s  p50=3.201ms   p95=29.334ms  p99=58.086ms 
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **19.74s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **91%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **5.79s** (29.3% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 5.79s | 29.3% | 0.1% |
| 2 | `syscall.RawSyscall6` | 4.74s | 24.0% | 0.0% |
| 3 | `syscall.Syscall` | 4.59s | 23.3% | 0.0% |
| 4 | `wavespanv1connect.NewKvServiceHandler.func1` | 3.86s | 19.6% | 0.1% |
| 5 | `syscall.Write` | 3.22s | 16.3% | 0.0% |
| 6 | `syscall.write` | 3.22s | 16.3% | 0.0% |
| 7 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 2.24s | 11.3% | 0.0% |
| 8 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 1.99s | 10.1% | 0.1% |
| 9 | `kv.(*Service).Put` | 1.97s | 10.0% | 0.1% |
| 10 | `kv.(*Coordinator).Put` | 1.93s | 9.8% | 0.0% |
| 11 | `kv.(*Coordinator).write` | 1.93s | 9.8% | 0.0% |
| 12 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 1.89s | 9.6% | 0.0% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **77.12s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **17.09s** (22.2% of the sampled block).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 17.09s | 22.2% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 16.37s | 21.2% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 16.35s | 21.2% | 0.0% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 16.35s | 21.2% | 0.0% |
| 5 | `kv.(*Coordinator).Put` | 16.35s | 21.2% | 0.0% |
| 6 | `kv.(*Coordinator).write` | 16.35s | 21.2% | 0.0% |
| 7 | `kv.(*Service).Put` | 16.35s | 21.2% | 0.0% |
| 8 | `wavespanv1connect.KvServiceHandler.Put-fm` | 16.35s | 21.2% | 0.0% |
| 9 | `kv.(*Coordinator).replicateMinAck` | 14.60s | 18.9% | 0.0% |
| 10 | `connect.(*Client[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45]).CallUnary` | 14.42s | 18.7% | 0.0% |
| 11 | `connect.NewClient[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45].func1` | 14.42s | 18.7% | 0.0% |
| 12 | `connect.NewClient[go.shape.5ed2f44a42fd145d96ac27f32b51f9e66f03fb0424f1f7e6860b4420c0b27ad0,go.shape.48ae3598c10154e38a1633a1b2bcb21e94147e913abee537480dca3008cfde45].func2` | 14.42s | 18.7% | 0.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **6.96s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **6.96s** (100.0% of the sampled mutex).
- contention in the **record store** write path — commits may share one lock.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 6.96s | 100.0% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 5.96s | 85.7% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 4.66s | 67.0% | 0.0% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 3.41s | 49.0% | 0.0% |
| 5 | `kv.(*Coordinator).Put` | 3.41s | 49.0% | 0.0% |
| 6 | `kv.(*Coordinator).write` | 3.41s | 49.0% | 0.0% |
| 7 | `kv.(*Service).Put` | 3.41s | 49.0% | 0.0% |
| 8 | `connect.(*connectUnaryHandlerConn).Receive` | 2.64s | 37.9% | 0.0% |
| 9 | `connect.(*connectUnaryUnmarshaler).Unmarshal` | 2.64s | 37.9% | 0.0% |
| 10 | `connect.(*connectUnaryUnmarshaler).UnmarshalFunc` | 2.64s | 37.9% | 0.0% |
| 11 | `connect.(*errorTranslatingHandlerConnCloser).Receive` | 2.64s | 37.9% | 0.0% |
| 12 | `recordstore.(*Store).Apply` | 2.51s | 36.1% | 0.0% |

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **1.61GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **52%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **1019.8MB** (61.7% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 1019.8MB | 61.7% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 707.1MB | 42.8% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 407.5MB | 24.7% | 0.2% |
| 4 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 372.5MB | 22.5% | 0.0% |
| 5 | `kv.(*Service).Put` | 372.5MB | 22.5% | 0.2% |
| 6 | `kv.(*Coordinator).Put` | 359.0MB | 21.7% | 0.0% |
| 7 | `kv.(*Coordinator).write` | 359.0MB | 21.7% | 0.0% |
| 8 | `wavespanv1connect.NewReplicationServiceHandler.func1` | 254.5MB | 15.4% | 0.0% |
| 9 | `recordstore.(*Store).Apply` | 233.5MB | 14.1% | 1.1% |
| 10 | `connect.(*connectUnaryMarshaler).Marshal` | 214.1MB | 12.9% | 0.0% |
| 11 | `membership.(*Roster).Members` | 184.6MB | 11.2% | 7.6% |
| 12 | `membership.(*Service).Members` | 184.6MB | 11.2% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **252** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `wavesdb.(*DB).flushWorker` at **12** (4.8% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `wavesdb.(*DB).flushWorker` | 12 | 4.8% | 0.0% |
| 2 | `wavesdb.(*DB).compactionWorker` | 6 | 2.4% | 0.0% |
| 3 | `cache.(*Evictor).Run` | 3 | 1.2% | 0.0% |
| 4 | `membership.(*Service).Run` | 3 | 1.2% | 0.0% |
| 5 | `local.(*Fanout).Run` | 3 | 1.2% | 0.0% |
| 6 | `local.(*IntraAntiEntropy).Run` | 3 | 1.2% | 0.0% |
| 7 | `local.(*RepairEngine).Run` | 3 | 1.2% | 0.0% |
| 8 | `ttl.(*Sweeper).Run` | 3 | 1.2% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
