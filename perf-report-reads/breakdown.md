<!-- client-side workload result
kv-get                       ops=474289  errs=0       23714/s  p50=5.035ms   p95=8.997ms   p99=11.828ms 
kv-put                       ops=0       errs=0           0/s  p50=0s        p95=0s        p99=0s       
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=128 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **32.40s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **94%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **10.39s** (32.1% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 10.39s | 32.1% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 10.06s | 31.0% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 8.40s | 25.9% | 0.1% |
| 4 | `syscall.Syscall` | 5.04s | 15.6% | 0.0% |
| 5 | `syscall.RawSyscall6` | 4.93s | 15.2% | 0.0% |
| 6 | `connect.receiveUnaryRequest[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 4.08s | 12.6% | 0.2% |
| 7 | `connect.(*errorTranslatingHandlerConnCloser).Receive` | 4.02s | 12.4% | 0.0% |
| 8 | `connect.receiveUnaryMessage[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 3.99s | 12.3% | 0.1% |
| 9 | `connect.(*connectUnaryUnmarshaler).Unmarshal` | 3.88s | 12.0% | 0.0% |
| 10 | `connect.(*connectUnaryUnmarshaler).UnmarshalFunc` | 3.88s | 12.0% | 0.1% |
| 11 | `connect.(*connectUnaryHandlerConn).Receive` | 3.82s | 11.8% | 0.0% |
| 12 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func1` | 3.32s | 10.2% | 0.1% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **440.78s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **87.44s** (19.8% of the sampled block).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 87.44s | 19.8% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 87.22s | 19.8% | 0.0% |
| 3 | `connect.(*connectUnaryUnmarshaler).Unmarshal` | 83.94s | 19.0% | 0.0% |
| 4 | `connect.(*connectUnaryUnmarshaler).UnmarshalFunc` | 83.94s | 19.0% | 0.0% |
| 5 | `connect.(*connectUnaryHandlerConn).Receive` | 83.69s | 19.0% | 0.0% |
| 6 | `connect.(*errorTranslatingHandlerConnCloser).Receive` | 83.69s | 19.0% | 0.0% |
| 7 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 83.15s | 18.9% | 0.0% |
| 8 | `connect.receiveUnaryMessage[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 83.14s | 18.9% | 0.0% |
| 9 | `connect.receiveUnaryRequest[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 83.14s | 18.9% | 0.0% |
| 10 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 4.07s | 0.9% | 0.0% |
| 11 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 3.75s | 0.8% | 0.0% |
| 12 | `kv.(*Coordinator).Put` | 3.75s | 0.8% | 0.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **62.75s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **21.49s** (34.2% of the sampled mutex).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 21.49s | 34.2% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 21.49s | 34.2% | 0.0% |
| 3 | `connect.(*connectUnaryUnmarshaler).Unmarshal` | 21.46s | 34.2% | 0.0% |
| 4 | `connect.(*connectUnaryUnmarshaler).UnmarshalFunc` | 21.46s | 34.2% | 0.0% |
| 5 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 21.44s | 34.2% | 0.0% |
| 6 | `connect.(*connectUnaryHandlerConn).Receive` | 21.43s | 34.2% | 0.0% |
| 7 | `connect.(*errorTranslatingHandlerConnCloser).Receive` | 21.43s | 34.2% | 0.0% |
| 8 | `connect.receiveUnaryMessage[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 21.43s | 34.2% | 0.0% |
| 9 | `connect.receiveUnaryRequest[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4]` | 21.43s | 34.2% | 0.0% |
| 10 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func1` | 30.3ms | 0.0% | 0.0% |
| 11 | `connect.NewUnaryHandler[go.shape.79ed9bc99cd79a6f42ec60efc24a1c0ac97389576f03a668d0e01b92beebf3c3,go.shape.d175b22a8b403c26d32b97654b40f24ce73aed556cd26abfd84f811375427b05].func2` | 30.3ms | 0.0% | 0.0% |
| 12 | `kv.(*Coordinator).Put` | 30.3ms | 0.0% | 0.0% |

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **4.76GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **70%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `connect.(*Handler).ServeHTTP` at **2.22GB** (46.6% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `connect.(*Handler).ServeHTTP` | 2.22GB | 46.6% | 0.0% |
| 2 | `wavespanv1connect.NewKvServiceHandler.func1` | 2.13GB | 44.8% | 0.0% |
| 3 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func2` | 1.22GB | 25.6% | 1.4% |
| 4 | `connect.NewUnaryHandler[go.shape.2bba6984dd13522db756084f76df939240dd5316924b89800c69bf7ff441d7c4,go.shape.07f96899219f8831c4094f87251c56860eed3d383a23ccfbccbe5b90a24ddc8a].func1` | 676.1MB | 13.9% | 0.0% |
| 5 | `kv.(*Service).Get` | 676.1MB | 13.9% | 0.0% |
| 6 | `kv.(*Reader).Get` | 667.1MB | 13.7% | 2.2% |
| 7 | `recordstore.(*Store).Get` | 486.0MB | 10.0% | 0.0% |
| 8 | `connect.(*connectHandler).NewConn` | 425.1MB | 8.7% | 7.2% |
| 9 | `connect.(*connectUnaryMarshaler).Marshal` | 318.6MB | 6.5% | 0.0% |
| 10 | `connect.(*connectUnaryHandlerConn).Send` | 285.0MB | 5.9% | 0.0% |
| 11 | `connect.(*errorTranslatingHandlerConnCloser).Send` | 285.0MB | 5.9% | 0.0% |
| 12 | `connect.(*connectHandler).SetTimeout` | 284.0MB | 5.8% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **96** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `wavesdb.(*DB).flushWorker` at **12** (12.5% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `wavesdb.(*DB).flushWorker` | 12 | 12.5% | 0.0% |
| 2 | `wavesdb.(*DB).compactionWorker` | 6 | 6.2% | 0.0% |
| 3 | `cache.(*Evictor).Run` | 3 | 3.1% | 0.0% |
| 4 | `membership.(*Service).Run` | 3 | 3.1% | 0.0% |
| 5 | `local.(*Fanout).Run` | 3 | 3.1% | 0.0% |
| 6 | `local.(*IntraAntiEntropy).Run` | 3 | 3.1% | 0.0% |
| 7 | `local.(*RepairEngine).Run` | 3 | 3.1% | 0.0% |
| 8 | `ttl.(*Sweeper).Run` | 3 | 3.1% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
