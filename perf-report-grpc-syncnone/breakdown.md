<!-- client-side workload result
kv-get                       ops=405251  errs=11      20263/s  p50=826µs     p95=1.7ms     p99=3.345ms  
kv-put                       ops=174794  errs=7        8740/s  p50=1.284ms   p95=2.603ms   p99=4.821ms  
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **69.64s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **89%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.New.inflightLimitInterceptor.func2` at **23.29s** (33.4% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.New.inflightLimitInterceptor.func2` | 23.29s | 33.4% | 0.1% |
| 2 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func3` | 23.03s | 33.1% | 0.0% |
| 3 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func4` | 22.03s | 31.6% | 0.1% |
| 4 | `v1._KvService_Put_Handler` | 11.81s | 17.0% | 0.0% |
| 5 | `syscall.Syscall` | 10.95s | 15.7% | 0.0% |
| 6 | `syscall.RawSyscall6` | 10.74s | 15.4% | 0.0% |
| 7 | `recordstore.(*Store).Apply` | 10.68s | 15.3% | 0.1% |
| 8 | `grpcsrv.(*KV).Put` | 10.59s | 15.2% | 0.0% |
| 9 | `v1._KvService_Put_Handler.func1` | 10.59s | 15.2% | 0.0% |
| 10 | `kv.(*Coordinator).Put` | 10.47s | 15.0% | 0.0% |
| 11 | `kv.(*Coordinator).write` | 10.47s | 15.0% | 0.0% |
| 12 | `syscall.Write` | 8.93s | 12.8% | 0.0% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **60.02s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `http.(*ServeMux).ServeHTTP` at **60.02s** (100.0% of the sampled block).
- **Hottest leaf: `runtime.selectgo` at 60.00s flat (100.0%)** — the actual blocking happens here, not just in callers above it.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `http.(*ServeMux).ServeHTTP` | 60.02s | 100.0% | 0.0% |
| 2 | `http.(*Server).Serve.gowrap3` | 60.02s | 100.0% | 0.0% |
| 3 | `http.(*conn).serve` | 60.02s | 100.0% | 0.0% |
| 4 | `http.HandlerFunc.ServeHTTP` | 60.02s | 100.0% | 0.0% |
| 5 | `http.serverHandler.ServeHTTP` | 60.02s | 100.0% | 0.0% |
| 6 | `pprof.Profile` | 60.02s | 100.0% | 0.0% |
| 7 | `pprof.sleep` | 60.00s | 100.0% | 0.0% |
| 8 | `runtime.selectgo` | 60.00s | 100.0% | 100.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **0ns** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **0%** of leaf cost; the application/storage/RPC frames below are the rest.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **6.60GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **67%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.New.inflightLimitInterceptor.func2` at **3.10GB** (46.9% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.New.inflightLimitInterceptor.func2` | 3.10GB | 46.9% | 0.0% |
| 2 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func3` | 3.06GB | 46.3% | 0.0% |
| 3 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func4` | 2.71GB | 41.1% | 0.0% |
| 4 | `v1._KvService_Put_Handler` | 1.75GB | 26.6% | 0.4% |
| 5 | `grpcsrv.(*KV).Put` | 1.62GB | 24.5% | 0.1% |
| 6 | `v1._KvService_Put_Handler.func1` | 1.62GB | 24.5% | 0.0% |
| 7 | `kv.(*Coordinator).Put` | 1.56GB | 23.7% | 0.0% |
| 8 | `kv.(*Coordinator).write` | 1.56GB | 23.7% | 0.0% |
| 9 | `recordstore.(*Store).Apply` | 1.06GB | 16.1% | 2.6% |
| 10 | `v1._KvService_Get_Handler` | 826.1MB | 12.2% | 0.7% |
| 11 | `v1._ReplicationService_StoreReplica_Handler` | 782.8MB | 11.6% | 0.3% |
| 12 | `kv.(*Coordinator).replicateMinAck` | 757.6MB | 11.2% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **1,516** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **99%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `local.(*RepairEngine).Run.func1` at **48** (3.2% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `local.(*RepairEngine).Run.func1` | 48 | 3.2% | 0.0% |
| 2 | `observability.(*countingConn).Read` | 19 | 1.3% | 0.0% |
| 3 | `wavesdb.(*DB).flushWorker` | 12 | 0.8% | 0.0% |
| 4 | `observability.(*countingListener).Accept` | 9 | 0.6% | 0.0% |
| 5 | `syscall.Read` | 6 | 0.4% | 0.0% |
| 6 | `syscall.Syscall` | 6 | 0.4% | 0.4% |
| 7 | `syscall.read` | 6 | 0.4% | 0.0% |
| 8 | `wavesdb.(*DB).compactionWorker` | 6 | 0.4% | 0.0% |
| 9 | `backup.(*Coordinator).RunSweep` | 3 | 0.2% | 0.0% |
| 10 | `cache.(*Evictor).Run` | 3 | 0.2% | 0.0% |
| 11 | `collections.(*Manager).sweepLoop` | 3 | 0.2% | 0.0% |
| 12 | `collections.(*httpTransport).Start.func1` | 3 | 0.2% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
