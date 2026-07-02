<!-- client-side workload result
kv-get                       ops=513827  errs=34      25691/s  p50=525µs     p95=893µs     p99=1.413ms  
kv-put                       ops=221569  errs=16      11078/s  p50=1.446ms   p95=2.454ms   p99=3.458ms  
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **47.76s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **86%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `recordstore.(*Store).Apply` at **10.49s** (22.0% of the sampled cpu).
- **protobuf (de)serialization** is a large CPU share — consider fewer/larger RPCs or caching decoded forms.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `recordstore.(*Store).Apply` | 10.49s | 22.0% | 0.0% |
| 2 | `grpcsrv.New.inflightLimitInterceptor.func6` | 10.38s | 21.7% | 0.1% |
| 3 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func7` | 10.22s | 21.4% | 0.0% |
| 4 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func8` | 9.52s | 19.9% | 0.0% |
| 5 | `storage.(*WavesdbStore).BatchRC` | 8.32s | 17.4% | 0.0% |
| 6 | `storage.(*WavesdbStore).batch` | 8.31s | 17.4% | 0.3% |
| 7 | `wavesdb.(*Txn).Commit` | 7.62s | 16.0% | 0.1% |
| 8 | `wavesdb.(*ColumnFamily).applyCommit` | 7.25s | 15.2% | 0.1% |
| 9 | `v1._KvService_Put_Handler` | 7.08s | 14.8% | 0.0% |
| 10 | `grpcsrv.(*KV).Put` | 6.12s | 12.8% | 0.0% |
| 11 | `v1._KvService_Put_Handler.func1` | 6.12s | 12.8% | 0.0% |
| 12 | `kv.(*Coordinator).Put` | 6.05s | 12.7% | 0.0% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **224.88s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.New.GRPCMetricsUnaryInterceptor.func8` at **223.80s** (99.5% of the sampled block).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func8` | 223.80s | 99.5% | 0.0% |
| 2 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func7` | 223.80s | 99.5% | 0.0% |
| 3 | `grpcsrv.New.inflightLimitInterceptor.func6` | 223.80s | 99.5% | 0.0% |
| 4 | `v1._KvService_Put_Handler` | 220.53s | 98.1% | 0.0% |
| 5 | `grpcsrv.(*KV).Put` | 220.52s | 98.1% | 0.0% |
| 6 | `kv.(*Coordinator).Put` | 220.52s | 98.1% | 0.0% |
| 7 | `kv.(*Coordinator).write` | 220.52s | 98.1% | 0.0% |
| 8 | `v1._KvService_Put_Handler.func1` | 220.52s | 98.1% | 0.0% |
| 9 | `kv.(*Coordinator).replicateMinAck` | 220.09s | 97.9% | 0.0% |
| 10 | `grpcsrv.(*Replication).StoreReplicaBatch` | 3.26s | 1.4% | 0.0% |
| 11 | `v1._ReplicationService_StoreReplicaBatch_Handler` | 3.26s | 1.4% | 0.0% |
| 12 | `v1._ReplicationService_StoreReplicaBatch_Handler.func1` | 3.26s | 1.4% | 0.0% |

## Lock contention (request path)

Time request goroutines spent waiting on contended mutexes. High values = a serialization bottleneck. Background-loop contention is excluded.

Total sampled: **472.7ms** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.(*Replication).StoreReplicaBatch.func1` at **296.4ms** (62.7% of the sampled mutex).
- contention in the **record store** write path — commits may share one lock.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.(*Replication).StoreReplicaBatch.func1` | 296.4ms | 62.7% | 0.0% |
| 2 | `grpcsrv.(*Replication).StoreReplicaBatch.gowrap1` | 296.4ms | 62.7% | 0.0% |
| 3 | `local.(*Receiver).Apply` | 296.4ms | 62.7% | 0.0% |
| 4 | `local.(*Idempotency).Check` | 195.1ms | 41.3% | 0.0% |
| 5 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func8` | 60.6ms | 12.8% | 0.0% |
| 6 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func7` | 60.6ms | 12.8% | 0.0% |
| 7 | `grpcsrv.New.inflightLimitInterceptor.func6` | 60.6ms | 12.8% | 0.0% |
| 8 | `grpcsrv.(*KV).Put` | 59.0ms | 12.5% | 0.0% |
| 9 | `kv.(*Coordinator).Put` | 59.0ms | 12.5% | 0.0% |
| 10 | `kv.(*Coordinator).write` | 59.0ms | 12.5% | 0.0% |
| 11 | `v1._KvService_Put_Handler` | 59.0ms | 12.5% | 0.0% |
| 12 | `v1._KvService_Put_Handler.func1` | 59.0ms | 12.5% | 0.0% |

## Allocations (GC pressure)

Bytes allocated since start. Heavy allocation drives GC, which adds tail latency.

Total sampled: **6.47GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **61%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.New.inflightLimitInterceptor.func6` at **2.06GB** (31.8% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.New.inflightLimitInterceptor.func6` | 2.06GB | 31.8% | 0.0% |
| 2 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func7` | 2.02GB | 31.3% | 0.0% |
| 3 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func8` | 1.64GB | 25.4% | 0.0% |
| 4 | `v1._KvService_Put_Handler` | 1.43GB | 22.1% | 0.5% |
| 5 | `grpcsrv.(*KV).Put` | 1.26GB | 19.5% | 0.2% |
| 6 | `v1._KvService_Put_Handler.func1` | 1.26GB | 19.5% | 0.0% |
| 7 | `recordstore.(*Store).Apply` | 1.23GB | 19.0% | 3.7% |
| 8 | `kv.(*Coordinator).Put` | 1.17GB | 18.1% | 0.0% |
| 9 | `kv.(*Coordinator).write` | 1.17GB | 18.1% | 0.0% |
| 10 | `storage.(*WavesdbStore).BatchRC` | 824.1MB | 12.4% | 0.0% |
| 11 | `storage.(*WavesdbStore).batch` | 824.1MB | 12.4% | 0.0% |
| 12 | `v1._KvService_Get_Handler` | 764.6MB | 11.5% | 1.2% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **1,526** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `local.(*RepairEngine).Run.func1` at **48** (3.1% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `local.(*RepairEngine).Run.func1` | 48 | 3.1% | 0.0% |
| 2 | `observability.(*countingConn).Read` | 18 | 1.2% | 0.0% |
| 3 | `wavesdb.(*DB).flushWorker` | 12 | 0.8% | 0.0% |
| 4 | `observability.(*countingListener).Accept` | 9 | 0.6% | 0.0% |
| 5 | `wavesdb.(*DB).compactionWorker` | 6 | 0.4% | 0.0% |
| 6 | `backup.(*Coordinator).RunSweep` | 3 | 0.2% | 0.0% |
| 7 | `cache.(*Evictor).Run` | 3 | 0.2% | 0.0% |
| 8 | `collections.(*Manager).sweepLoop` | 3 | 0.2% | 0.0% |
| 9 | `collections.(*httpTransport).Start.func1` | 3 | 0.2% | 0.0% |
| 10 | `health.(*Monitor).Start.func1` | 3 | 0.2% | 0.0% |
| 11 | `membership.(*Service).Run` | 3 | 0.2% | 0.0% |
| 12 | `local.(*Fanout).Run` | 3 | 0.2% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
