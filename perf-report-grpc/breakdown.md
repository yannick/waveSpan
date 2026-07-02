<!-- client-side workload result
kv-get                       ops=35057   errs=0        1753/s  p50=395µs     p95=912µs     p99=1.804ms  
kv-put                       ops=15054   errs=0         753/s  p50=34.28ms   p95=85.772ms  p99=140.443ms
-->

# WaveSpan performance breakdown

**Workload:** kv @ concurrency=32 for 20s

**Nodes profiled:** node1, node2, node3 · **CPU profile window:** 20s

> Read order for a latency hunt: **Latency (block)** and **Lock contention** first — that is where wall-clock time is lost. CPU and allocations explain *throughput* ceilings and GC tail latency.

## CPU — where on-CPU time goes

Sampled on-CPU work. High cumulative % = code paths burning CPU.

Total sampled: **21.83s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **93%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `syscall.Syscall` at **7.74s** (35.5% of the sampled cpu).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `syscall.Syscall` | 7.74s | 35.5% | 0.1% |
| 2 | `syscall.RawSyscall6` | 7.65s | 35.0% | 0.0% |
| 3 | `grpcsrv.New.inflightLimitInterceptor.func2` | 6.90s | 31.6% | 0.0% |
| 4 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func3` | 6.88s | 31.5% | 0.1% |
| 5 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func4` | 6.79s | 31.1% | 0.0% |
| 6 | `recordstore.(*Store).Apply` | 5.64s | 25.8% | 0.0% |
| 7 | `storage.(*WavesdbStore).BatchRC` | 5.35s | 24.5% | 0.0% |
| 8 | `storage.(*WavesdbStore).batch` | 5.35s | 24.5% | 0.1% |
| 9 | `wavesdb.(*Txn).Commit` | 5.25s | 24.0% | 0.2% |
| 10 | `wavesdb.(*ColumnFamily).applyCommit` | 5.16s | 23.6% | 0.0% |
| 11 | `wal.(*WAL).AppendBatch` | 4.70s | 21.5% | 0.0% |
| 12 | `wal.(*WAL).flushGroup` | 4.15s | 19.0% | 0.1% |

## Latency — where REQUEST goroutines BLOCK (off-CPU)

Off-CPU wait time on the request path (fsync, channel/select, network, sync waits) — where latency hides. Idle background loops (repair/anti-entropy/evictor tickers) are EXCLUDED; only samples passing through a request handler are counted.

Total sampled: **60.01s** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `http.(*ServeMux).ServeHTTP` at **60.01s** (100.0% of the sampled block).
- **Hottest leaf: `runtime.selectgo` at 60.00s flat (100.0%)** — the actual blocking happens here, not just in callers above it.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `http.(*ServeMux).ServeHTTP` | 60.01s | 100.0% | 0.0% |
| 2 | `http.(*Server).Serve.gowrap3` | 60.01s | 100.0% | 0.0% |
| 3 | `http.(*conn).serve` | 60.01s | 100.0% | 0.0% |
| 4 | `http.HandlerFunc.ServeHTTP` | 60.01s | 100.0% | 0.0% |
| 5 | `http.serverHandler.ServeHTTP` | 60.01s | 100.0% | 0.0% |
| 6 | `pprof.Profile` | 60.01s | 100.0% | 0.0% |
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

Total sampled: **2.36GB** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **68%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `grpcsrv.New.inflightLimitInterceptor.func2` at **457.6MB** (19.0% of the sampled alloc).
- **RPC (de)serialization** allocates heavily — the main GC driver on the hot path.
- **scan/result buffering** allocates a lot — large intermediate slices per query.

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `grpcsrv.New.inflightLimitInterceptor.func2` | 457.6MB | 19.0% | 0.0% |
| 2 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func3` | 450.6MB | 18.7% | 0.0% |
| 3 | `local.(*RepairEngine).Backfill` | 446.6MB | 18.5% | 0.0% |
| 4 | `local.(*RepairEngine).BackfillOnce` | 446.6MB | 18.5% | 0.0% |
| 5 | `recordstore.(*Store).ScanRecordsFrom` | 439.6MB | 18.2% | 0.9% |
| 6 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func4` | 384.1MB | 15.9% | 0.0% |
| 7 | `local.(*IntraAntiEntropy).ReconcileOnce` | 320.7MB | 13.3% | 0.0% |
| 8 | `local.(*IntraAntiEntropy).Run` | 320.7MB | 13.3% | 0.0% |
| 9 | `proto.Unmarshal` | 280.9MB | 11.6% | 0.0% |
| 10 | `proto.UnmarshalOptions.unmarshal` | 280.9MB | 11.6% | 0.0% |
| 11 | `impl.(*MessageInfo).unmarshal` | 280.4MB | 11.6% | 0.0% |
| 12 | `impl.(*MessageInfo).unmarshalPointer` | 280.4MB | 11.6% | 0.0% |

## Goroutine concurrency snapshot

Where goroutines were parked at capture time — corroborates the blocking story.

Total sampled: **1,523** across 3 node(s). Go runtime / HTTP-framework plumbing accounts for **100%** of leaf cost; the application/storage/RPC frames below are the rest.

- Biggest single cost (cum): `local.(*RepairEngine).Run.func1` at **48** (3.2% of the sampled goroutine).

Top application / storage / RPC frames (cum = time spent in this function + everything it calls):

| # | function | cum | cum % | flat % |
|---|---|--:|--:|--:|
| 1 | `local.(*RepairEngine).Run.func1` | 48 | 3.2% | 0.0% |
| 2 | `observability.(*countingConn).Read` | 15 | 1.0% | 0.0% |
| 3 | `recordstore.(*Store).Apply` | 15 | 1.0% | 0.0% |
| 4 | `storage.(*WavesdbStore).BatchRC` | 15 | 1.0% | 0.0% |
| 5 | `storage.(*WavesdbStore).batch` | 15 | 1.0% | 0.0% |
| 6 | `wavesdb.(*ColumnFamily).applyCommit` | 15 | 1.0% | 0.0% |
| 7 | `wavesdb.(*Txn).Commit` | 15 | 1.0% | 0.0% |
| 8 | `wal.(*WAL).AppendBatch` | 15 | 1.0% | 0.0% |
| 9 | `grpcsrv.(*KV).Put` | 14 | 0.9% | 0.0% |
| 10 | `grpcsrv.New.GRPCMetricsUnaryInterceptor.func4` | 14 | 0.9% | 0.0% |
| 11 | `grpcsrv.New.Identity.GRPCUnaryInterceptor.func3` | 14 | 0.9% | 0.0% |
| 12 | `grpcsrv.New.inflightLimitInterceptor.func2` | 14 | 0.9% | 0.0% |

---
_Generated by `wavespan-profile`. Captured via Go's net/http/pprof on each node's admin port._
