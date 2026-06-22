# KV throughput optimization

Targets: **> 10,000 inserts/sec** and **> 20,000 queries/sec** per node.

Result (3-node docker cluster, single coordinator, `wavespan-bench kv`, 3000 keys, conc 128):

| workload | before this work | after | target |
|---|--:|--:|--:|
| pure read (queries) | ~5,400/s | **26,746/s** | > 20,000 ✅ |
| pure write (inserts) | ~2,400/s | **15,574/s** | > 10,000 ✅ |

Both exceeded with margin, 0 errors, p99 < 30 ms. (Numbers are bench-only; pprof depresses
throughput ~2×. The Docker-for-Mac host CPU caps the *mixed* 50/50 run at ~16.8k combined — a host
artifact, not a code limit.)

## The 10 changes

| # | change | status | effect |
|---|---|---|---|
| 1 | **HTTP/2 (h2c) on the plaintext data port + clients** (`internal/rpcopts`) | done | the dominant lever — HTTP/1.1 serialized in-flight requests per connection; h2c multiplexes. ~2× throughput, tail latency 50 ms → 8 ms |
| 2 | **Concurrent commits** — `storage.BatchRC` (ReadCommitted) + per-key stripe lock in `recordstore` | done | removed the global `db.writeMu` serialization; independent keys commit in parallel. Puts 5.2k → 8.1k |
| 6 | **Tuned h2c transport** (pooled, keep-alive, read-idle/ping timeouts) | done | bundled with #1; connection reuse for internal + bench clients |
| 7 | **Skip read-before-write** — in-memory latest-version cache per stripe | done | the monotonic common write skips the latest-pointer storage Get + decode. Writes 13.3k → 15.6k |
| 10 | **GC tuning** — `GOGC=300`, `GOMEMLIMIT` | done | with allocation already cut ~3×, collect less often. Reads 21.8k → 26.7k |
| — | (prior) skip gzip on small responses (`WithCompressMinBytes`) | done | removed the top heap allocator + ~35% CPU |
| 5 | **Shard the per-CF write path** | verified | the wavesdb memtable is already internally locked and `applyCommit` does `mem.Put` outside `cf.mu`, so same-CF writes already proceed concurrently; finer sharding is not the bottleneck (writes exceed target) |
| 3 | **Pipeline/batch origin+1 replication** | addressed | h2c multiplexing already removes the per-write connection serialization; secondary target-N fanout is async. A batched `StoreReplica` RPC remains a future latency win |
| 4 | **Group-commit at the recordstore layer** | addressed | subsumed by #2 (parallel independent commits) + the WAL group commit already in wavesdb |
| 8 | **Faster codec + message pooling** | partial | the dominant per-write buffer is already pooled (`encBufPool`); pooling the small `LatestPointer`/`MutationEnvelope` structs is marginal and deferred |
| 9 | **Local decoded-record read cache** | deferred | reads already exceed the target by 34%; a write-invalidated read cache would lift the host-bound *mixed* ceiling but adds read-consistency surface — deferred until a real-hardware bottleneck justifies it |

## Correctness

All changes are validated: race-clean unit tests; the **Jepsen harness** (register-under-partition,
set-under-kill, idempotency-under-pause) confirms convergence / durability / lww-determinism /
idempotency still hold under faults with the concurrent-commit path; the full docker integration
suite passes.

## Reproduce

```bash
docker compose -f docker/docker-compose.profile.yaml up -d
bin/wavespan-bench load --addr localhost:7831 --kv 3000 --users 0 --follows 0
bin/wavespan-bench kv --addr localhost:7831 --concurrency 128 --duration 8s --keys 3000 --read-ratio 1.0  # queries
bin/wavespan-bench kv --addr localhost:7831 --concurrency 128 --duration 8s --keys 3000 --read-ratio 0.0  # inserts
```
