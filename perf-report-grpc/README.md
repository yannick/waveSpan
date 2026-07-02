# gRPC-era performance baseline (design/37 P0.3)

Date: 2026-07-02. First profile capture on the **live gRPC transport** (post commit `6917b85`);
every older `perf-report-*` directory profiles the removed Connect/h2c stack and is historical only.

Method: 3-node `docker/docker-compose.profile.yaml` cluster (Docker Desktop for Mac, M2 Max —
fsync in this VM is pathologically slow, treat write latencies as worst-case), 3,000-key preload,
`wavespan-profile --workload kv --seconds 20 --concurrency 32` (pprof depresses throughput ~2×,
see bench/THROUGHPUT.md).

## Headline: throughput under the new durability default

Two runs, identical except `storage.engine.syncMode` (design/37 P0.1 flipped the default to `full`):

| syncMode | kv-put | kv-get |
|---|---|---|
| `none` (old default) — `perf-report-grpc-syncnone/` | 8,740/s, p50 1.28ms, p99 4.8ms | 20,263/s, p50 826µs |
| `full` (new default) — this directory | 753/s, p50 34ms, p99 140ms | 1,753/s (workers starved by puts) |

The gap is dominated by Docker-for-Mac's fsync (~15–30ms per group commit, paid on origin AND on
the sync replica); production Linux NVMe fdatasync is ~50–500µs and group commit amortizes across
concurrent committers, so the production delta will be far smaller — but this needs a Linux
measurement before the P0.1 default can be called cheap. Dev/profile composes that don't need the
durability contract should override `WAVESPAN_TUNABLE_STORAGE_ENGINE_SYNC_MODE`.

## Write path: serial replication RPC is the dominant blocker (A1 confirmed live)

From the raw block profile of the `syncMode=none` run (node1, 20s):

- `kv.(*Coordinator).replicateMinAck` accounts for **83s of request blocking**, of which
  **99.55% is inside `local.(*ConnectReplicator).StoreReplica`** — the one-at-a-time unary
  replica write (~1.4ms avg per put). This is design/37 A1 / task P1.4, now confirmed on the
  shipping transport.

## CPU (syncMode=none run, 69.6s sampled across 3 nodes)

- gRPC unary interceptor chain (`inflightLimit` → `Identity` → `Metrics`) carries ~33% cum;
  `v1._KvService_Put_Handler` 17%; `recordstore.(*Store).Apply` 15.3%; network syscalls ~15%.
- Protobuf (de)serialization remains the main CPU driver on the hot path — fewer/larger RPCs
  (P1.4 batching) attacks this directly.
- Alloc: interceptor chain ~19%; `local.(*RepairEngine).Backfill` 18.5% — the rolling
  full-keyspace backfill scan (design/37 B1/P1.5) allocates heavily even in a 20s window.

## Tooling gap found (fix before trusting block/mutex sections)

`wavespan-profile`'s "request-path" filter for the block/mutex sections matches Connect/net-http
frames, which now only exist on the admin server — so its BLOCK section reports the pprof capture
handler's own 20s sleep (`pprof.Profile → pprof.sleep → runtime.selectgo`, "100%") instead of
request blocking. Until it is taught gRPC frames (`v1._KvService_*_Handler`,
`grpcsrv.*`), drill into the raw `.pb.gz` files with
`go tool pprof -show_from '<frame>'` as above.
