# Architecture

WaveSpan layers distributed behaviour on top of `wavesdb`, an embedded Go LSM key-value engine.
`wavesdb` provides local persistence only; everything distributed — membership, replication,
caching, repair, routing — is built above it.

```
        clients (wavespanctl / Connect / gRPC)
                       │
                       ▼
   ┌──────────────────────────────────────────────────┐
   │ data pod (cmd/wavespan-node)                       │
   │                                                    │
   │  KvService  ──►  coordinator ──► origin+1 write     │
   │     │                │                              │
   │     │                ├─► placement (latency graph)  │
   │     │                ├─► StoreReplica → nearby peers │
   │     │                └─► fanout / repair (target-N)  │
   │     │                                               │
   │  reader ──► local store / closest-holder fetch ──► dynamic cache + subscribe
   │     │                                               │
   │  scanner ──► cache-fast | routed | cache-complete   │
   │                                                    │
   │  membership (SWIM gossip + latency graph)           │
   │  recordstore ──► wavesdb (column families)          │
   └──────────────────────────────────────────────────┘
                       ▲
                       │  gossip (SWIM) + holder summaries
                  other data pods
```

## Components

Each **data pod** (`wavespan-node`) is a self-contained, stateful unit:

- **`internal/recordstore`** — the local versioned-record primitive over `wavesdb`: it assigns
  HLC versions, applies a record + latest-pointer + mutation-log entry atomically, does
  hlc-last-write-wins resolution, and serves local reads and range scans. The only package that
  knows the on-disk key layout.
- **`internal/membership`** — SWIM-style gossip, the liveness state machine
  (`ALIVE → SUSPECT → UNREACHABLE → DEAD → FORGOTTEN`), and a directed, time-decayed latency graph
  (EWMA + p95 + packet loss) that is the authoritative signal for placement. Gossip also carries
  compact holder summaries (bloom filters) so reads can resolve holders without broadcasting.
- **`internal/placement`** — picks nearby durable-replica candidates: hard filters (alive,
  distinct node, geo/compliance) then scoring by the latency graph.
- **`internal/replication/local`** — the `StoreReplica` receiver/client, the origin+1 nothing-
  fancy fanout, the per-key holder directory, the target-N background fill, and the repair engine
  (a severity-ordered priority queue with rate limiting and churn backpressure).
- **`internal/cache`** — the holder-summary directory, the closest-holder `FetchReplica` client,
  the dynamic cache store, live key subscriptions (`SubscribeKey` streaming + resync), eviction,
  and range-coverage certificates.
- **`internal/kv`** — the public `KvService`: the write coordinator, the read path, and the scan
  dispatcher.
- **`internal/ttl`** — lazy TTL: read-time hide-expired and a background sweeper that tombstones
  expired keys.
- **`internal/config` / `internal/observability` / `internal/version`** — config loading, logging
  + Prometheus metrics + health, and HLC/version primitives.

The optional **gateway** (`wavespan-gateway`) is a stateless front door (auth, routing, query
planning) — a stub today. The **operator** (Kubernetes) arrives in a later milestone.

## The keyspace

All models are encoded into one ordered keyspace, split across `wavesdb` column families:

| Column family | Holds |
|---|---|
| `sys` | member metadata, durable storage UUID |
| `kv_data` | versioned records: `lenPrefix(ns) ‖ lenPrefix(userKey) ‖ version` |
| `kv_meta` | latest-pointer per key (`lenPrefix(ns) ‖ userKey`, order-preserving) + the TTL bucket index |
| `repl_log` | the local mutation log (replay, repair, replication, recovery) |
| `graph_*`, `vector_*`, `cache_meta` | reserved for later milestones |

The latest-pointer key is **order-preserving** in the user key, so range scans within a namespace
return keys in their natural byte order.

## Identity

- **`memberId`** — runtime identity (also the DNS-resolvable advertise host in Docker).
- **`storageUuid`** — durable storage identity, persisted in `sys`. A pod rescheduled onto an
  empty volume gets a new storage UUID and is treated as a brand-new storage member — central to
  surviving spot churn.

## A write (origin+1)

1. A client `Put` lands on any pod; that pod becomes the **write coordinator**.
2. It stamps an HLC version and atomically writes the record + latest pointer + mutation-log entry
   locally (the origin durable copy).
3. It selects nearby candidates from the latency graph and sends `StoreReplica` to them.
4. It **acknowledges once at least one nearby durable replica has stored the mutation** (origin+1).
5. In the background it fans out to the target-N replica count and gossips its holder summary.

If no nearby replica can be reached, the write fails (`InsufficientNearbyReplicas`) rather than
silently dropping durability — unless `minAck=0` is configured (single-node / local-only dev).

## A read

1. The serving pod checks its local store. If present, it returns the value, tagging the source
   `LOCAL_DURABLE` or `LOCAL_DYNAMIC_CACHE`.
2. On a miss, it resolves likely holders from the gossiped holder directory (no broadcast), fetches
   from the closest one (`FETCHED_CLOSEST_HOLDER`), stores the value as a dynamic cache replica, and
   subscribes to future updates so later reads are local.

## A scan

The scan dispatcher offers four modes and always declares completeness in the stream header:

- `cache-fast` (default) — local cache/durable only; fast, `BEST_EFFORT`, never `COMPLETE`.
- `routed-eventual` — contact known holders, k-way merge sorted/deduped; `PARTIAL`.
- `cache-complete` — `COMPLETE` **only** with a valid range-coverage certificate, else downgrades.
- `local-only` — local store only; for debugging.

See [Consistency & replication](consistency-and-replication.md) for the guarantees behind these.
