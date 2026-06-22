---
title: Architecture
section: Introduction
order: 2
summary: The major subsystems — storage, membership, latency graph, placement, replication, cache, graph & vector — and how a read and a write flow through them.
---

# Architecture

WaveSpan is a single Go binary (`wavespan-node`) that runs as a Kubernetes pod. Every pod is identical and embeds all subsystems in-process; there is no separate coordinator tier.

## Component map

```text
        Clients (KV API · Cypher · vector procedures)
                          │
        ┌─────────────────┴─────────────────┐
        │     Gateway (optional routing,     │
        │       auth, Cypher planning)       │
        └─────────────────┬─────────────────┘
                          │
   ┌──────────┬───────────┼───────────┬──────────┐
   │  Pod A   │   Pod B   │   Pod C   │   …      │   ← one cluster
   │ wavesdb  │  wavesdb  │  wavesdb  │          │
   │ gossip   │  gossip   │  gossip   │  ← SWIM + latency graph
   │ kv/gr/vec│  kv/gr/vec│  kv/gr/vec│          │
   └──────────┴───────────┴───────────┴──────────┘
                          │  active-active async
              ┌───────────┴────────────┐
              │   Peer clusters (global) │   ← multi-region
              └──────────────────────────┘
```

## Subsystems

| Subsystem | Package | Responsibility |
|-----------|---------|----------------|
| Local storage | `internal/storage` | Thin wrapper over the `wavesdb` LSM engine — column families, MVCC snapshots, iterators, TTL, transactions. |
| Membership & gossip | `internal/membership` | SWIM-style liveness and metadata exchange. |
| Latency graph | `internal/latencygraph` | Time-decayed RTT graph between every pair of members; drives placement. |
| Placement | `internal/placement` | Selects which nodes hold a replica given latency + geo policy. |
| KV store | `internal/kv` | Point operations: Put / Get / Delete / MultiGet / Scan. |
| Local replication | `internal/replication/local` | Origin+1 write path, async fanout, repair, backfill. |
| Global replication | `internal/replication/global` | Active-active cross-cluster replication and anti-entropy. |
| Cache | `internal/cache` | Dynamic read-created replicas and subscriptions. |
| Versioning | `internal/version` | Hybrid-logical clocks and writer identity. |
| Conflict | `internal/conflict` | Pluggable resolution (HLC-LWW, keep-siblings). |
| Graph | `internal/graph` | Property-graph encoding into the ordered keyspace. |
| Cypher | `internal/cypher` | Parser + planner for the supported Cypher subset. |
| Vector | `internal/vector` | Raw vectors, exact search, and ANN indexes. |
| Observability | `internal/observability` | Prometheus metrics, tracing, readiness, the streaming feeds this UI uses. |

## The write path

1. A client sends `Put(namespace, key, value)` to any pod. That pod becomes the **origin**.
2. The origin writes locally to `wavesdb` and appends to its **mutation log** in the same transaction.
3. It replicates to the nearest durable candidate (chosen from the latency graph).
4. Once durable on the **origin + 1** nearby replica, it returns an ACK with a version.
5. Asynchronously, a **fanout** worker fills the remaining target-N replicas; the **repair** engine later heals anything still under-replicated.

## The read path

1. A client sends `Get(namespace, key)` to a pod.
2. The pod checks its **local latest pointer**. A hit returns immediately as `LOCAL_DURABLE` or `LOCAL_DYNAMIC_CACHE`.
3. On a miss, it consults the gossip-propagated **holder directory**, fetches from the **closest holder**, and returns `FETCHED_CLOSEST_HOLDER`.
4. The fetched value is stored as a **dynamic cache replica** and the pod subscribes to future updates.

Every response is tagged with its read source, completeness, and conflict state — see [Consistency & replication](doc:consistency-and-replication).

## Internal keyspace

All data lives in the single ordered `wavesdb` keyspace, partitioned by logical prefix:

```text
/kv/{namespace}/data/{key}            # KV records
/graph/{graph}/node/{id}              # graph nodes
/graph/{graph}/edge/out/{src}/{type}/{dst}/{edge}
/vector/{collection}/raw/{id}         # raw vectors
/vector/{collection}/ann/{index}/...  # ANN segments
```

Ranges of this keyspace are the unit of repair scheduling, scan routing, and holder-directory compression.
