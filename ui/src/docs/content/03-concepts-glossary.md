---
title: Concepts & Glossary
section: Introduction
order: 3
summary: The vocabulary you need to read everything else — origin+1, target-N, holders, dynamic cache replicas, ranges, HLC, and the consistency labels on every response.
---

# Concepts & Glossary

WaveSpan has a precise vocabulary. These terms recur across the docs and appear directly in API responses.

## Write & durability

| Term | Meaning |
|------|---------|
| **Origin pod** | The pod that receives and coordinates a write. |
| **Origin+1** | Default write durability: the origin plus at least one nearby durable replica. The client ACK waits for both. |
| **Target-N** | The background convergence goal: each key held by the origin + N nearby replicas (default N=3). |
| **Durable replica** | Persisted to disk on a distinct Kubernetes node; counts toward the write ACK and the target. |
| **Fanout** | Post-ACK asynchronous replication to the remaining nearby candidates. |
| **Repair** | Background anti-entropy worker that detects and fills under-replicated keys. |

## Reads & locality

| Term | Meaning |
|------|---------|
| **Nearby** | Determined by the latency graph score, not static topology — measured RTT is authoritative. |
| **Holder** | Any pod holding a copy of a key/range (durable or cached). |
| **Holder directory** | Gossip-propagated summaries of what each member holds, per namespace; used for read routing and repair. |
| **Dynamic cache replica** | A value fetched on a read miss and cached locally with a live subscription. Derived and disposable; does **not** count toward target-N. |
| **Range** | A logical slice of the ordered keyspace. The unit of repair, scan routing, and directory compression. |
| **Range coverage certificate** | Issued by a range owner to a cache holder with a live subscription; lets that holder declare a scan `COMPLETE`. |

## Consistency labels (on every response)

`ResponseMeta` accompanies every result and carries three enums:

- **Read source** — where the value came from:
  `LOCAL_DURABLE` · `LOCAL_DYNAMIC_CACHE` · `FETCHED_CLOSEST_HOLDER` · `ROUTED_RANGE` · `GLOBAL_REMOTE`
- **Completeness** — for scans:
  `COMPLETE` · `PARTIAL` · `BEST_EFFORT`
- **Conflict state** — concurrency outcome:
  `CONFLICT_NONE` · `CONFLICT_RESOLVED` · `CONFLICT_SIBLINGS_PRESENT`

## Versioning & identity

| Term | Meaning |
|------|---------|
| **HLC** | Hybrid-logical clock: a physical millisecond timestamp + a logical counter + writer identity. Gives deterministic ordering of concurrent writes. |
| **Mutation ID** | `cluster_id + member_id + writer_sequence` (or a client idempotency key). Deduplicates retries and cross-cluster replays. |
| **Mutation log** | Append-only log written in the same transaction as each data write. Drives repair, subscriptions, global replication, and crash recovery. |

## Membership

| Term | Meaning |
|------|---------|
| **SWIM** | The gossip protocol: ping, suspect, confirm, and full-state exchange. |
| **Liveness states** | `ALIVE → SUSPECT → UNREACHABLE → DEAD → FORGOTTEN`. |
| **Membership UUID** | Persistent storage identity per pod. A rescheduled pod with an empty volume is a *new* member. |
| **Latency graph** | Directed, time-decayed graph of RTT probes; edges carry EWMA / p95 / packet-loss. |
| **Spot node** | An ephemeral Kubernetes node that may disappear with little warning. |

## Replication scope

| Term | Meaning |
|------|---------|
| **`all`** | Every node in the *current* cluster holds the key. Never crosses a cluster boundary. |
| **`global`** | Every node in *every* cluster holds the key, shipped across peers. |
| **Backfill** | A joining node pages existing records from a peer via the `Backfill` RPC for `all`/`global` namespaces. |
| **Geo policy** | How replication respects geography: `prefer-local-geo`, `require-local-geo`, `latency-only`, `global-active-active`. |
| **Spillover** | When `prefer-local-geo` can't place a same-geo replica, it may spill to the nearest allowed geo for durability. |
