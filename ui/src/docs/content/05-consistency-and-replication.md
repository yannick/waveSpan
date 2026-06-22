---
title: Consistency & Replication
section: Reference
order: 5
summary: The eventual-consistency contract — origin+1 acks, dynamic cache replicas, repair, and the optional session read-your-writes token.
---

# Consistency & Replication

WaveSpan's central design decision (ADR-0001) is to **accept eventual consistency explicitly**. There is no linearizable read or write mode in v1. In exchange you get low latency, durability beyond a single pod, and metadata that never lies about what you got.

## The write contract

A write is acknowledged when it is durable on the **origin pod plus at least one nearby durable replica** (origin+1, ADR-0002). This is the floor; `minAckNearbyReplicas` can require more.

- **Origin+1** guarantees the value survives the loss of any single pod *before* the client sees an ACK.
- **Target-N** (default 3) is the *eventual* replica count, reached asynchronously after the ACK.
- The gap between them is filled by **fanout** (immediately after ack) and **repair** (continuously).

```text
Put ──▶ origin writes locally ─┬─▶ replicate to nearest durable peer
                               │       │
                               │       └─▶ both durable ──▶ ACK (origin+1)
                               │
                               └─▶ async fanout to target-N ──▶ repair heals the rest
```

## The read contract

A read returns the newest value known to the serving pod or the holder it fetches from. It may be stale. The `ResponseMeta.source` tells you which path served it:

| Source | Meaning | Typical latency |
|--------|---------|-----------------|
| `LOCAL_DURABLE` | served from a durable local replica | lowest |
| `LOCAL_DYNAMIC_CACHE` | served from a read-created cache replica | lowest |
| `FETCHED_CLOSEST_HOLDER` | local miss; fetched from the nearest holder | one hop |
| `ROUTED_RANGE` | routed to a range holder | one hop |
| `GLOBAL_REMOTE` | served across a cluster boundary | cross-region |

## Dynamic cache replicas

When a read misses locally, the serving pod fetches the value from the closest holder, **stores it as a dynamic cache replica**, and subscribes to future updates (ADR-0003).

- Cache replicas are **derived and disposable** — they can be evicted under memory pressure and do not count toward target-N durability.
- Subscriptions are **best-effort**: gaps are allowed. If a subscription lapses, the next read simply re-fetches.
- This gives progressive read locality: hot keys naturally migrate close to where they're read.

## Repair & anti-entropy

The repair engine (`internal/replication/local/repair.go`) is a background worker that:

1. Detects under-replication via the gossiped holder directory.
2. Elects a repair source using the latency graph.
3. Streams missing records and applies them idempotently (last-write-wins by HLC).

Repair is what makes the system **spot-node tolerant** — when a pod dies, its durable replicas are rebuilt elsewhere without operator intervention.

## Session read-your-writes (optional)

For the common "I just wrote it, I need to read it" case, a client may carry a **session token**. Reads in that session wait for — or fetch — at least the version the session last wrote, giving read-your-writes within the session without imposing global linearizability.

## What you do *not* get

- No linearizable or serializable reads/writes.
- No globally-consistent scans (completeness is declared, not guaranteed `COMPLETE`).
- No exact TTL (expiry is lazy — see [Configuration](doc:configuration)).

These are deliberate non-goals. If your workload needs them, WaveSpan is the wrong fit, and it tells you so rather than pretending.
