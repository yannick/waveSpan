---
title: FAQ & Non-goals
section: Operations
order: 14
summary: Honest answers about what WaveSpan does and does not guarantee — consistency, SQL, performance, and when not to use it.
---

# FAQ & Non-goals

## What consistency do I actually get?

**Eventual consistency, explicitly.** A read returns the newest value known to the serving pod or the holder it fetches from, and it *tells you the source* on every response. Writes are durable on two distinct nodes (origin+1) before ack. There is no linearizable mode in v1.

## Can I do read-your-writes?

Yes, *within a session*. Carry the optional session token and reads will wait for — or fetch — at least the version your session last wrote. There is no global read-your-writes across independent clients.

## Why no SQL?

Two reasons. First, point reads must be a single framed protobuf round-trip — SQL parsing and planning would add latency to the hot path. Second, the data model is KV + property-graph + vectors, which Cypher expresses more naturally. So there are exactly two query surfaces: the gRPC KV API and the Cypher subset.

## Is the Cypher real openCypher?

It's a **production-safe subset**, not full compatibility. The common read/write clauses work; `LOAD CSV`, arbitrary procedures, and full subqueries do not. See [Cypher & Graph](doc:cypher-and-graph).

## What happens when a spot node dies mid-write?

If the write was already acked, it was durable on origin+1 — so it survives. In-flight writes to a dying node fail and should be retried (use an `idempotency_key` so the retry is safe). Repair rebuilds the lost node's replicas elsewhere, and a replacement node backfills `all`/`global` namespaces.

## How is `all` different from `global`?

`all` = every node in **this** cluster. `global` = every node in **every** cluster. `all` never crosses a cluster boundary. Picking the wrong one is a common bug — verify holders in the [Data Browser](doc:overview). Full detail in [Replication Factor](doc:replication-factor).

## Is TTL exact?

No. TTL is **lazy and best-effort** — expired records are dropped at compaction and propagated via tombstones. A lagging replica may serve an expired value briefly. Don't use TTL for correctness-critical expiry.

## What's the performance story?

WaveSpan optimizes the point-read/-write path: h2c, concurrent commits, skip-read on put, connection pooling, and `MultiGet` to batch reads. Recent work targeted ~10× KV throughput. Run `wavespan-bench` for numbers on your hardware.

## Explicit non-goals (design doc 20)

WaveSpan deliberately does **not** provide:

- SQL.
- Linearizable or serializable reads/writes.
- Serializable transactions.
- Globally-consistent scans.
- Exact TTL.
- Conflict-free active-active without a chosen policy (you pick HLC-LWW or keep-siblings).

## When should I *not* use WaveSpan?

If your workload needs strong consistency, multi-key serializable transactions, or globally-consistent scans, WaveSpan is the wrong tool — and it says so up front rather than failing surprisingly under load. It is built for geo-distributed, latency-sensitive, churn-tolerant workloads that can reason about eventual consistency with honest metadata.

---

*This documentation is rendered inside the WaveSpan node console, styled with the Linea/Olivetti design system. Use the navigation to explore each subsystem, or switch to the live tabs to inspect a running cluster.*
