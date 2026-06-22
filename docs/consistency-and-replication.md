# Consistency & replication

WaveSpan is **eventually consistent by default**. It deliberately does not run a global consensus
layer on the hot path. This page explains what that means and how durability, repair, and caching
work.

## The contract

WaveSpan favours:

- low local write latency;
- durability beyond a single pod (every acknowledged write is on ≥2 distinct nodes);
- fast repeated reads from nearby caches;
- graceful degradation during spot-node churn;
- convergence after partitions;
- explicit conflict handling and honest metadata.

It does **not** promise (by default): linearizable reads or writes, serializable transactions,
globally consistent range scans, or exact TTL deletion. Reads may be stale; scans may be partial.
Every response says which.

## Versions and conflict resolution

Every mutation carries a **hybrid logical clock (HLC) `Version`**: a 48-bit physical millisecond
component, a 16-bit logical counter, and the writer's cluster/member/sequence. Conflicts resolve
by **hlc-last-write-wins**, ordered by:

1. HLC physical, then 2. HLC logical, then 3. writer cluster id, 4. writer member id, 5. writer
sequence.

This is a deterministic total order: every node, given the same set of versions, picks the same
winner regardless of arrival order. (Keep-siblings and CRDT policies are specified for global
active-active in a later milestone.)

## Origin+1 writes (ADR-0002)

A write is acknowledged when:

```
the origin pod durably stored the mutation
AND
at least one nearby durable replica durably stored it
```

The configured target replica count `N` is a **background convergence goal**, not the
acknowledgement threshold. This gives durable-beyond-one-pod writes at low latency.

Consequences you should know:

- a write **fails** if no nearby durable replica can be reached (durability is never faked) —
  unless `minAck=0` is configured for single-node/local development;
- there is a brief under-replication window between ACK and target-N fill — repair closes it;
- if both the origin and its first replica are lost before repair runs, that write can be lost.
  This is the explicit trade-off for low-latency eventual consistency.

## Holder directory and closest-holder reads

Each node advertises the keys it durably holds as a compact **bloom-filter holder summary**,
gossiped to peers. A read miss consults the local holder directory (built from those summaries) to
find likely holders and fetches from the closest by the latency graph — **never by broadcasting**.
A stale directory entry just triggers a fallback to another holder.

## Dynamic cache replicas (ADR-0003)

On a read miss a pod fetches the value, stores it as a **dynamic cache replica**, and subscribes to
the holder for live updates. Cache replicas:

- are **derived and disposable** — they never count toward write durability or target-N;
- are kept fresh by `SubscribeKey` streaming; on a stream gap they resync (refetch) rather than
  serve silently-stale data;
- are evicted when idle (or under cache pressure), and durable replicas are never touched by the
  cache evictor.

A read served from cache is tagged `LOCAL_DYNAMIC_CACHE` so clients know it may be slightly behind.

## Repair and spot-node churn

WaveSpan assumes pods vanish without warning (spot nodes). The **repair engine** continuously
restores under-replicated keys:

- a **severity priority queue** drains the most under-replicated keys first;
- a token-bucket rate limit and **churn backpressure** keep repair from amplifying instability when
  many nodes are flapping;
- when a holder dies, the keys it held are re-replicated onto surviving nodes — no manual action;
- repair never re-replicates an expired key unless a tombstone is needed for convergence.

`kv_under_replicated_keys_estimate` is the signal to watch: it should drain to zero after churn
stops. The target replica count is capped by the live cluster size, so a small cluster does not
churn forever chasing an unreachable target.

## What "eventual" means in practice

- After a write ACKs, other nodes converge to it as fanout, gossip, and (for caches) subscriptions
  propagate — typically sub-second on a healthy LAN, longer under partition.
- After a partition heals, anti-entropy and repair reconcile divergent state (global active-active
  anti-entropy lands in a later milestone).
- A range scan reflects what the contacted holders knew at scan time; only a `cache-complete` scan
  with a valid coverage certificate claims `COMPLETE`.
