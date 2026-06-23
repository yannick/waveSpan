---
title: Replicated Collections
section: Reference
order: 9.5
summary: Sets, hash tables, and sorted sets on a strongly-consistent multi-Raft tier — written from a central point, read fleet-wide. The RPC API, SDK, Cypher built-ins, and admin tab.
---

# Replicated Collections

Replicated collections are **sets, hash tables, and sorted sets** held on a strongly-consistent
**consensus tier** that runs alongside the eventually-consistent KV cache. They are meant for
distributing application data across the fleet **from a central point**: writes are linearizable
through a Raft leader, and every node can read the data — locally and cheaply.

This is a different contract from the [KV API](doc:kv-api), which is an AP cache optimised for
local-node writes and tunable durability. Use the KV cache for high-volume local caching; use
collections when many nodes must agree on the same set/map/leaderboard. The two tiers share the same
`wavesdb` store but never touch each other's hot paths. The full design is in `design/30`.

> **Configuring it.** The tier is **enabled by default** (a single node is its own voter, so it works
> out of the box). Environment variables tune it:
> - `WAVESPAN_COLLECTIONS_ENABLED=0` — turn the tier **off** (the RPC/Cypher/UI surfaces then return
>   *"collections backend not configured"*).
> - `WAVESPAN_COLLECTIONS_RAFT_ADDR` — this node's Raft transport address (default: data port + 1000).
>   Raft traffic rides the cluster's mTLS.
> - `WAVESPAN_COLLECTIONS_VOTERS` — comma-separated Raft addresses of the **stable core** (voters);
>   `replicaID = index + 1`. Each listed node bootstraps the meta + data shards with this voter set.
>   Leave it unset on a single node; set it to form a multi-node stable core.
>
> **Consensus tunables** (each falls back to its default when unset):
> - `WAVESPAN_COLLECTIONS_RTT_MS` — base Raft RTT unit in ms (default `50`); election and heartbeat
>   timeouts are multiples of it.
> - `WAVESPAN_COLLECTIONS_ELECTION_RTT` — election timeout in RTT units (default `10` → 500 ms). Lower
>   it for faster failover at the cost of more election churn.
> - `WAVESPAN_COLLECTIONS_HEARTBEAT_RTT` — leader heartbeat in RTT units (default `1`).
> - `WAVESPAN_COLLECTIONS_SNAPSHOT_ENTRIES` — log entries between snapshots (default `1000`); smaller
>   means faster learner catch-up but more snapshot I/O.
> - `WAVESPAN_COLLECTIONS_COMPACTION_OVERHEAD` — entries retained after a snapshot (default `500`).
> - `WAVESPAN_COLLECTIONS_SWEEP_MS` — TTL sweep interval on shard leaders in ms (default `500`).
>   Empty ⇒ single-node. A node **not** in the list is a **spot node**: it holds no shards, joins the
>   meta shard as a learner in the background to obtain the directory, then serves collections by
>   demand-filling each one's data shard on first read.

## Data model

A **collection** is addressed by a `(namespace, collection)` pair, like the KV API. Each collection has
a fixed **type** — set, hash, or sorted set — recorded on first write. A mutation that targets a
collection of a different type fails with `WRONGTYPE` (`FailedPrecondition` over RPC). Elements are
arbitrary bytes.

| Type | Holds | Key operations |
|------|-------|----------------|
| **Set** | unique members | `SAdd`, `SRem`, `SIsMember`, `SCard`, `SMembers` |
| **Hash** | field → value map | `HSet`, `HDel`, `HGet`, `HLen`, `HGetAll`, `HIncrBy`, `HIncrByFloat` |
| **Sorted set** | members ordered by score | `ZAdd`, `ZRem`, `ZScore`, `ZCard`, `ZRange` |

Cross-collection: `BulkRemove` deletes a list of members from many collections (or all in a namespace)
in one call.

Cardinality is **exact** — maintained by the single writer, not estimated. Set members support a
per-element **TTL** (`SAddTTL`): the absolute expiry is stamped before the write is proposed, and the
shard leader sweeps expired elements via committed log entries, so every replica expires them
identically.

## Consistency

- **Writes** are linearizable: routed to the owning shard's Raft leader and acknowledged after commit.
- **Reads** default to **bounded-stale local reads** — served from the nearest replica without a
  quorum round-trip. Pass `linearizable: true` to force a quorum read when you need read-your-writes
  across nodes.

Every RPC reply carries `ResponseMeta` (which node served it, the source, completeness) so the
consistency mode is never hidden.

## RPC API

`CollectionService` is defined in `proto/wavespan/v1/collections.proto` and exposed over Connect
(HTTP/2, mTLS) on the data port — the same transport as the KV and Cypher services.

## Go SDK

The SDK exposes the tier through `Client.Collections()`:

```go
c, _ := wavespan.Dial(wavespan.Options{Endpoint: "localhost:7800"})
defer c.Close()
col := c.Collections()

// Set
col.SAdd(ctx, "flags", []byte("enabled"), []byte("feature-x"), []byte("feature-y"))
ok, _ := col.SIsMember(ctx, "flags", []byte("enabled"), []byte("feature-x"), false)

// Set with a TTL (members expire after the duration)
col.SAddTTL(ctx, "sessions", []byte("active"), 30*time.Minute, []byte("user-42"))

// Hash
col.HSet(ctx, "profile", []byte("u1"), wavespan.FieldValue{Field: []byte("name"), Value: []byte("Ada")})
name, found, _ := col.HGet(ctx, "profile", []byte("u1"), []byte("name"), false)

// Sorted set (leaderboard)
col.ZAdd(ctx, "scores", []byte("game-7"), wavespan.ScoredMember{Member: []byte("ada"), Score: 99})
top, _ := col.ZRange(ctx, "scores", []byte("game-7"), 10, false) // ascending score order
```

Writes are linearizable; reads take a `linearizable bool` (pass `false` for the fast bounded-stale
path). `WRONGTYPE` surfaces as a Connect `FailedPrecondition` error.

## Atomic counters

`HIncrBy` (integer) and `HIncrByFloat` (float) atomically add a delta to a numeric hash field and
return the **new value**. The whole read-add-write happens inside one Raft entry, so concurrent
increments are **exact — no lost updates**, unlike a read-then-write in application code. The value is
stored as a decimal string, so `HGet` returns it verbatim; incrementing a non-numeric field fails with
`InvalidArgument`.

```go
n, _ := col.HIncrBy(ctx, "metrics", []byte("page:home"), []byte("views"), 1)   // -> new int64
r, _ := col.HIncrByFloat(ctx, "metrics", []byte("page:home"), []byte("rate"), 0.5) // -> new float64
```

## Bulk member removal

`BulkRemove` deletes a list of members from **many collections at once** — a named list, or (when the
list is empty) **every collection in the namespace**. It is type-agnostic: each target collection's
actual type is honored (set → `SRem`, hash → `HDel`, sorted set → `ZRem`). Useful for fan-out cleanup
such as "remove this user from every set and hash in the namespace".

```go
// remove "user-42" from a named list of collections
res, _ := col.BulkRemove(ctx, "app", [][]byte{[]byte("admins"), []byte("online")}, [][]byte{[]byte("user-42")})

// remove "user-42" from EVERY collection in the namespace (empty collection list)
res, _ = col.BulkRemove(ctx, "app", nil, [][]byte{[]byte("user-42")})
for _, e := range res { /* e.Collection, e.Removed, e.Error */ }
```

It is **best-effort across shards**: each collection's change is atomic on its shard, the overall
fan-out is eventually-consistent (collections can live on different Raft groups), and a per-collection
result is returned so a partial failure is visible rather than hidden.

## Idempotency

Because node-side leader routing (below) can retry a write, and counters are not naturally
idempotent, a write may carry an **idempotency key**. The owning shard caches the result of a keyed
write and returns it unchanged on a retry, so the write applies **exactly once** — a re-sent
`HIncrBy` returns the original new value instead of incrementing twice.

```go
col.WithIdempotencyKey("req-7f3a").HIncrBy(ctx, "metrics", []byte("page:home"), []byte("views"), 1)
```

Use a fresh key per logical write. The cache is a bounded, replicated FIFO ring, so it covers the
retry window without growing unbounded.

## Calling any node

Writes must commit on the owning shard's **leader**, but a client may call **any node**: a node that
isn't the leader transparently **forwards** the write to the one that is (it caches the leader, so the
steady state is a single extra hop). The SDK therefore needs no leader discovery — point it at any
endpoint. Reads are served locally (bounded-stale) or via a read-index (linearizable) on any replica.

## From Cypher

The collections are also reachable from the [Cypher console](doc:cypher-and-graph) as built-ins,
alongside `kv.*`. Scalar functions read inline (usable in `WHERE`); procedures mutate or enumerate.

```cypher
// Gate a query on set membership
MATCH (u:User)
WHERE set.contains('flags', 'beta-users', u.id)
RETURN u.name

// Mutate
CALL set.add('flags', 'beta-users', 'u-42') YIELD added RETURN added
CALL hash.set('profile', 'u1', 'name', 'Ada') YIELD added RETURN added
CALL zset.add('scores', 'game-7', 'ada', 99) YIELD added RETURN added

// Enumerate
CALL zset.range('scores', 'game-7') YIELD member, score RETURN member, score
CALL hash.getAll('profile', 'u1') YIELD field, value RETURN field, value
```

Available built-ins: `set.contains` / `set.card` / `hash.get` / `zset.score` (functions); `set.add` /
`set.remove` / `set.members` / `hash.set` / `hash.getAll` / `zset.add` / `zset.range` (procedures).
Reads are bounded-stale.

## From the admin console

The **Collections** tab is a small operator console: pick a namespace, collection, and type, then add
or remove elements and list the contents with the live cardinality. It calls the same
`CollectionService`, mounted on the admin port for same-origin access.

Below the browser, a read-only **Consensus tier** panel shows the node's placement (voter or spot,
its replica id, Raft address), the active tunables, and a **per-shard leader table** — which shard
this node hosts, whether it has a leader, and whether this node currently leads it. It polls every few
seconds, so leadership changes show up live. The same data is available programmatically via the
`TierInfo` RPC (and `Collections().TierInfo(ctx)` in the Go SDK).

## Under the hood

Collections live on a **range-based multi-Raft** layout (design/30): the keyspace is partitioned into
ranges, each range is an independent Raft shard, and a **meta Raft group** holds the range directory
that routes a collection to its shard. The control plane supports **range split** and **merge**
(migrate-based, since shards are independent groups) and **learner demand-fill** — a node that is
asked for a collection it does not host can join that shard as a non-voting learner, stream its state,
and then serve local reads, forming a dynamically-filling read cache. Voters stay on the stable core;
learners can live anywhere and are evicted when cold.
