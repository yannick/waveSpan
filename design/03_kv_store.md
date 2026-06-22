# 03. Key-value store

## Scope

The KV store supports:

- byte-string keys;
- byte-string values;
- point `put`, `get`, `delete`;
- compare-and-set as best-effort conditional write;
- range scans;
- lazy TTL;
- watches;
- dynamic cache replicas;
- active-active replication.

## Namespaces

A **namespace** is the KV store's logical keyspace and its unit of policy. Every operation takes
one — `Put(namespace, key, …)`, `Get(namespace, key, …)`, `Scan(namespace, …)` — and the record's
true identity is the pair **`(namespace, key)`**, not the key alone. The same user key in two
namespaces is two unrelated records.

### Identity and isolation

Records are physically scoped by namespace: the storage key is a **length-prefixed namespace
followed by the raw user key** (`internal/recordstore/encode.go`, `latestKey`/`dataKey`/`ttlKey`).
Two consequences follow:

- **Isolation.** Keys in different namespaces never collide and never appear in each other's scans.
- **Ordered, per-namespace scans.** Within a namespace user keys sort in natural byte order, so a
  range scan is a contiguous prefix scan over exactly that namespace
  (`namespacePrefix`, design/03 "Range scans").

Namespaces are the KV concept. The graph and vector stores have their own analogous scoping
(`graph_id` for graph entities, `collection` for vectors); this section is about KV.

### Per-namespace policy

A namespace is configured under `namespaces[]` (`internal/config/global.go`, `NamespaceConfig`).
Unlisted namespaces use the defaults. Four independent knobs:

| Knob | Meaning | Default | Detail |
|---|---|---|---|
| `conflictPolicy` | how concurrent versions of a key are resolved | `hlc-last-write-wins` | this doc, "Conflict handling" |
| `replicationFactor` | how widely a write spreads (see below) | cluster target-N | design/28 |
| `globalDurabilityRequired` | block the local write ACK when the cross-cluster out-log is full, so an accepted write is guaranteed queued for peers | `false` | design/06, design/28 |
| `hideExpiredOnRead` | filter expired records at read time for a tighter TTL bound | `false` | this doc, "TTL across clusters and strict namespaces" |

### Significance: the two replication axes

The namespace's `replicationFactor` controls **two orthogonal things** — how widely a key spreads
*inside* its cluster, and whether it *crosses* to peer clusters
(`NamespaceConfig.ReplicateEverywhere`/`Scope`, design/28):

| Axis | Question it answers | Set by |
|---|---|---|
| **intra-cluster spread** | how many nodes of *this* cluster hold the key? | `replicationFactor` |
| **global boundary** | does the write leave this cluster for peers? | `replicationFactor` + global config (design/06) |

`replicationFactor` values:

| Value | Intra-cluster spread | Global boundary |
|---|---|---|
| `""` (default) | origin + target-N nearby durable replicas | ships to peers **iff** global replication is enabled |
| `"N"` (e.g. `"5"`) | origin + N nearby (override the target holder count) | same as default |
| `"all"` | **every node of the current cluster** | **never** crosses to peer clusters (local-only) |
| `"global"` | **every node of the current cluster** | **always** ships to **every** peer cluster |

The useful mental model: `all` = "everywhere locally, nowhere globally"; `global` = "everywhere
locally *and* in every cluster". Both also make a **joining node backfill the full set** on
bootstrap (design/28 "Node sync"), and both cost ~Nx the *background* replication work — so reserve
them for **small, slowly-changing reference data** (feature flags, config), not hot keys.

### Significance for cluster replication

For a normal namespace the namespace doesn't change *placement* — the coordinator still picks
origin + nearby durable replicas over the latency graph — it only changes the **target count**
(`replicationFactor: "N"`) or forces full-cluster spread (`all`/`global`). Repair and the
under-replication estimate use the namespace's effective target, so an `all` namespace is "under
replicated" until every alive node holds the key (design/28 "Write path").

### Significance for global (cross-cluster) replication

Across clusters the namespace decides three things (design/06):

1. **Whether writes ship at all.** `global` always ships; `all` never does; default follows the
   cluster's global-replication config.
2. **How cross-cluster conflicts resolve.** `conflictPolicy` is applied per namespace when a peer
   mutation is applied: `hlc-last-write-wins` collapses concurrent writes to one deterministic
   winner; `keep-siblings` preserves both and a `Get` reports `SIBLINGS_PRESENT` for the client to
   resolve.
3. **Whether acceptance implies global durability.** With `globalDurabilityRequired=true` the local
   ACK is withheld if the per-peer out-log is full, so "write accepted" means "queued for every
   peer" rather than merely "locally durable".

### Configuration

```yaml
namespaces:
  - name: default                       # implicit; LWW, target-N, ships iff global enabled
  - name: flags
    replicationFactor: global           # every node of every cluster (reference data)
  - name: cluster-cache
    replicationFactor: all              # every node of THIS cluster, never leaves it
  - name: hot
    replicationFactor: "5"              # override the nearby target for this namespace
  - name: siblings
    conflictPolicy: keep-siblings       # preserve concurrent writes as siblings
  - name: ledger
    globalDurabilityRequired: true      # block ACK until the write is queued for peers
```

Env equivalents exist for dev/compose: `WAVESPAN_KEEP_SIBLINGS_NAMESPACES`,
`WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES` (= `all`), and `WAVESPAN_REPLICATE_GLOBAL_NAMESPACES`
(= `global`) — see `internal/config/global.go`.

## Key ordering

Keys are lexicographically ordered byte arrays.

All APIs must treat keys as bytes. Higher-level encodings may be provided by clients.

## Public operations

```text
Put(namespace, key, value, ttl?, options?) -> PutResult
Get(namespace, key, options?) -> GetResult
Delete(namespace, key, options?) -> DeleteResult
Scan(namespace, start_key, end_key, limit?, options?) -> stream ScanResult
Watch(namespace, key_or_range, options?) -> stream WatchEvent
CompareAndSet(namespace, key, expected_version, value, ttl?, options?) -> CasResult
```

## Default operation semantics

| Operation | Default semantics |
|---|---|
| `Put` | Eventual. ACK after origin durable + one nearby durable replica. |
| `Get` | Eventual. Local first, then closest known holder. May create dynamic cache replica. |
| `Delete` | Eventual tombstone write. ACK after origin durable + one nearby durable replica. |
| `Scan` | Eventual. May use cache. Returns completeness metadata. |
| `Watch` | Best-effort subscription stream. Gaps require refetch. |
| `CAS` | Best-effort conditional at serving coordinator. Not globally linearizable. |

## Put path

```text
1. Receive Put at any data pod.
2. Assign version using local HLC and writer sequence.
3. Persist versioned record to local WavesDB.
4. Append mutation log entry.
5. Select nearby candidates from latency graph.
6. Send StoreReplica to candidates.
7. Return success after at least one candidate durably stores the mutation.
8. Continue fanout until targetNearbyReplicaCount is reached.
9. Notify local dynamic subscribers.
10. Enqueue global replication if enabled.
```

## Get path

```text
1. Check local latest pointer.
2. If acceptable, return local value.
3. If missing or caller requests fresher version, resolve holders.
4. Pick closest holder by latency graph.
5. Fetch value and metadata.
6. Store fetched value as dynamic cache copy if enabled.
7. Subscribe to future updates if policy allows.
8. Return value plus freshness metadata.
```

## Delete path

Deletes are tombstones.

```text
Delete(key) = Put(key, tombstone=true)
```

Tombstones must replicate and participate in conflict resolution. Physical deletion is delayed.

## Range scans

Range scans are explicitly eventually consistent.

Supported scan modes:

| Mode | Behavior |
|---|---|
| `cache-fast` | Scan local cache and local durable copies only. Fastest. May be incomplete. |
| `cache-complete` | Use local cache only if it has range coverage certificate. Else fall back. |
| `routed-eventual` | Query known holders for subranges and merge results. More complete but still eventual. |
| `local-only` | Only local WavesDB. Useful for debugging and local analytics. |

Default mode:

```yaml
scanMode: cache-fast
```

Every scan result stream must include a header:

```protobuf
message ScanHeader {
  ScanMode mode = 1;
  Completeness completeness = 2;
  repeated RangeCoverage coverage = 3;
  optional Version low_watermark = 4;
  optional Version high_watermark = 5;
}

enum Completeness {
  COMPLETENESS_UNKNOWN = 0;
  COMPLETE = 1;
  PARTIAL = 2;
  BEST_EFFORT = 3;
}
```

Do not silently return partial cache scans as complete scans.

## Cache range coverage certificate

A local pod can claim complete cache coverage for `[start, end)` only if it has an active range subscription or recent full snapshot.

```protobuf
message RangeCoverageCertificate {
  string namespace = 1;
  bytes start_key = 2;
  bytes end_key = 3;
  string owner_member_id = 4;
  uint64 owner_epoch = 5;
  Version high_watermark = 6;
  int64 valid_until_unix_ms = 7;
}
```

If certificate expires, scan must downgrade to `best_effort` or fetch.

## TTL semantics

TTL is approximate.

Write:

```text
expires_at = local_hlc_physical + ttl
```

Read:

- default: expired records may be hidden if detected;
- strict option: `hideExpiredOnRead=true` hides detected expired records;
- no promise that all nodes detect expiration at the same time.

Physical GC:

- background sweeper scans TTL buckets;
- emits tombstone mutations;
- eventually compacts obsolete versions.

### Observable staleness bound

An expired record may remain visible after its `expires_at` for a bounded window. The
maximum observable staleness for lazy TTL is:

```text
maxExpiredVisibility = bucketSize + sweepInterval + replicationLag
```

- `bucketSize` is the TTL bucket granularity (`ttl.bucketSeconds`, default `60s`): the
  sweeper acts at bucket boundaries, so detection can trail real expiry by up to one
  bucket;
- `sweepInterval` is the period between sweeper passes over the buckets;
- `replicationLag` is the time for the resulting tombstone to reach a given holder.

On a single up-to-date holder the bound collapses to `bucketSize + sweepInterval`;
`replicationLag` accounts for holders that have not yet applied the tombstone.

### TTL across clusters and strict namespaces

Remote clusters use the origin `expires_at` carried in the mutation and **never recompute
TTL from apply time**. A record received late still expires at the same wall-clock instant
it would have on the origin, so apply-time skew does not extend its lifetime.

For namespaces that cannot tolerate the staleness window, set `hideExpiredOnRead=true`.
This forces read-time filtering: a record whose `expires_at` is in the past is hidden on
read even if the sweeper has not yet produced a tombstone for it. This trades a small read
cost for a tighter expiry guarantee; the physical GC bound above still governs when the
record is actually removed.

## Conflict handling in KV

Each key can have multiple concurrent versions.

Default namespace policy:

```yaml
conflictPolicy: hlc-last-write-wins
```

Other policies:

```yaml
conflictPolicy: keep-siblings
conflictPolicy: crdt-counter
conflictPolicy: crdt-set
conflictPolicy: app-resolver
```

For `hlc-last-write-wins`, choose winner by:

1. highest HLC physical time;
2. highest HLC logical counter;
3. lexicographic writer cluster ID;
4. lexicographic writer member ID;
5. writer sequence.

This is deterministic. It is not semantically safe for every workload.

## Compare-and-set semantics

`CompareAndSet` is **best-effort at the coordinator**. It is evaluated against the
coordinator's local latest pointer at commit time. It is **not linearizable** and there
is **no global compare path in v1**.

The coordinator compares `expected_version` against its local latest pointer:

```text
1. resolve local latest pointer for key;
2. if local latest == expected_version, apply the write through the normal Put path;
3. otherwise reject with the observed local version.
```

Because the decision uses only local state, a concurrent winning write committed on
another pod that has **not yet been applied locally** can make the CAS decision wrong:
the coordinator may accept a CAS that a globally-fresh observer would reject, or reject a
CAS whose expectation actually still holds elsewhere. This is the explicit race; callers
that need strict conditional semantics must not rely on CAS in v1.

To make this race observable, the response carries `cas_conflict_window`:

```protobuf
message CasResult {
  bool applied = 1;
  Version observed_version = 2;        // coordinator-local latest at decision time
  Version applied_version = 3;         // set when applied == true
  bool cas_conflict_window = 4;        // decision may be racing concurrent state
  ResponseMeta meta = 5;
}
```

The coordinator sets `cas_conflict_window = true` when, at decision time, either:

- it holds unapplied inbound replication mutations for the key
  (pending `repl/global/in` entries not yet merged into the latest pointer); **or**
- the key currently has unmerged sibling state under a `keep-siblings` policy.

When `cas_conflict_window = true`, the CAS result reflects a snapshot that is known to be
racing other state. Clients should treat such a result as advisory and re-read, rather
than as a durable conditional guarantee.

## Watch API

Watches use mutation logs and cache subscriptions.

Watch event:

```protobuf
message WatchEvent {
  bytes key = 1;
  bytes value = 2;
  Version version = 3;
  bool tombstone = 4;
  optional int64 expires_at_unix_ms = 5;
  bool gap = 6;
}
```

If `gap=true`, client must refetch.

## Response metadata

All KV responses include:

```protobuf
message ResponseMeta {
  string served_by_member_id = 1;
  string served_by_cluster_id = 2;
  ReadSource source = 3;
  Version observed_version = 4;
  ConflictState conflict_state = 5;
  Completeness completeness = 6;
  optional int64 observed_at_unix_ms = 7;
}
```

## Implementation checklist

- [ ] Key encoder preserves lexicographic ordering.
- [ ] Version envelope implemented for all writes.
- [ ] Local latest pointer updated on writes and conflict resolution.
- [ ] Put ACK waits for origin + one nearby replica.
- [ ] Get local-fast path implemented.
- [ ] Holder lookup implemented without broadcast.
- [ ] Dynamic cache enrollment implemented.
- [ ] Range scan modes implemented with completeness metadata.
- [ ] TTL buckets and sweeper implemented.
- [ ] Tombstone replication implemented.
- [ ] Conflict policy plug-in interface implemented.

