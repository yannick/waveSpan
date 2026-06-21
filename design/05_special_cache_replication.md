# 05. Special local cache replication mode

## Purpose

This mode gives low-latency writes and progressively improves read locality.

When a key is written on a pod:

1. store it durably on the origin pod;
2. replicate it to nearby pods on distinct nodes;
3. acknowledge after at least one nearby durable replica succeeds;
4. continue replicating until target `N` nearby replicas exist;
5. do not send to other geo unless policy allows or global replication is separately enabled.

When a key is read and not available locally:

1. find the closest known holder;
2. fetch from that holder;
3. store locally as dynamic cache replica;
4. subscribe to future updates;
5. serve future reads locally while the subscription is healthy enough for the requested read mode.

## Replica types

| Type | Created by | Durable | Counts for write ACK | Receives updates | Can be evicted |
|---|---|---:|---:|---:|---:|
| Origin copy | local write coordinator | Yes | Yes | Yes | No, until repaired elsewhere |
| Nearby durable replica | write fanout / repair | Yes | Yes | Yes | Only by repair/rebalance |
| Dynamic cache replica | read miss | Usually yes on disk, but derived | No | Yes while subscribed | Yes |
| Range cache replica | range scan/watch | Usually yes on disk, but derived | No | Yes while subscribed | Yes |
| Global remote copy | global replication | Yes | No for local write | Via global stream | Policy-dependent |

## Policy object

```yaml
apiVersion: db.Wavespan.io/v1
kind: ReplicationPolicy
metadata:
  name: local-cache-default
spec:
  mode: local-cache
  local:
    targetNearbyReplicaCount: 3
    minAckNearbyReplicas: 1
    requireDistinctNodes: true
    geoPolicy: prefer-local-geo
    maxReplicaRttMs: 15
    allowSpilloverForDurability: true
  cache:
    dynamicSubscriptions: true
    cacheReadMode: eventual
    maxSubscribersPerKey: 1024
    maxDynamicCacheBytesPerPod: 200Gi
    idleSubscriptionTtlSeconds: 600
    maxSubscriptionLagMs: 5000
    allowRangeCache: true
```

## Write coordinator

The receiving pod is the write coordinator for that mutation.

The coordinator does not need to be a stable owner. This fits active-active and eventual consistency.

Coordinator responsibilities:

- assign version;
- persist local copy;
- append mutation log;
- pick nearby durable replicas;
- wait for one durable remote acknowledgement;
- fan out to target `N`;
- register holder records;
- notify dynamic subscribers known to this coordinator;
- enqueue global replication if enabled.

## Write state machine

```text
RECEIVED
  -> LOCAL_PERSISTING
  -> LOCAL_DURABLE
  -> REPLICATING_MIN_ACK
  -> ACKABLE
  -> FANOUT_TARGET_N
  -> SUBSCRIBER_NOTIFY
  -> GLOBAL_ENQUEUE
  -> COMPLETE
```

Failure paths:

```text
LOCAL_PERSISTING failure -> FAIL
REPLICATING_MIN_ACK timeout -> RETRY_CANDIDATES
RETRY_CANDIDATES exhausted -> FAIL unless policy allows degraded-local-only writes
FANOUT_TARGET_N failure -> ACK already returned; schedule repair
SUBSCRIBER_NOTIFY failure -> mark subscriber lagging
GLOBAL_ENQUEUE failure -> retry from mutation log
```

Default does not allow degraded-local-only writes because the user requirement says at least one replication is required.

## Write algorithm

Pseudocode:

```go
func (c *Coordinator) Put(ctx context.Context, req PutRequest) (PutResponse, error) {
    version := c.versionClock.Next(req.Key)
    record := NewStoredRecord(req.Key, req.Value, version, req.TTL)

    // Atomic local commit: one wavesdb Txn across the data, meta, log and TTL CFs.
    if err := c.local.Batch(
        PutVersionedRecord(record),
        PutLatestPointer(record),
        AppendMutationLog(record),
        PutTTLBucketIfNeeded(record),
    ); err != nil {
        return PutResponse{}, err
    }

    candidates := c.placement.SelectNearbyReplicas(SelectOpts{
        Key:                 req.Key,
        Target:              c.policy.Local.TargetNearbyReplicaCount,
        RequireDistinctNodes: true,
        GeoPolicy:           c.policy.Local.GeoPolicy,
    })

    acked := 0
    for r := range c.replicateParallel(ctx, candidates, record, c.policy.WriteTimeout) {
        if r.Err == nil {
            acked++
            c.holderDir.RecordHolder(req.Key, r.Member, record.Version)
            if acked >= c.policy.Local.MinAckNearbyReplicas {
                break
            }
        }
    }

    if acked < c.policy.Local.MinAckNearbyReplicas {
        return PutResponse{}, ErrInsufficientNearbyReplicas
    }

    c.background.EnqueueFillTargetN(req.Key, record)
    c.background.EnqueueNotifySubscribers(req.Key, record)
    c.background.EnqueueGlobalIfEnabled(record)

    return PutResponse{Version: version, AckedNearbyReplicas: acked}, nil
}
```

## Candidate selection

Candidate filter:

```text
peer != self
peer.liveness in {ALIVE, SUSPECT_RECENT? only if policy allows}
peer.node_name != self.node_name
peer.accepting_writes == true
peer.disk_pressure < threshold
peer.replication_queue_depth < threshold
peer.satisfies_compliance_boundary == true
```

Then score by latency graph:

```text
placement_score = latency_score + topology_penalty + load_penalty + repair_balance_penalty
```

Doc 00 summarizes the policy-mode contract. This section specifies how the write
coordinator selects candidates and decides whether to spill.

For `prefer-local-geo`:

1. select same-geo candidates, ordered by the latency-graph placement score;
2. attempt to satisfy `minAckNearbyReplicas` from same-geo candidates within the
   remaining `writeTimeout` budget, retrying alternate same-geo candidates on failure;
3. only if same-geo candidates are exhausted before `minAckNearbyReplicas` is reached,
   spill to the **nearest allowed geo** — and only when
   `allowSpilloverForDurability=true`;
4. when a spilled replica is used, tag the write response and metrics with
   `geoSpillover=true`;
5. if spillover is disabled (`allowSpilloverForDurability=false`) and same-geo candidates
   cannot satisfy the ack, fail like `require-local-geo` rather than crossing the geo.

Spillover always picks the nearest allowed geo by latency, never a broadcast; the spilled
replica is a real nearby durable replica and counts toward the ack and toward target `N`.

For `require-local-geo` (compliance):

1. consider **only** candidates inside the configured compliance boundary;
2. attempt `minAckNearbyReplicas` with bounded retry over alternate in-boundary candidates
   within `writeTimeout`;
3. on timeout or candidate exhaustion, **fail** the write with `InsufficientLocalReplicas`
   — never spill to another geo, even for durability.

```text
prefer-local-geo  + allowSpilloverForDurability=true   -> retry same-geo, then spill (tag geoSpillover=true)
prefer-local-geo  + allowSpilloverForDurability=false   -> retry same-geo, then fail InsufficientLocalReplicas
require-local-geo                                       -> retry in-boundary, then fail InsufficientLocalReplicas
```

`require-local-geo` is the hard-compliance path: it treats the geo boundary as a
correctness constraint, so a durability shortfall is surfaced as a failure rather than
silently satisfied across the boundary.

## StoreReplica protocol

```protobuf
message StoreReplicaRequest {
  string namespace = 1;
  bytes key = 2;
  StoredRecord record = 3;
  ReplicaClass replica_class = 4; // NEARBY_DURABLE
  string coordinator_member_id = 5;
  string mutation_id = 6;
}

message StoreReplicaResponse {
  bool durable = 1;
  string member_id = 2;
  Version applied_version = 3;
  ConflictState conflict_state = 4;
}
```

Receiver rules:

1. validate policy and namespace;
2. compare incoming version with local versions;
3. apply conflict policy;
4. store versioned record and latest pointer;
5. append mutation log with `source=local-replica`;
6. acknowledge only after durable local store.

## Dynamic cache read path

```text
GET key on Pod X
  local lookup miss
  resolve holders from local holder directory
  if uncertain, ask range directory peers
  choose closest holder by latency graph
  FetchReplica from holder
  store local dynamic cache copy
  open subscription to update source
  return value
```

## FetchReplica protocol

```protobuf
message FetchReplicaRequest {
  string namespace = 1;
  bytes key = 2;
  optional Version min_version = 3;
  bool want_subscription_offer = 4;
}

message FetchReplicaResponse {
  bool found = 1;
  StoredRecord record = 2;
  repeated string alternate_holder_member_ids = 3;
  optional SubscriptionOffer subscription_offer = 4;
}
```

If holder is stale, it should return alternate holders when known.

## Subscription protocol

Dynamic cache replicas subscribe to the best available update source.

Subscription source preference:

1. latest write coordinator if alive;
2. nearby durable replica with freshest known version;
3. range summary owner;
4. global local applier for globally replicated keys.

Subscription request:

```protobuf
message SubscribeKeyRequest {
  string namespace = 1;
  bytes key = 2;
  Version from_version = 3;
  string subscriber_member_id = 4;
  int64 requested_lease_ms = 5;
}

message SubscribeKeyResponse {
  string subscription_id = 1;
  int64 lease_until_unix_ms = 2;
  Version current_version = 3;
  bool snapshot_required = 4;
}
```

Update event:

```protobuf
message CacheUpdate {
  string subscription_id = 1;
  bytes key = 2;
  StoredRecord record = 3;
  uint64 stream_sequence = 4;
  bool snapshot_required = 5;
}
```

Ack:

```protobuf
message CacheUpdateAck {
  string subscription_id = 1;
  uint64 stream_sequence = 2;
  Version applied_version = 3;
}
```

## Subscription state machine

```text
INACTIVE
  -> SNAPSHOTTING
  -> ACTIVE
  -> LAGGING
  -> RESYNCING
  -> ACTIVE
  -> EXPIRED
  -> EVICTED
```

Transitions:

| From | To | Trigger |
|---|---|---|
| `ACTIVE` | `LAGGING` | missed sequence or lag over threshold |
| `LAGGING` | `RESYNCING` | source requests snapshot |
| `RESYNCING` | `ACTIVE` | snapshot applied and stream resumed |
| `ACTIVE` | `EXPIRED` | idle lease not renewed |
| any | `EVICTED` | cache pressure |

## Cache eviction

Evict dynamic caches by score:

```text
eviction_score =
    idle_time_weight
  + size_weight
  + lag_weight
  + low_hit_rate_weight
  + update_fanout_cost_weight
```

Do not evict nearby durable replicas with the dynamic cache evictor.

## Update propagation

On new mutation:

1. apply locally;
2. replicate to nearby durable peers;
3. append to mutation log;
4. notify active subscribers;
5. subscribers apply if version wins or creates sibling;
6. subscribers ack stream sequence;
7. source drops slow subscribers after threshold.

A missed update is not fatal. Subscriber performs fetch/resync.

## Dead-subscriber detection

The update source must not stream into the void forever. For every subscription it tracks,
per subscriber:

```text
last_acked_seq      // highest stream_sequence the subscriber has acked
lease_until         // current lease deadline (renewed by acks / keepalives)
unacked_pushes      // consecutive pushes sent with no ack
```

The source **drops** a subscriber, releasing its stream state, when any of:

```text
lag > maxSubscriptionLagMs                          // ack lag past the configured bound
now > lease_until                                   // lease expired (idleSubscriptionTtl)
unacked_pushes >= maxConsecutiveUnackedPushes       // N consecutive unacked pushes
```

`maxSubscriptionLagMs` and `idleSubscriptionTtlSeconds` come from the cache policy;
`maxConsecutiveUnackedPushes` defaults to a small N (e.g. 8). Lag is measured as the time
between the source's newest `stream_sequence` and the subscriber's `last_acked_seq`.

A dropped subscriber is not an error for the source: it simply stops pushing and reclaims
the slot against `maxSubscribersPerKey`. The subscriber, if still alive, observes the drop
(stream close or a later push gap) and re-subscribes through the normal
`INACTIVE -> SNAPSHOTTING -> ACTIVE` path. This closes the "streams into the void forever"
gap: a crashed or partitioned subscriber is reclaimed within
`min(maxSubscriptionLagMs, idleSubscriptionTtl)` rather than held indefinitely.

Emit:

```text
cache_subscriptions_dropped_total   // labeled by reason: lag | lease_expired | unacked
```

## Range cache

Range cache is created by:

- repeated range scans;
- explicit `WatchRange`;
- prewarm policy.

Range subscription request:

```protobuf
message SubscribeRangeRequest {
  string namespace = 1;
  bytes start_key = 2;
  bytes end_key = 3;
  Version from_watermark = 4;
  string subscriber_member_id = 5;
}
```

A range cache can serve `cache-complete` scans only when it holds a valid coverage certificate.

## Repair loop

Repair continuously enforces target nearby replica count.

Inputs:

- holder summaries;
- dead/suspect members;
- mutation logs;
- random key sampling;
- TTL tombstones;
- global replication lag.

Repair action:

```text
if durable_holder_count(key) < targetNearbyReplicaCount + originSurvivor:
    select new nearby candidate
    copy latest winning record and siblings/tombstone
    update holder directory
```

Do not repair expired keys unless tombstone is needed for conflict convergence.

## Metrics

Required metrics:

```text
kv_write_origin_plus_one_latency_ms
kv_write_min_ack_failures_total
kv_write_target_n_fill_lag_ms
kv_geo_spillover_total
kv_dynamic_cache_hits_total
kv_dynamic_cache_misses_total
kv_cache_subscription_lag_ms
kv_cache_subscriptions_active
cache_subscriptions_dropped_total
kv_cache_evictions_total
kv_range_cache_coverage_count
kv_under_replicated_keys_estimate
```

## Implementation checklist

- [ ] Origin+1 write acknowledgement implemented.
- [ ] Target-N asynchronous fanout implemented.
- [ ] Geo policy modes implemented.
- [ ] Latency graph candidate selection implemented.
- [ ] Dynamic cache fetch path implemented.
- [ ] Key subscription stream implemented.
- [ ] Range subscription stream implemented.
- [ ] Cache eviction implemented.
- [ ] Repair loop implemented.
- [ ] Holder directory avoids broadcast.
- [ ] All read/scan responses expose freshness/completeness metadata.

