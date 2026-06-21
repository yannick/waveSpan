# 00. Assumptions and product contract

## User-confirmed requirements

| Area | Requirement |
|---|---|
| Write acknowledgement | A write must be replicated to at least one other nearby pod before success is returned. |
| Geo boundary | Avoid cross-geo local replication for latency/cost. Some namespaces may require hard compliance boundaries. |
| Closeness | Measured latency is more important than static topology labels. Build a latency graph through gossip. |
| KV consistency | Eventual consistency by default. |
| Range scans | Range scans may read from cache. |
| TTL | TTL does not need to be precise. |
| Vector search | Support approximate nearest-neighbor and exact search. |
| Query language | Use a production subset of Cypher with vector extensions. No SQL interface is required. |
| Graph/vector replication | Graph and vector data should use geo-replication features where it can be kept fast. |
| Global replication | Active-active with conflict resolution. |
| Tenancy | Single tenant. No hard tenant isolation required in v1. |
| Control plane | Production assumes Kubernetes. Docker must work for testing. |
| Persistence | Operator-owned persistent volumes in production. |
| Large values/vectors | Store in `wavesdb` for now. Object storage can be added later. |
| Failure model | Large Kubernetes clusters with many spot nodes. Spot nodes vanish often. Occasional intra-cluster downtime due to VPN/network restarts. |

## Product contract

WaveSpan provides an eventually consistent distributed data system optimized for low-latency local access, read-created cache replicas, and active-active geo replication.

The system favors:

- low local write latency;
- local durability beyond one pod;
- fast repeated reads from nearby caches;
- graceful degradation during spot-node churn;
- convergence after partitions;
- explicit conflict handling.

The system does not promise by default:

- linearizable reads;
- linearizable writes;
- serializable transactions;
- globally consistent range scans;
- exact TTL deletion time;
- conflict-free active-active writes without a declared merge policy.

## Default consistency vocabulary

| Term | Meaning |
|---|---|
| Eventual read | Returns the newest value known to the serving pod or fetched holder. May be stale. |
| Session read-your-writes | Optional client session token forces reads to wait for or fetch at least the version written by that session. |
| Best-effort cache scan | Range scan served partly or fully from local cache/indexes. May be incomplete. Must declare completeness. |
| Complete cache scan | Range scan served from a local cache only if the local pod has a range coverage certificate. |
| Authoritative scan | Range scan sent to known range owners/replicas to improve completeness. Still eventual unless a later strong mode is added. |

## Write acknowledgement rule

Default write success condition:

```text
local origin write is durable
AND
one nearby durable replica write is durable
```

Background convergence then attempts to reach:

```text
origin durable copy
+
N nearby durable copies
+
optional dynamic cache subscribers
+
optional global active-active streams
```

`N` is target fanout, not the success quorum.

## Geo policy modes

| Mode | Behavior |
|---|---|
| `prefer-local-geo` | Prefer same geo. May spill to the nearest allowed geo only under the bounded rule below. Default for latency/cost. |
| `require-local-geo` | Never replicate outside the configured local geo. Bounded retry, then fail. Use for compliance. |
| `latency-only` | Ignore geo labels. Choose by latency graph subject to node diversity and health. |
| `global-active-active` | Replicate local mutation logs to configured peer clusters asynchronously. |

### Compliance-spillover rule (concrete)

The origin pod tries to satisfy `origin + 1 nearby durable replica` from same-geo candidates first. When no same-geo candidate is reachable in time, the two modes diverge as follows. Both are bounded by `replication.writeTimeout` (default `2s`) — neither may block indefinitely.

`require-local-geo` (hard compliance boundary):

1. Retry same-geo candidate selection within a bounded loop until `writeTimeout` elapses (re-probing the latency graph and health between attempts).
2. If origin+1 is still not satisfiable inside the local geo when `writeTimeout` elapses, **fail the write** with `InsufficientLocalReplicas`.
3. Never replicate outside the local geo. Never spill. Never block past `writeTimeout`. The origin durable copy is retained, but the write is reported as failed and the caller must retry or relocate load.

`prefer-local-geo` (latency/cost preference, durability-protective):

1. Retry same-geo candidate selection within the same bounded loop until `writeTimeout` elapses.
2. If origin+1 is still not satisfiable inside the local geo, spill to the nearest allowed geo **only if** `localReplication.allowSpilloverForDurability=true`. The spilled replica is tagged `geoSpillover=true` so repair and observability can see and later relocate it once a same-geo candidate appears.
3. If `allowSpilloverForDurability=false`, behave like `require-local-geo` step 2 and fail with `InsufficientLocalReplicas` rather than crossing the geo boundary.

`allowSpilloverForDurability` defaults to `true` for `prefer-local-geo` and is ignored by `require-local-geo` (which never spills regardless of the flag). Spillover targets are still restricted to geos in the namespace's allowed-geo set; "nearest allowed geo" never means an arbitrary geo.

## Node closeness

Closeness is computed from:

1. measured pod-to-pod RTT;
2. packet loss and timeout rate;
3. recent connection health;
4. Kubernetes node identity and zone/region labels when available;
5. disk/load pressure penalty;
6. whether the peer is on a distinct node.

Static labels can break under VPNs, overlays, and spot churn. The latency graph is the source of truth.

## Spot-node assumption

The system must assume any pod can disappear with little or no warning. Therefore:

- every acknowledged write must be stored on at least two distinct nodes;
- repair workers must detect under-replicated keys and ranges quickly;
- dynamic caches must be disposable;
- active subscribers must tolerate missed updates and re-snapshot;
- node identity must include persistent storage UUIDs, not only pod names;
- a pod rescheduled with an empty volume must be treated as a new storage member.

