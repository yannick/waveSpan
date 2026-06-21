# 01. Architecture

## Goal

Build a distributed database where `wavesdb` is the embedded local engine and WaveSpan provides the distributed behavior.

WaveSpan is implemented entirely in Go. The data node, gateway, CLI, and Kubernetes operator are Go processes, and `wavesdb` is linked in-process as a Go library (no FFI). The C engine `tidesdb` is reference-only and does not appear in the build. See `adr/0005_go_and_wavesdb_engine.md`.

## High-level components

```text
                  +-------------------+
Client KV API --->| Gateway / Router  |----+
Client Cypher --->| Cypher Frontend    |    |
                  +-------------------+    |
                                               v
+----------------------------------------------------------------+
|                        WaveSpan data pods                       |
|                                                                |
|  +------------+  +------------+  +------------+  +------------+ |
|  | Data Pod A |  | Data Pod B |  | Data Pod C |  | Data Pod D | |
|  | wavesdb    |  | wavesdb    |  | wavesdb    |  | wavesdb    | |
|  | Gossip     |  | Gossip     |  | Gossip     |  | Gossip     | |
|  | KV/Graph   |  | KV/Graph   |  | KV/Graph   |  | KV/Graph   | |
|  | Vector     |  | Vector     |  | Vector     |  | Vector     | |
|  +------------+  +------------+  +------------+  +------------+ |
+----------------------------------------------------------------+

+-------------------+        +---------------------+
| Kubernetes        |        | WaveSpan operator   |
| StatefulSets/PVs  |<------>| CRDs/Reconcile      |
| Services/labels   |        | Upgrades/repair     |
+-------------------+        +---------------------+

+----------------------------------------------------+
| Optional peer Kubernetes clusters                   |
| Active-active mutation log replication              |
+----------------------------------------------------+
```

## Component responsibilities

### Data pod

A data pod is the stateful unit. It is a single Go process that runs:

- `wavesdb` local engine, opened in-process via the `internal/storage` `LocalStore` adapter;
- key-value API handler;
- range scanner;
- TTL sweeper;
- local replication sender/receiver;
- dynamic cache manager;
- subscription update stream;
- membership and gossip agent;
- latency graph builder;
- graph storage adapter;
- Cypher execution fragments;
- vector raw store;
- vector index workers;
- global replication log sender/receiver;
- metrics and tracing server.

### Gateway

The gateway is optional. Clients may connect directly to data pods in simple deployments, but gateways are useful in production.

Gateway responsibilities:

- authentication and authorization;
- request routing;
- routing-cache maintenance;
- Cypher parsing and planning;
- scatter/gather for distributed graph/vector queries;
- admission control;
- response metadata normalization.

Gateways do not own authoritative data.

### Operator

The operator owns production deployment:

- StatefulSet creation;
- PVC templates;
- headless Services;
- public Services;
- pod disruption budgets;
- topology spread constraints;
- TLS secrets;
- CRD reconciliation;
- rolling upgrades;
- backup/restore orchestration;
- scale-up/scale-down orchestration;
- cluster-peer configuration.

The operator does not decide runtime write success. Runtime data-plane logic does.

### Runtime membership layer

WaveSpan maintains its own membership layer. Kubernetes provides inputs, but the runtime membership layer must also work under Docker.

Runtime membership tracks:

- member ID;
- storage UUID;
- cluster ID;
- node name;
- advertised addresses;
- health;
- latency graph edges;
- disk pressure;
- cache pressure;
- known key/range ownership summaries.

## Data model layers

```text
KV layer
  ordered byte keys, values, TTL, range scans, watches

Graph layer
  property graph encoded into ordered keyspace

Vector layer
  vector properties attached to graph entities or direct vector collections

Replication layer
  local origin+1 writes, target-N repair, dynamic cache subscriptions

Global layer
  active-active asynchronous mutation-log replication

Storage layer
  wavesdb local persistence (in-process Go library)
```

## Consistency design

WaveSpan uses eventual consistency by default.

Key ideas:

1. Any data pod can accept a write.
2. The accepting pod becomes the write coordinator for that mutation.
3. The coordinator writes locally and replicates to at least one nearby durable peer before acknowledging.
4. The coordinator and repair workers asynchronously fill the configured nearby target replica count.
5. Conflicting writes are detected by version metadata and resolved by policy.
6. Caches subscribe to updates, but missed updates are allowed. They recover with fetch or snapshot.
7. Global replication streams mutation logs asynchronously and resolves conflicts on apply.

## Internal keyspace

All user-facing models are encoded into ordered keys.

```text
/sys/cluster/{cluster_id}
/sys/member/{member_id}
/sys/range/{range_id}
/sys/latency/{from}/{to}
/sys/policy/{namespace}

/kv/{namespace}/data/{user_key}/{version}
/kv/{namespace}/meta/{user_key}
/kv/{namespace}/ttl/{bucket_ts}/{user_key}
/kv/{namespace}/holder/{range_id}/{key_hash}/{member_id}

/graph/{graph}/node/{node_id}
/graph/{graph}/label/{label}/{node_id}
/graph/{graph}/edge/out/{src}/{type}/{dst}/{edge_id}
/graph/{graph}/edge/in/{dst}/{type}/{src}/{edge_id}
/graph/{graph}/prop/{label}/{prop}/{encoded_value}/{node_id}

/vector/{collection}/raw/{vector_id}
/vector/{collection}/meta/{vector_id}
/vector/{collection}/ann/{index_id}/{segment_id}/...
/vector/{collection}/delta/{index_id}/{seq}

/repl/local/log/{partition}/{seq}
/repl/global/out/{peer_cluster}/{partition}/{seq}
/repl/global/in/{origin_cluster}/{partition}/{seq}
/cache/sub/{key_or_range}/{subscriber_id}
```

## Range model

The ordered keyspace is split into logical ranges. Ranges are used for:

- scan routing;
- repair scheduling;
- holder directory compression;
- mutation log partitioning;
- graph partitioning;
- vector index partitioning.

Ranges do not imply linearizable ownership. They are routing and maintenance units in v1.

Range metadata:

```yaml
rangeId: r-00123
startKey: /kv/default/data/a
endKey: /kv/default/data/f
namespace: default
preferredHolders:
  - member-a
  - member-b
  - member-c
observedHolders:
  - member-a
  - member-d
latencyRegion: cluster-local
policy: local-cache-default
versionWatermark: 123881
```

## Request paths

### KV write

```text
client -> gateway -> receiving data pod
receiving pod writes local wavesdb (in-process)
receiving pod sends StoreReplica to nearby candidates
first nearby durable ACK satisfies write success
background fills target-N replicas
background streams to global peers if enabled
```

### KV read

```text
client -> gateway -> nearest data pod
local lookup
if found and acceptable: return
if missing/stale: ask holder directory / range summary
fetch from closest holder
store as dynamic cache replica
subscribe to future updates if enabled
return
```

### Range scan

```text
client -> gateway -> local data pod
choose scan mode:
  cache-fast: scan local cache and known local ranges
  cache-complete: require range coverage certificate
  routed-eventual: contact holders by range and merge
return rows plus completeness metadata
```

### Graph/vector query

```text
client -> gateway/cypher frontend
parse Cypher subset
plan graph/vector fragments
route fragments to data pods
execute local scans/index lookups/vector searches
merge results
return rows plus freshness metadata
```

## Design constraints

Do not hide eventual semantics. Every API response should include:

```yaml
servedBy: member-id
readMode: local-cache | fetched-holder | routed-range | global-replica
freshness:
  version: ...
  observedAt: ...
  conflictState: none | resolved | siblings-present
completeness:
  point: complete
  scan: complete | partial | best-effort
```

