---
title: KV API
section: Reference
order: 4
summary: The gRPC/Connect key-value surface — Put, Get, MultiGet, Delete, Scan — and the ResponseMeta that rides on every reply.
---

# KV API

The KV service is the primary data surface. It is defined in `proto/wavespan/v1/kv.proto` and exposed over gRPC + Connect (HTTP/2, mTLS). The browser console talks to the same service via `connect-es`.

## Operations

```protobuf
service KvService {
  rpc Put(PutRequest) returns (PutResult);
  rpc Get(GetRequest) returns (GetResult);
  rpc MultiGet(MultiGetRequest) returns (MultiGetResult);
  rpc Delete(DeleteRequest) returns (DeleteResult);
  rpc Scan(ScanRequest) returns (stream ScanResponse);
}
```

### Put

```protobuf
message PutRequest {
  string namespace = 1;
  bytes  key = 2;
  bytes  value = 3;
  optional int64 ttl_ms = 4;            // lazy, best-effort expiry
  optional string idempotency_key = 5;  // dedupes retries
  bool require_origin_plus_one = 6;      // default true
}
```

A `Put` is acknowledged once it is durable on the **origin + 1** nearby replica. The result reports the assigned `version` and how many nearby replicas acked.

```protobuf
message PutResult {
  ResponseMeta meta = 1;
  Version version = 2;
  uint32 acked_nearby_replicas = 3;
  bool geo_spillover = 4;
}
```

### Get

```protobuf
message GetRequest {
  string namespace = 1;
  bytes  key = 2;
  bool   allow_dynamic_cache = 3;   // may serve from a cached replica
  bool   hide_expired_on_read = 4;  // suppress lazily-expired records
}
```

The `GetResult` includes the value (if found), its expiry, and — crucially — the `ResponseMeta` telling you *where* the value came from.

### MultiGet

`MultiGet` batches point reads into a single round-trip to amortize per-request RPC overhead. Results are returned **in request order**, each with its own `ResponseMeta`.

### Delete

A delete writes a **tombstone**, not a physical removal. The tombstone replicates and propagates like any other write so lagging replicas eventually stop serving the old value. Deletes are also origin+1 by default.

### Scan

`Scan` is a server-streaming range read:

```protobuf
message ScanRequest {
  string namespace = 1;
  bytes  start_key = 2;
  bytes  end_key = 3;
  uint32 limit = 4;
  ScanMode mode = 5; // CACHE_FAST | CACHE_COMPLETE | ROUTED_EVENTUAL | LOCAL_ONLY
}
```

The stream ends with a trailer carrying the **final completeness** and any warnings. A scan is never silently truncated — if gaps are possible, the completeness says so.

## ResponseMeta — the honesty contract

Every KV reply carries this block:

```protobuf
message ResponseMeta {
  string served_by_cluster_id = 1;
  string served_by_member_id = 2;
  ReadSource source = 3;            // LOCAL_DURABLE | LOCAL_DYNAMIC_CACHE | …
  optional Version observed_version = 4;
  ConflictState conflict_state = 5; // NONE | RESOLVED | SIBLINGS_PRESENT
  Completeness completeness = 6;    // COMPLETE | PARTIAL | BEST_EFFORT
  int64 observed_at_unix_ms = 7;
  repeated string warnings = 8;
}
```

Read it on every response. It is how WaveSpan keeps eventual consistency *honest* rather than surprising.

## Idempotency

Supply an `idempotency_key` on `Put`/`Delete` to make retries safe. WaveSpan dedupes by `cluster_id + member_id + writer_sequence` or your supplied key, so a re-sent write after a timeout does not produce a duplicate or an out-of-order version.

## Reaching the same keys from Cypher

These exact records are also reachable from Cypher — the graph layer and the KV API share one store and one namespace scheme. Use `kv.get` / `kv.put` / `kv.delete` to read or mutate KV inline in a query (e.g. join a `MATCH` against a profile blob, or filter rows on a flag). See [Cypher & Graph](doc:cypher-and-graph) for the built-ins and examples.

> The KV API is an AP cache (local-node writes, tunable durability). For data that many nodes must agree on — shared sets, maps, or leaderboards written from a central point — use the strongly-consistent [Replicated Collections](doc:replicated-collections) tier instead.

> Try it: the [KV Writer](doc:overview) tab writes a record through a chosen coordinator node, and the [Data Browser](doc:overview) shows it propagating across the cluster.
