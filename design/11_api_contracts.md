# 11. API contracts

## Public APIs

WaveSpan exposes:

- gRPC KV API;
- gRPC Cypher API;
- admin API;
- live observability API (the streaming companion to admin; powers the embedded node UI);
- internal replication API;
- internal gossip API.

REST can be added as a gateway wrapper later. gRPC is the implementation target. The browser
UI (`26_node_ui_and_observability.md`) talks to the same protos over ConnectRPC (`connect-es`),
so no REST shim is needed for it.

## Interface design rationale

WaveSpan deliberately uses **two surfaces, no SQL**:

- **KV is a dedicated typed gRPC interface** (`Put/Get/Delete/Scan/CompareAndSet/Watch`), not a
  query language. A point read must be a single framed protobuf round-trip on the latency-critical
  path; a SQL/relational front-end would pay lexing + parsing + planning on every call for no benefit.
- **Graph and vector use the Cypher subset** (`CypherService`), where traversal and vector procedures
  justify a declarative language.
- **SQL is an explicit non-goal** (`20_risks_and_non_goals.md`). SQL's value — joins, aggregations,
  globally consistent set queries — depends on guarantees WaveSpan intentionally does not make
  (no serializable transactions, no globally consistent scans, no linearizable reads). Exposing SQL
  would imply a contract the engine refuses to honor. The per-response consistency metadata
  (`ResponseMeta.source / completeness / conflict_state`) also maps naturally onto protobuf and
  awkwardly onto SQL result sets.

**Transport stance.** All services — public and internal — use gRPC over HTTP/2 with mTLS in v1, for
uniformity, streaming (`Scan/Watch/Subscribe*/PushGlobal`), and tooling. This is a deliberate v1
choice, **not** a proven-optimal one for the internal hot path: `StoreReplica`/`FetchReplica`/gossip
pay HTTP/2 framing and per-call overhead. If origin+1 latency misses its SLO
(see `IMPLEMENTATION_STRATEGY.md`), a leaner intra-cluster binary transport behind the same Go
interfaces is the first optimization lever — profile before replacing.

**Declarative KV filtering (deferred).** The one place a query capability would help KV is server-side
filtering of a `Scan` (e.g. prefix + value predicate) to avoid shipping rows the client discards. The
answer is **predicate pushdown on `Scan`** — a typed, optional `ScanFilter` message — **not** SQL.
Deferred past v1; `ScanRequest` reserves a field for it (see below).

## Common protobuf types

```protobuf
syntax = "proto3";

package wavespan.v1;

message Version {
  uint64 hlc_physical_ms = 1;
  uint32 hlc_logical = 2;
  string writer_cluster_id = 3;
  string writer_member_id = 4;
  uint64 writer_sequence = 5;
  bytes vector_clock = 6;
}

enum ConflictState {
  CONFLICT_STATE_UNSPECIFIED = 0;
  CONFLICT_NONE = 1;
  CONFLICT_RESOLVED = 2;
  CONFLICT_SIBLINGS_PRESENT = 3;
}

enum Completeness {
  COMPLETENESS_UNSPECIFIED = 0;
  COMPLETE = 1;
  PARTIAL = 2;
  BEST_EFFORT = 3;
}

enum ReadSource {
  READ_SOURCE_UNSPECIFIED = 0;
  LOCAL_DURABLE = 1;
  LOCAL_DYNAMIC_CACHE = 2;
  FETCHED_CLOSEST_HOLDER = 3;
  ROUTED_RANGE = 4;
  GLOBAL_REMOTE = 5;
}

message ResponseMeta {
  string served_by_cluster_id = 1;
  string served_by_member_id = 2;
  ReadSource source = 3;
  optional Version observed_version = 4;
  ConflictState conflict_state = 5;
  Completeness completeness = 6;
  int64 observed_at_unix_ms = 7;
  repeated string warnings = 8;
}
```

## KV service

```protobuf
service KvService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  rpc Scan(ScanRequest) returns (stream ScanResponse);
  rpc CompareAndSet(CompareAndSetRequest) returns (CompareAndSetResponse);
  rpc Watch(WatchRequest) returns (stream WatchEvent);
}

message PutRequest {
  string namespace = 1;
  bytes key = 2;
  bytes value = 3;
  optional int64 ttl_ms = 4;
  PutOptions options = 5;
}

message PutOptions {
  bool require_origin_plus_one = 1; // default true
  optional string conflict_policy_override = 2;
  optional string geo_policy_override = 3;
  optional string request_id = 4; // idempotency key; same id => one logical mutation
}

message PutResponse {
  ResponseMeta meta = 1;
  Version version = 2;
  uint32 acked_nearby_replicas = 3;
  bool geo_spillover = 4;
}

message GetRequest {
  string namespace = 1;
  bytes key = 2;
  GetOptions options = 3;
}

message GetOptions {
  bool allow_dynamic_cache = 1; // default true
  optional Version min_session_version = 2;
  bool hide_expired_on_read = 3;
}

message GetResponse {
  ResponseMeta meta = 1;
  bool found = 2;
  bytes key = 3;
  bytes value = 4;
  optional int64 expires_at_unix_ms = 5;
  repeated StoredSibling siblings = 6;
}

message StoredSibling {
  bytes value = 1;
  Version version = 2;
  bool tombstone = 3;
}

message DeleteRequest {
  string namespace = 1;
  bytes key = 2;
}

message DeleteResponse {
  ResponseMeta meta = 1;
  Version version = 2;
  uint32 acked_nearby_replicas = 3;
}

message CompareAndSetRequest {
  string namespace = 1;
  bytes key = 2;
  optional Version expected_version = 3; // absent => expect key absent
  bytes value = 4;
  optional int64 ttl_ms = 5;
  optional string request_id = 6;
}

message CompareAndSetResponse {
  ResponseMeta meta = 1;
  bool applied = 2;
  Version version = 3;
  uint32 acked_nearby_replicas = 4;
  // Best-effort at the coordinator vs its local latest pointer; NOT linearizable.
  // True when the coordinator has unapplied repl/global/in mutations or unmerged
  // siblings for the key, so the compare may have raced a not-yet-applied winner.
  // See 03_kv_store.md "Compare-and-set semantics".
  bool cas_conflict_window = 5;
}

enum ScanMode {
  SCAN_MODE_UNSPECIFIED = 0;
  CACHE_FAST = 1;
  CACHE_COMPLETE = 2;
  ROUTED_EVENTUAL = 3;
  LOCAL_ONLY = 4;
}

message ScanRequest {
  string namespace = 1;
  bytes start_key = 2;
  bytes end_key = 3;
  uint32 limit = 4;
  ScanMode mode = 5;
  // reserved for server-side predicate pushdown (deferred past v1; see
  // "Interface design rationale"). A typed filter, never an embedded query language.
  optional ScanFilter filter = 6;
}

message ScanResponse {
  oneof msg {
    ScanHeader header = 1;
    ScanRow row = 2;
    ScanTrailer trailer = 3;
  }
}

message ScanHeader {
  ResponseMeta meta = 1;
  ScanMode mode = 2;
}

message ScanRow {
  bytes key = 1;
  bytes value = 2;
  Version version = 3;
  optional int64 expires_at_unix_ms = 4;
}

message ScanTrailer {
  uint64 rows_returned = 1;
  Completeness final_completeness = 2;
  repeated string warnings = 3;
}
```

## Cypher service

```protobuf
service CypherService {
  rpc Query(CypherRequest) returns (stream CypherResponse);
}

message CypherRequest {
  string graph = 1;
  string query = 2;
  map<string, Value> params = 3;
  CypherOptions options = 4;
}

message CypherOptions {
  bool allow_cache = 1;
  uint32 timeout_ms = 2;
  uint32 max_rows = 3;
}

message CypherResponse {
  oneof msg {
    CypherHeader header = 1;
    CypherRow row = 2;
    CypherTrailer trailer = 3;
  }
}

message CypherHeader {
  ResponseMeta meta = 1;
  repeated string columns = 2;
}

message CypherRow {
  repeated Value values = 1;
}

message CypherTrailer {
  uint64 rows_returned = 1;
  repeated string warnings = 2;
}
```

## Internal replication service

```protobuf
service ReplicationService {
  rpc StoreReplica(StoreReplicaRequest) returns (StoreReplicaResponse);
  rpc FetchReplica(FetchReplicaRequest) returns (FetchReplicaResponse);
  rpc SubscribeKey(SubscribeKeyRequest) returns (stream CacheUpdate);
  rpc SubscribeRange(SubscribeRangeRequest) returns (stream CacheUpdate);
  rpc PushGlobal(stream GlobalMutation) returns (stream GlobalAck);
}
```

## Admin service

```protobuf
service AdminService {
  rpc GetMembership(GetMembershipRequest) returns (GetMembershipResponse);
  rpc GetLatencyGraph(GetLatencyGraphRequest) returns (GetLatencyGraphResponse);
  rpc GetRepairStatus(GetRepairStatusRequest) returns (GetRepairStatusResponse);
  rpc TriggerRepair(TriggerRepairRequest) returns (TriggerRepairResponse);
  rpc GetReplicationLag(GetReplicationLagRequest) returns (GetReplicationLagResponse);
}
```

## Observability service

The live/streaming companion to `AdminService`'s point queries. `AdminService` answers
"what is the state now"; `ObservabilityService` lets a caller **subscribe** to gossip as it
happens and **stream-inspect** local and global data. It is the surface behind the embedded
node UI (`26_node_ui_and_observability.md`), and the same Connect surface the correctness
harness (`25_*`) and `wavespanctl` use programmatically.

**Transport.** Like every other service here it is ConnectRPC over HTTP/2: a buf-generated
`connect-go` server (`internal/ui`/`internal/observability`) and `connect-es` clients in the
React UI, from the *same* protos. Streaming RPCs send a header first and a trailer last, as
required below. It is mounted on the admin port (7900) behind admin auth (`15_security.md`),
read-only — value reveal requires the `admin` role and an explicit opt-in.

```protobuf
service ObservabilityService {
  // Live gossip with a server-side filter and a backfill-then-tail ring buffer.
  rpc StreamGossip(GossipStreamRequest) returns (stream GossipEvent);
  // Browse THIS node's local data by prefix/range across the logical keyspace.
  rpc InspectLocal(InspectLocalRequest) returns (stream InspectRow);
  // Resolve a key/range across the cluster (and peer clusters); eventual/best-effort.
  rpc InspectGlobal(InspectGlobalRequest) returns (stream InspectRow);
  // Membership + latency-graph snapshot for the topology view.
  rpc GetClusterView(GetClusterViewRequest) returns (GetClusterViewResponse);
}

enum GossipKind {
  GOSSIP_KIND_UNSPECIFIED = 0;
  GOSSIP_PING = 1;
  GOSSIP_ACK = 2;
  GOSSIP_SUSPECT = 3;
  GOSSIP_ALIVE = 4;
  GOSSIP_UNREACHABLE = 5;
  GOSSIP_DEAD = 6;
  GOSSIP_HOLDER_SUMMARY = 7;
  GOSSIP_LATENCY_EDGE = 8;
  GOSSIP_MEMBERSHIP_DELTA = 9;
}

enum GossipDirection {
  GOSSIP_DIRECTION_UNSPECIFIED = 0;
  GOSSIP_IN = 1;
  GOSSIP_OUT = 2;
}

message GossipFilter {
  repeated GossipKind kinds = 1;        // empty => all kinds
  repeated string peer_member_ids = 2;  // empty => all peers; matches from or to
  GossipDirection direction = 3;        // UNSPECIFIED => both
  optional string namespace = 4;        // for holder-summary / membership-delta events
  optional string range_id = 5;
}

message GossipStreamRequest {
  GossipFilter filter = 1;
  bool backfill = 2;          // replay matching ring-buffer events before tailing live
  uint32 backfill_limit = 3;  // cap on replayed events; 0 => server default
}

message GossipEvent {
  oneof msg {
    GossipRecord record = 1;
    GapMarker gap = 2;        // emitted when the server dropped events under backpressure
  }
}

message GossipRecord {
  int64 observed_at_unix_ms = 1;
  string from_member_id = 2;
  string to_member_id = 3;
  GossipKind kind = 4;
  GossipDirection direction = 5;
  optional string namespace = 6;
  optional string range_id = 7;
  // Decoded, human-readable per-kind summary; never raw record bytes (see 15_security.md).
  GossipPayloadSummary summary = 8;
  uint32 payload_size_bytes = 9;
  bool backfill = 10;         // true if replayed from the ring buffer rather than live
}

message GossipPayloadSummary {
  oneof kind {
    PingAckSummary ping_ack = 1;        // rtt for ping/ack
    LivenessSummary liveness = 2;       // new state for suspect/alive/unreachable/dead
    HolderSummary holder_summary = 3;   // reuses the type from admin.proto
    LatencyEdge latency_edge = 4;       // reuses the type from admin.proto
    MembershipDeltaSummary membership_delta = 5;
  }
}

message PingAckSummary { double rtt_ms = 1; bool indirect = 2; }
message LivenessSummary { string member_id = 1; string new_state = 2; }
message MembershipDeltaSummary {
  repeated string added_member_ids = 1;
  repeated string removed_member_ids = 2;
  repeated string changed_member_ids = 3;
}

message GapMarker {
  uint64 dropped_count = 1;
  int64 since_unix_ms = 2;
}

enum HolderClass {
  HOLDER_CLASS_UNSPECIFIED = 0;
  HOLDER_ORIGIN = 1;
  HOLDER_DURABLE = 2;
  HOLDER_DYNAMIC_CACHE = 3;
  HOLDER_SUMMARY_ONLY = 4;
}

enum Keyspace {
  KEYSPACE_UNSPECIFIED = 0;
  KEYSPACE_KV = 1;
  KEYSPACE_GRAPH = 2;
  KEYSPACE_VECTOR = 3;
  KEYSPACE_SYS = 4;
  KEYSPACE_REPL = 5;
  KEYSPACE_CACHE = 6;
}

message InspectLocalRequest {
  Keyspace keyspace = 1;
  bytes prefix = 2;            // either prefix, or [start_key,end_key)
  bytes start_key = 3;
  bytes end_key = 4;
  uint32 limit = 5;
  bool include_value = 6;      // requires admin role; otherwise value is redacted
}

message InspectGlobalRequest {
  Keyspace keyspace = 1;
  bytes prefix = 2;
  bytes start_key = 3;
  bytes end_key = 4;
  uint32 limit = 5;
  bool include_peer_clusters = 6;  // include global/peer-cluster holders when enabled
  bool include_value = 7;          // requires admin role
}

message InspectRow {
  oneof msg {
    InspectHeader header = 1;
    InspectKey row = 2;
    InspectTrailer trailer = 3;
  }
}

message InspectHeader {
  ResponseMeta meta = 1;
  Keyspace keyspace = 2;
}

message InspectKey {
  string logical_path = 1;     // decoded key path, e.g. /kv/default/data/<user_key>
  string key_hash = 2;         // base64url(blake3(namespace+key)); always present
  bytes value = 3;             // empty unless include_value && admin role
  Version version = 4;
  ConflictState conflict_state = 5;
  optional int64 expires_at_unix_ms = 6;
  repeated InspectHolder holders = 7;   // local: 1 entry (self); global: per holder
  repeated InspectSibling siblings = 8;
}

message InspectHolder {
  string member_id = 1;
  string cluster_id = 2;       // set for peer-cluster holders in InspectGlobal
  HolderClass holder_class = 3;
  Version version = 4;
  ConflictState conflict_state = 5;
  optional int64 replication_lag_ms = 6;
  optional int64 global_repl_lag_ms = 7;
  bool reachable = 8;          // false => contributed staleness, see trailer completeness
}

message InspectSibling {
  Version version = 1;
  bool tombstone = 2;
  bytes value = 3;             // empty unless include_value && admin role
}

message InspectTrailer {
  uint64 rows_returned = 1;
  Completeness final_completeness = 2;   // PARTIAL/BEST_EFFORT if truncated or holders missed
  repeated string warnings = 3;          // unreachable holders, stale summaries, skipped clusters
}

message GetClusterViewRequest {
  bool include_repair_pressure = 1;
}

message GetClusterViewResponse {
  ResponseMeta meta = 1;
  repeated Member members = 2;           // roster + liveness (reuses admin.proto Member)
  repeated LatencyEdge edges = 3;        // directed latency graph (04_*; admin.proto)
  repeated RangeRepairStatus repair = 4; // per-range under-replication (23_*)
}

message RangeRepairStatus {
  string range_id = 1;
  string namespace = 2;
  uint32 durable_replica_count = 3;
  uint32 target_replica_count = 4;
  bool under_replicated = 5;
  optional int64 fill_lag_seconds = 6;
}
```

`Member`, `LatencyEdge`, and `HolderSummary` are the messages defined for `AdminService`
(`04_membership_latency_gossip.md`; `proto/wavespan/v1/admin.proto`); `ObservabilityService`
reuses them rather than redefining them.

## Management plane: one authoritative surface

The management plane is consolidated to avoid drift across four overlapping surfaces:

- **`AdminService` (gRPC) is the single source of truth** for runtime introspection and operations.
  Operations split by safety:
  - *Read-only diagnostics* (`GetMembership`, `GetLatencyGraph`, `GetRepairStatus`,
    `GetReplicationLag`): reader/operator role.
  - *Mutating operations* (`TriggerRepair`, drain, and future lifecycle ops): operator/admin role
    only (see `15_security.md`). Replicator credentials must never reach these.
- **HTTP `/admin/*` (`14_observability.md`) is a thin read-only/debug gateway** over `AdminService`,
  for humans and dashboards. It must not be a second mutation path.
- **`ObservabilityService` (Connect) is the read-only live/streaming companion** to
  `AdminService`: subscribe to gossip, stream-inspect local/global data, snapshot the cluster
  view. It is the surface behind the embedded node UI (`26_node_ui_and_observability.md`) and
  is consumed by the harness (`25_*`) and `wavespanctl`. It performs no mutations and is not a
  second mutation path; value reveal requires the `admin` role and an explicit opt-in.
- **`wavespanctl` is a gRPC client of `AdminService` (and `ObservabilityService`)**, not a
  parallel implementation.
- **Declarative config has one schema, two loaders.** Kubernetes CRDs (`12_crds.md`) are the source
  of truth in production; the *same* config schema is loaded from YAML in Docker (`10_docker_dev.md`).
  Generate both from one definition so they cannot diverge.

## API requirements

- All mutation APIs must be idempotent if client supplies request ID.
- All read APIs must include response metadata.
- Streaming APIs must send a header first and trailer last.
- Internal APIs must require mTLS.
- Public APIs must support deadlines and cancellation.
- Server must propagate cancellation to remote fragments.

