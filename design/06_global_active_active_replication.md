# 06. Global active-active replication

## Goal

Support multiple Kubernetes clusters that can all accept writes and converge asynchronously.

Default global mode:

```yaml
globalReplication:
  mode: active-active-async
```

No cross-cluster consensus is used in the hot path.

## Global topology

```yaml
clusters:
  - clusterId: prod-use1
    geo: us-east
    endpoints:
      gossip: Wavespan-gossip.use1.example:7700
      repl: Wavespan-repl.use1.example:7801
  - clusterId: prod-euw1
    geo: eu-west
    endpoints:
      gossip: Wavespan-gossip.euw1.example:7700
      repl: Wavespan-repl.euw1.example:7801
```

## Data movement

Every committed local mutation is appended to a partitioned global replication log:

```text
/repl/global/out/{peer_cluster}/{partition}/{seq}
```

Replicators stream these entries to peer clusters.

Remote clusters write received entries into:

```text
/repl/global/in/{origin_cluster}/{partition}/{seq}
```

Then they apply mutation envelopes idempotently to local WavesDB.

## Mutation identity

Global mutation IDs must be stable and idempotent.

```text
mutation_id = cluster_id + member_id + writer_sequence
```

Receiver must ignore already-applied mutation IDs.

## Conflict policies

Conflict policy is namespace/index/graph configurable.

### HLC last-write-wins

Default for simple KV.

Pros:

- deterministic;
- simple;
- fast;
- low storage overhead.

Cons:

- can lose concurrent updates;
- unsafe for counters, sets, append logs, and business-critical updates.

### Keep siblings

Store concurrent versions and return them to the client.

Pros:

- does not silently lose data;
- good for early correctness.

Cons:

- client/application must resolve;
- storage can grow if unresolved.

### CRDT policies (deferred post-v1)

The following typed merge policies are **defined as `ConflictResolver` contracts but not
implemented in v1**:

- grow-only counter;
- PN-counter;
- OR-set;
- LWW-register;
- append-only log.

They exist in the design so the resolver interface and namespace policy enum are stable,
but selecting one in v1 is a validation error, not a working merge. Do not assume CRDT
semantics are available before they ship.

Pros (once implemented):

- safe automatic merge for supported semantics.

Cons:

- only works for typed values;
- **not in v1** — post-v1 work.

### Application resolver

Call a resolver plugin or configured WASM function.

Use later, not v1.

## v1 conflict-resolver scope

To be explicit about what actually ships:

| Policy | v1 status |
|---|---|
| `hlc-last-write-wins` | Implemented. Default. |
| `keep-siblings` | Implemented. |
| `crdt-counter` (G-counter / PN-counter) | Interface only. Deferred post-v1. |
| `crdt-set` (OR-set) | Interface only. Deferred post-v1. |
| `lww-register` | Interface only. Deferred post-v1. |
| `append-log` | Interface only. Deferred post-v1. |
| `app-resolver` (plugin/WASM) | Interface only. Deferred post-v1. |

**v1 ships exactly two working resolvers: `hlc-last-write-wins` and `keep-siblings`.**
Everything else is the interface contract below with no v1 implementation.

## Conflict resolution interface

Every resolver implements one Go interface. v1 provides the HLC-LWW and keep-siblings
implementations; the typed-CRDT and app-resolver implementations are deferred.

```go
// ConflictResolver merges concurrent versions of a single key/record.
// Implementations must be deterministic: the same input set must yield the
// same ResolveResult on every member so clusters converge.
type ConflictResolver interface {
    Resolve(existing []StoredRecord, incoming StoredRecord) ResolveResult
}

type ResolveKind int

const (
    ResolveWinner ResolveKind = iota // single winning record
    ResolveSiblings                  // keep concurrent versions
    ResolveTombstone                 // delete wins
    ResolveReject                    // cannot merge; reason set
)

type ResolveResult struct {
    Kind     ResolveKind
    Winner   StoredRecord   // set when Kind == ResolveWinner or ResolveTombstone
    Siblings []StoredRecord // set when Kind == ResolveSiblings
    Reason   string         // set when Kind == ResolveReject
}
```

In v1 the resolver registry only binds `hlc-last-write-wins` and `keep-siblings`. Binding
any deferred policy name fails namespace admission (see doc 12).

## Delete conflicts

Deletes are tombstones. Tombstones must have versions and replicate globally.

Default rule under HLC-LWW:

```text
tombstone wins only if its version wins
```

Under keep-siblings:

```text
tombstone may coexist with sibling values until resolved
```

## TTL in global replication

TTL is based on expiration timestamp from the origin mutation.

Remote clusters must not recompute TTL duration from apply time.

```text
expires_at = origin_expires_at
```

Because TTL is approximate, remote clusters may expose data briefly after expiration unless namespace says `hideExpiredOnRead=true`.

## Anti-entropy

Use periodic anti-entropy to repair missed replication:

1. divide keyspace into ranges;
2. compute per-range Merkle summaries or hash trees;
3. exchange summaries with peer clusters;
4. identify divergent ranges;
5. exchange missing mutation IDs or records;
6. apply through normal conflict resolver.

Anti-entropy is mandatory because VPN restarts and long partitions are expected.

## Global read routing

Reads default to local cluster.

Optional read policies:

| Policy | Behavior |
|---|---|
| `local-first` | Read local cluster only unless miss. |
| `closest-global-holder` | Use global holder directory to fetch from nearest cluster. |
| `multi-cluster-merge` | Query multiple clusters and merge versions. Higher latency. |

Default:

```yaml
globalReadPolicy: local-first
```

## Graph and vector global replication

Graph and vector raw records replicate through the same mutation-log protocol.

Derived indexes are built locally.

For vector data:

- replicate raw vector record;
- replicate metadata and graph links;
- do not replicate HNSW internal graph as authoritative data in v1;
- rebuild/update ANN index in each cluster from mutation logs;
- exact search can use raw vectors immediately after apply;
- approximate search sees vector after local index update or delta-index insert.

This keeps global replication fast because the wire format contains source records, not index internals.

## Backpressure

Global replication queues must be bounded. Each peer has a per-peer out-log disk budget:

```yaml
globalOutLogBudgetBytesPerPeer: 50Gi   # example
```

### Default behavior: never block local writes

By default the out-log is decoupled from the local write path. When a peer is unreachable
or slow, the out-log for that peer grows until its budget is reached. On budget exceedance:

```text
1. stop streaming new entries to that peer;
2. mark the peer LAGGING;
3. rely on anti-entropy to repair divergence after the peer reconnects;
4. drop the oldest out-log entries ONLY after an anti-entropy checkpoint has been
   recorded that covers them — never drop entries that no checkpoint has superseded.
```

This means a single down or slow peer cannot stall local writes: the local cluster keeps
accepting writes, the peer falls behind, and convergence is recovered through anti-entropy
once connectivity returns. Dropping pre-checkpoint out-log entries would risk silent data
loss, so it is gated on a covering checkpoint.

### Durability-required namespaces: apply write backpressure

For namespaces declaring `globalDurabilityRequired=true`, falling behind is not
acceptable. When the out-log budget for a required peer is exceeded, apply **write
backpressure** to local writers for that namespace instead of dropping or detaching: local
writes block (subject to `writeTimeout`) until the out-log drains below budget. These
namespaces trade local write availability for guaranteed global durability.

```text
globalDurabilityRequired=false  (default)  -> never block local writes; LAGGING + anti-entropy repair
globalDurabilityRequired=true              -> apply write backpressure when out-log budget exceeded
```

In all modes:

- compact/drop old logs only after an anti-entropy checkpoint covers them;
- expose replication lag and budget metrics (below).

### Alerts

```text
GlobalReplicationLagHigh        // out/in lag past threshold for a peer
GlobalOutLogBudgetExceeded      // per-peer out-log budget reached; peer marked LAGGING
```

## Split-brain behavior

During cluster partition:

- both sides continue accepting writes;
- conflicts are expected;
- convergence happens after connectivity returns;
- application must choose conflict policy per namespace.

This is intentional active-active behavior.

## Required metrics

```text
global_repl_out_lag_seconds
global_repl_in_lag_seconds
global_repl_bytes_sent_total
global_repl_bytes_received_total
global_repl_conflicts_total
global_repl_conflicts_by_policy_total
global_repl_anti_entropy_runs_total
global_repl_anti_entropy_divergent_ranges_total
global_repl_apply_errors_total
```

## Implementation checklist

- [ ] Global peer config and authentication implemented.
- [ ] Outbound mutation log partitioning implemented.
- [ ] Inbound idempotent apply implemented.
- [ ] HLC-LWW resolver implemented.
- [ ] Keep-siblings resolver implemented.
- [ ] Tombstone conflict behavior implemented.
- [ ] Anti-entropy hash summaries implemented.
- [ ] Replication lag metrics implemented.
- [ ] Vector raw replication and local index update implemented.

