# Global Data Browser Resolution — Design

**Status:** approved design, pre-implementation
**Date:** 2026-06-26
**Area:** `internal/observability`, `internal/replication/global`, `proto/wavespan/v1`, `ui`

## Goal

Make the Data Browser's **Global** scope return a correct, stable, cluster-and-peer-wide
answer to "who holds this key, at what version, with what value" — instead of the current
stub that returns only the serving node's own local copy and always reports `PARTIAL`.

## Background — the bug

`ObsService.InspectGlobal` (`internal/observability/inspect_global.go`) today:

1. reads the **serving node's own** local record (`s.rstore.GetRecord`), and
2. checks `s.globalInspector`, which is **never wired** in `cmd/wavespan-node/main.go`, so it
   always falls into the `else` branch: `complete = false` + warning
   `"global holder resolution not configured on this node"`.

Confirmed live via CDP against the running cluster: 5 back-to-back `InspectGlobal` calls each
returned one key row with **no holders / no version** and a trailer of
`finalCompleteness: PARTIAL` + that warning. Because each node answers only with its *own*
local copy, different nodes give different results for the same global key — which is what the
user observed. (The "Cluster" scope, by contrast, works: it fans out via
`clusterScan.ScanLocal` and merges holders.)

Two gaps must close:

- **Within-cluster:** Global must show every holder in *this* cluster, not just the serving
  node. This is what fixes the single-cluster (e.g. staging `dev`) instability.
- **Cross-cluster:** When active-active replication is on, Global must also resolve holders in
  configured **peer clusters** (`design/06`), tagged by cluster.

## Architecture

A two-layer resolver wired into `InspectGlobal`:

```
InspectGlobal(ns, key, include_peer_clusters)
  ├─ Layer 1: resolve holders within THIS cluster   (always)
  │     fan out a point lookup to every alive member, merge by version
  └─ Layer 2: resolve holders in PEER clusters       (if include_peer_clusters && global on)
        one InspectKey RPC per peer cluster; the peer runs ITS OWN Layer 1 and
        returns holders tagged with its cluster_id
  → merge, emit one InspectKey listing every holder, honest COMPLETE/PARTIAL
```

### Layer 1 — within-cluster resolution (new, shared)

A focused unit resolves a **single exact key** across this cluster's alive members:

- start from the serving node's own record (`rstore`);
- for each *other* alive member (`cluster.Members()` where `State == Alive`), issue a point
  `FetchReplica(target, ns, key)` over the existing `ReplicationService`;
- merge: one `InspectHolder` per member that holds the key, carrying that member's `Version`;
  the surfaced value/version is the **latest** (`version.Compare`) across holders;
- **deterministic order:** holders sorted by `member_id` (mirroring `inspect_local.go`), so the
  UI rows never shuffle between identical requests;
- **completeness:** `complete = true` only if *every* alive member answered; an unreachable
  member appends a warning and flips `complete = false`. Best-effort — one slow peer never
  fails the whole call.

This is the same fan-out shape that already powers cluster-wide `InspectLocal`, specialized to
a point lookup. It lives in its own package (`internal/holderinspect`) so both the
observability service and the peer-side RPC handler can call it without an import cycle
(`global` must not import `observability`).

**Interface (consumed by Layer 1):**
```go
type MemberSource interface { Members() []membership.MemberView }
type ReplicaFetcher interface {
    FetchReplica(ctx, target membership.Member, ns string, key []byte) (*wavespanv1.FetchReplicaResponse, error)
}
```
`ReplicaFetcher` is satisfied by `*local.ConnectReplicator` (a `FetchReplica` client method on
the now-gRPC replication client; added if not already present).

### Layer 2 — cross-cluster resolution (new RPC)

A new RPC on the existing **`GlobalReplication`** service (already reached peer-to-peer on the
global-repl port via `WAVESPAN_GLOBAL_PEERS`):

```protobuf
service GlobalReplication {
  // ... existing PushGlobal / RangeSummary / FetchRange ...
  rpc InspectKey(InspectKeyRequest) returns (InspectKeyResponse);
}

message InspectKeyRequest  { string namespace = 1; bytes key = 2; bool include_value = 3; }
message InspectKeyResponse {
  repeated InspectHolder holders = 1;   // this cluster's holders, tagged with cluster_id
  StoredRecord best = 2;                // latest record this cluster holds (nil if none)
  bool complete = 3;                    // every alive member of this cluster answered
  repeated string warnings = 4;
}
```

The **peer-side handler** runs the peer's *own* Layer 1 resolver, stamps each returned
`InspectHolder.peer_cluster_id` with its own `cluster_id`, and returns. It does **not**
recurse into peers (no `include_peer_clusters` flag crosses the wire) — single-hop fan-out,
no cycles.

The **caller** (`PeerInspector`, satisfying `observability.GlobalInspector`):

- skips peer entries whose `ClusterID == self` or with an empty `ReplEndpoint`;
- calls `InspectKey` on each configured peer **in parallel**, bounded by a context deadline;
- aggregates holders + best record; `complete` is the AND of this cluster's Layer 1 and every
  peer's `complete`, and `false` if any peer is unreachable (with a naming warning).

`include_value` is honored end to end but values are revealed only when the *original* caller
is admin (`reveal := include_value && role == RoleAdmin`), preserving the existing redaction
rule. A peer returns values only when asked with `include_value` true.

### InspectGlobal orchestration

`inspect_global.go` becomes: emit header → Layer 1 (local cluster) → Layer 2 (peers, if
requested & enabled) → merge into one `InspectKey` (holders from both layers; value/version =
latest seen, so a value renders even when the serving node lacks the key) → trailer with merged
completeness + warnings. The "not configured" branch is removed; when global replication is off
or no peers are set, Layer 1 alone yields an honest `COMPLETE` for the local cluster (no scary
warning).

### Wiring (`cmd/wavespan-node/main.go`)

- Always construct the Layer 1 resolver (`holderinspect.New(self, rstore, membership, replicator)`).
- When `cfg.GlobalReplication.Enabled()`, also construct `PeerInspector(self.ClusterID,
  cfg.GlobalReplication.Peers, replicator)` and register the peer-side `InspectKey` handler on
  the `GlobalReplication` server (injecting the Layer 1 resolver).
- `obsSvc = obsSvc.WithGlobalInspector(combined)` — the combined inspector does Layer 1 always
  and Layer 2 when peers are configured.

### UI

- `DataBrowser.tsx`: Global mode sends `includePeerClusters: true`; the holders column renders
  `peer_cluster_id` when present (e.g. `b1 · test-b`) so cross-cluster holders are visible; the
  existing completeness badge + warnings already render.
- Value-modal change (separate, already drafted in this branch): the value cell clamps to a max
  width with ellipsis; values over a threshold get a **⤢** affordance opening a `Modal` with the
  full value (scrollable) and a **Copy** button. Copy uses a clipboard helper with a
  `<textarea>+execCommand` fallback because the admin console is often served over plain `http`
  (non-secure context, where `navigator.clipboard` is undefined).

## Error handling & semantics

- **Best-effort everywhere.** Unreachable member or peer → warning + `PARTIAL`, never an RPC
  error. The UI must never present a partial answer as global truth (it already surfaces the
  completeness badge + warnings).
- **Determinism.** Holders are sorted by `(peer_cluster_id, member_id)`. No map-iteration order
  leaks into the response, so identical requests yield byte-identical holder lists.
- **No recursion.** Peer `InspectKey` never crosses to further peers; the topology is a single
  hop from the serving node.
- **Redaction.** Values revealed only to admins; `key_hash` always present; a peer is asked for
  values only when the original request set `include_value` and the caller is admin.

## Testing

**Unit (`internal/holderinspect`, `internal/replication/global`):**
- Layer 1 merge: latest-version-wins, deterministic holder order, one unreachable member →
  `PARTIAL` + warning, all-answer → `COMPLETE`.
- `PeerInspector`: fake `FetchReplica`/`InspectKey` client — peer holders tagged with cluster_id;
  unreachable peer → `PARTIAL` + naming warning; `include_peer_clusters=false` or no peers →
  Layer-1-only, `COMPLETE`.
- Peer-side `InspectKey` handler over a real `httptest`/in-proc server (mirroring the existing
  `grpctest_test.go` pattern): a key held by the cluster resolves with the right holders/version.

**Integration (`tests/integration`, docker):**
- Use `docker/docker-compose.global.yaml` (cluster **test-a**: a1,a2 ↔ **test-b**: b1,b2,
  active-active). Write a `replicationFactor: global` key on **test-a**, await replication, then
  `InspectGlobal(include_peer_clusters=true)` against **test-b**'s admin port and assert holders
  from **both** clusters appear with `COMPLETE` and the value renders. Kill a peer pod and assert
  `PARTIAL` + a warning naming it.
- **Single-cluster regression:** against a one-cluster setup, assert Global now lists *all* alive
  members as holders (not just the serving node) and reports `COMPLETE` — the staging fix.

## Out of scope

- Peer-cluster gossip of holder summaries (Layer 2 resolves on demand via RPC, not gossip).
- Range/prefix global queries (Global remains a single-key resolver; prefix browsing is the
  Cluster scope's job).
- mTLS for cross-cluster inspect (inherits the deployment's existing global-repl transport
  posture).

## File structure

| File | Change |
|------|--------|
| `proto/wavespan/v1/replication.proto` | add `InspectKey` RPC + request/response messages; regenerate |
| `internal/holderinspect/resolver.go` (new) | Layer 1 single-key within-cluster resolver |
| `internal/replication/global/inspect.go` (new) | `PeerInspector` (Layer 2) + peer-side `InspectKey` handler |
| `internal/observability/inspect_global.go` | orchestrate Layer 1 + Layer 2; drop the stub branch |
| `internal/observability/obsservice.go` | `GlobalInspector` may already fit; adjust signature if needed |
| `internal/replication/local/connect.go` | `FetchReplica` client method (gRPC) if not present |
| `cmd/wavespan-node/main.go` | construct resolvers, register peer handler, `WithGlobalInspector` |
| `ui/src/views/DataBrowser.tsx` | send `includePeerClusters`; render `peer_cluster_id`; value modal |
| `ui/src/components/Modal.tsx`, `ui/src/lib/clipboard.ts` (new) | value modal + copy helper |
| `internal/holderinspect/*_test.go`, `internal/replication/global/inspect_test.go`, `tests/integration/global_inspect_test.go` | tests |
