# 26. Node UI and live observability

## Goal

Every `wavespan-node` process serves a small embedded web UI and a companion streaming
gRPC/Connect surface for live introspection. The UI lets an operator **watch the gossip
protocol as it happens** (subscribe and filter live gossip traffic) and **inspect both local
and global data** without leaving the node. There is no separate web server and no external
asset bundle: the UI ships *inside* the node binary.

This complements, and does not replace, the existing management plane. `AdminService`
(`11_api_contracts.md`) remains the authoritative surface for point queries and mutating
operations; the new `ObservabilityService` defined here is its **live/streaming companion** —
the same Connect surface the correctness harness (`25_*`) and `wavespanctl` consume
programmatically. The HTTP `/admin/*` gateway (`14_observability.md`) stays a thin read-only
debug surface and is unaffected.

## Non-goals

- Not a query console. KV/Cypher data manipulation stays on `KvService`/`CypherService`.
- Not a second mutation path. The UI issues no writes, no repair triggers, no lifecycle ops.
  Mutating operations live on `AdminService` behind the operator/admin role (`15_security.md`).
- Not a cluster-wide dashboard service. Each node serves only its own UI; cluster-wide views
  are assembled client-side from per-node `ObservabilityService` streams (or via a gateway
  that fans out — out of scope for v1).

## Embedding and serving

The UI is a **Vite + React + TypeScript** single-page app, built to static assets and embedded
into `wavespan-node` via `go:embed`. This keeps the `FROM scratch` image (`24_*`) a single
self-contained binary with no external files to mount.

```text
ui/                         # Vite + React + TS source (not shipped)
  src/...
  package.json
  vite.config.ts
internal/ui/
  embed.go                  # //go:embed dist  -> embedded fs.FS
  dist/                     # Vite build output, committed or built in CI; the embed target
  server.go                 # static asset handler + dev proxy + admin-port mount
```

Serving:

- **Production:** `internal/ui/server.go` serves the embedded `dist/` filesystem (SPA fallback
  to `index.html` for client-side routes). Assets are content-hashed by Vite and served with
  long-lived cache headers; `index.html` is served `no-cache`.
- **Development:** when `WAVESPAN_UI_DEV=1` (or `--ui-dev`), the node reverse-proxies UI asset
  requests to a running Vite dev server (default `http://localhost:5173`) for HMR, while RPC
  requests still hit the in-process Connect handlers. No embedded assets are used in dev.

### Port and auth

The UI and the `ObservabilityService` Connect endpoints are mounted on the **admin port
(7900)** (`04_membership_latency_gossip.md` "advertiseAddrs", `09_kubernetes_operator.md`),
alongside the existing `/admin/*` gateway. They sit **behind admin auth** (`15_security.md`):

- the static UI and all `ObservabilityService` read/stream RPCs require the `reader` role
  (or higher) and a valid admin token / mTLS identity;
- value reveal (see "Security and redaction") requires the `admin` role;
- in Docker `insecureDevMode` the auth check is bypassed exactly as the rest of the admin
  surface is, and is gated by the same explicit flag.

The admin port serves three co-mounted things on one HTTP/2 listener: `/admin/*` (existing
read-only gateway), `/wavespan.v1.AdminService/*` and `/wavespan.v1.ObservabilityService/*`
(Connect/gRPC), and `/` + `/assets/*` (the SPA). Connect's content negotiation lets the gRPC
internal callers and the browser (`connect-web`, HTTP/2 or HTTP/1.1) share the same routes.

## ObservabilityService (Connect/gRPC)

Transport is **ConnectRPC**: a buf-generated `connect-go` server and `connect-es` /
`@connectrpc/connect-web` + `@connectrpc/connect-query` clients in React. The *same* protos
power internal RPC and the browser. Live updates use Connect **server-streaming** RPCs, not a
separate WebSocket layer. All streaming RPCs send a header first and a trailer last, exactly
like `KvService.Scan` and the other streams (`11_api_contracts.md` "API requirements").

The proto is defined authoritatively in `11_api_contracts.md` ("ObservabilityService"); this
section describes behavior.

### StreamGossip — live gossip, filtered

```text
rpc StreamGossip(GossipStreamRequest) returns (stream GossipEvent)
```

Server-streams gossip activity observed by **this** node: SWIM ping/ack, indirect probes,
suspect/alive transitions, holder-summary exchanges, latency-edge updates, and membership
deltas (`04_membership_latency_gossip.md` "Gossip protocol", "Liveness states", "Holder
summaries", "Latency graph").

- **Ring buffer + tail.** The node maintains a bounded in-memory **ring buffer** of recent
  `GossipEvent`s (default 4096 events / ~last few minutes, configurable). On subscribe, the
  request's `backfill` controls whether the server first replays matching buffered events
  (oldest→newest, marked `backfill=true`) and then tails live. This lets the UI show recent
  history immediately, then follow.
- **Server-side filtering.** `GossipStreamRequest` carries a `GossipFilter` applied *before*
  events hit the stream, so a narrow filter keeps stream volume low:
  - by **kind**: any subset of `{ping, ack, suspect, alive, unreachable, dead,
    holder_summary, latency_edge, membership_delta}`;
  - by **peer** `member_id` (one or many): events whose `from`/`to` matches;
  - by **direction**: `IN`, `OUT`, or both;
  - by **namespace/range**: for holder-summary and membership-delta events that carry one.
- **Event shape.** Each `GossipEvent` carries: `observed_at_unix_ms`, `from_member_id`,
  `to_member_id`, `kind`, `direction`, an optional `namespace`/`range_id`, a **decoded payload
  summary** (a small human-readable struct per kind — e.g. RTT for ping/ack, new state for
  suspect/alive, watermark + approx key count for holder summaries, EWMA/p95 for latency
  edges, added/removed members for deltas), and the encoded `payload_size_bytes`. Raw payload
  bytes are **not** streamed by default (see redaction).
- **Backpressure.** Slow UI clients must not stall gossip. The per-subscription queue is
  bounded; on overflow the server **drops oldest** and emits a single `GapMarker` event
  (`dropped_count`, `since_unix_ms`) so the UI can render a visible gap rather than silently
  losing events. Gossip processing never blocks on a subscriber.

### InspectLocal — browse this node's data

```text
rpc InspectLocal(InspectLocalRequest) returns (stream InspectRow)
```

Server-streams rows from **this node's** `LocalStore` (`internal/storage`), browsing the
logical keyspace (`01_architecture.md` "Internal keyspace"): `/kv`, `/graph`, `/vector`,
`/sys`, `/repl`, `/cache`. The request carries a `keyspace` selector and a `prefix` or
`[start_key, end_key)` range plus a `limit`. Implementation reuses the existing `LocalStore`
scan — this is a read-only local scan, never a routed/global one.

Each `InspectRow` reports, per key: the decoded logical path, current `Version`
(`11_api_contracts.md`), **holder class** (`origin` / `durable` / `dynamic-cache`, from the
holder record / range directory), `ConflictState` and any `siblings` (version + tombstone
flag, values redacted by default), `expires_at_unix_ms` (TTL), and `present_locally=true`.
A `header` carries `ResponseMeta` with `source = LOCAL_DURABLE`/`LOCAL_DYNAMIC_CACHE`; a
`trailer` carries `rows_returned` and `final_completeness` (`COMPLETE` if the scan was not
truncated, `PARTIAL` if it hit `limit`).

### InspectGlobal — resolve a key/range across the cluster

```text
rpc InspectGlobal(InspectGlobalRequest) returns (stream InspectRow)
```

Resolves a key or range **across the cluster** (and across peer clusters when global
replication is enabled), answering "who holds this, at what version, with what conflict/lag".
Built on the **holder directory** + `ReplicationService.FetchReplica` (`11_api_contracts.md`)
and the global replication status (`06_global_active_active_replication.md`):

1. consult the range directory / holder summaries to find candidate holders for the
   key/range (`04_membership_latency_gossip.md` "Range directory", "Holder summaries");
2. `FetchReplica` from reachable holders (bounded fan-out, with deadlines);
3. for each key, emit an `InspectRow` listing **every known holder** with that holder's
   `Version`, holder class, and `ConflictState`/`siblings`, plus per-holder
   `replication_lag_ms` and, when global is enabled, peer-cluster presence and
   `global_repl_lag_ms`.

This is explicitly **eventual / best-effort**. The `header`'s `ResponseMeta` uses
`source = FETCHED_CLOSEST_HOLDER` / `GLOBAL_REMOTE` and the **`trailer` carries
`Completeness`**: `COMPLETE` only if every candidate holder answered, otherwise `PARTIAL` /
`BEST_EFFORT` with `warnings` naming unreachable holders, stale holder summaries
(`04` "Holder summary staleness"), and any clusters skipped. The UI must surface this
completeness metadata so a partial answer is never mistaken for a global truth.

### GetClusterView — membership + latency snapshot

```text
rpc GetClusterView(GetClusterViewRequest) returns (GetClusterViewResponse)
```

A unary snapshot of the membership roster (liveness state, node/zone/region/geo, storage UUID)
and the directed latency graph (`LatencyEdge` list: EWMA/p95 RTT, packet loss, sample count),
plus per-range under-replication / repair pressure (`23_repair_engine.md`,
`14_observability.md` "repair"). This feeds the topology view and seeds the live edges that
`StreamGossip` (kind `latency_edge`) then keeps current. It is the same data as
`AdminService.GetMembership` + `GetLatencyGraph`, returned in one snapshot shaped for the UI.

## UI views

### (a) Gossip Inspector

A live table of `GossipEvent`s (newest at top): time, direction, peer, kind, payload summary,
size. Controls:

- **filter bar** mapping to `GossipFilter` (kind multi-select, peer picker from the roster,
  direction toggle, namespace/range box) — applied server-side, so changing a filter reopens
  the stream;
- **pause/resume** (client-side: stop appending, keep the buffer; resume re-tails — combined
  with `backfill` so no events are lost across a pause);
- **gap markers** rendered inline when the server drops events under backpressure;
- **row drill-down** showing the full decoded payload summary for one event.

### (b) Data Browser

Two tabs over a shared key-detail panel:

- **Local** — drives `InspectLocal`: pick a keyspace (`/kv`, `/graph`, `/vector`, `/sys`,
  `/repl`, `/cache`), enter a prefix or range, stream rows.
- **Global** — drives `InspectGlobal` for a key/range; shows the per-holder table and a
  prominent **completeness banner** (complete / partial / best-effort + warnings).

The **key detail** panel shows version, holder class, conflict state, siblings, TTL, and (in
the global tab) the holders and replication/global lag. Values are redacted by default
(see below).

### (c) Cluster / Topology

A topology view fed by `GetClusterView` and kept live by `StreamGossip` (`latency_edge`,
`suspect`/`alive`/`membership_delta`): members as nodes coloured by liveness state, directed
latency edges labelled with EWMA/p95 RTT, and a repair / under-replication overlay
(`23_repair_engine.md`). Spot churn and drain states (`04` "Spot node handling", "Drain
protocol") are visible as they propagate.

### (d) Metrics summary

A compact panel of the headline counters/gauges (`14_observability.md` — gossip members
alive/suspect, origin+1 latency, under-replicated keys, global lag, conflict rate) with a link
out to the node's Prometheus `/metrics`. The UI does not re-implement Prometheus; it links to
it.

## Security and redaction

The UI inherits the admin surface's auth (`15_security.md`) and the project-wide redaction
rule:

- **Values redacted by default.** `InspectLocal`/`InspectGlobal` rows carry the
  `key_hash = base64url(blake3(namespace + key))` (`15_security.md` "Data redaction"), and
  omit raw values and raw sibling bytes. The same applies to gossip payloads: only decoded
  *summaries* (RTT, watermarks, counts, states) are streamed, never raw record bytes.
- **Reveal is opt-in and privileged.** Showing a raw value requires the `admin` role *and* an
  explicit per-request `include_value=true` (mirroring `/admin/key`'s `includeValue=true`),
  and is audit-logged. Without both, the value field stays empty.
- **Never log raw values.** UI access and reveal actions are logged with redaction; raw keys
  and values never appear in logs or in the gossip ring buffer
  (`14_observability.md` "Structured logs", "Do not log raw keys or values by default").

## Performance and resource bounds

- **Bounded gossip ring buffer** (default 4096 events) caps memory regardless of UI presence;
  buffering happens whether or not anyone is subscribed, so backfill is always available.
- **Server-side filtering** keeps stream volume proportional to what the operator asked for,
  not to total gossip traffic.
- **Backpressure = drop-oldest + gap marker** (per "StreamGossip"): a slow or stalled browser
  never applies backpressure to the gossip agent or the data path.
- **Inspect caps:** `InspectLocal`/`InspectGlobal` enforce a server-side max row count and
  per-row size, and `InspectGlobal` bounds holder fan-out and applies deadlines, so a wide
  range scan from the UI cannot overload the node or its peers. Truncation is reported via the
  trailer's `Completeness`.
- **One listener:** the UI adds no new port and no second TLS config; it rides the existing
  admin listener and its auth.

## Implementation checklist

- [ ] Vite + React + TS SPA scaffolded under `ui/`, building to `internal/ui/dist`.
- [ ] `go:embed` of `dist` and SPA-fallback static handler implemented (`internal/ui`).
- [ ] Dev-mode Vite reverse proxy gated by `WAVESPAN_UI_DEV`.
- [ ] UI + `ObservabilityService` mounted on the admin port behind admin auth.
- [ ] `ObservabilityService` proto + `connect-go` handlers implemented.
- [ ] Bounded gossip ring buffer + server-side `GossipFilter` + drop-oldest gap markers.
- [ ] `InspectLocal` over `LocalStore` scan with redaction and trailer completeness.
- [ ] `InspectGlobal` over holder directory + `FetchReplica` with completeness metadata.
- [ ] `GetClusterView` snapshot wired to membership + latency graph + repair status.
- [ ] Values redacted by default; reveal gated by admin role + explicit opt-in + audit log.
- [ ] `buf generate` produces both `connect-go` and `connect-es`; Vite build feeds `go:embed`.
