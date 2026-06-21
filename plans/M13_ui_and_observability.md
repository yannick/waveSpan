# M13 - Embedded node UI and live observability

**Milestone:** M13 (post-roadmap; see `design/18_implementation_roadmap.md` — extends the
management plane delivered across M2/M4 with a live UI surface)
**Depends on:** M0 (buf/Connect toolchain, admin server, `CGO_ENABLED=0` scratch build),
M2 (gossip/membership/latency graph to observe), M3 (KV data to inspect)
**Richer after:** M4 (target-N/repair pressure for the topology overlay), M7 (global
replication lag for `InspectGlobal` peer-cluster rows)
**Enables:** operator-facing live introspection; the same `ObservabilityService` surface the
correctness harness (`design/25_*`) and `wavespanctl` consume.

## Context

Every `wavespan-node` serves a small embedded web UI plus a streaming Connect surface for live
introspection (`design/26_node_ui_and_observability.md`). Two capabilities are required:
**watch the gossip protocol live** (subscribe + filter), and **inspect local and global data**.

The UI is a **Vite + React + TypeScript** SPA, built to static assets and embedded into the
node binary via `go:embed` so it ships inside the `FROM scratch` image (`design/24_*`) with no
external files. Transport is **ConnectRPC**: the *same* protos drive internal RPC and the
browser, via buf-generated `connect-go` (server) and `connect-es` + `@connectrpc/connect-web`
+ `@connectrpc/connect-query` (client). Live updates use Connect **server-streaming** RPCs, not
a separate WebSocket layer. The UI and `ObservabilityService` mount on the **admin port (7900)**
behind admin auth (`design/15_security.md`); values are redacted by default.

The buf Connect toolchain and admin server already exist from M0 (`plans/M00_bootstrap.md`:
`buf.gen.yaml` with `protocolbuffers/go` + `connectrpc/go`; `make proto`); M13 adds the
`connect-es` plugin and a new proto file, the Go handlers, the embed/serve glue, and the SPA.

## Files to create

```
proto/wavespan/v1/observability.proto        ObservabilityService + GossipEvent/InspectRow/ClusterView messages (design/11)
buf.gen.yaml                                  EXTEND: add connectrpc/es + protocolbuffers/es plugins -> ui/src/gen
internal/observability/gossipring.go          bounded ring buffer of GossipEvent; subscribe/backfill/tail; drop-oldest + GapMarker
internal/observability/gossiptap.go           hook into internal/membership gossip agent -> emit GossipRecord (decoded summaries, redacted)
internal/observability/obsservice.go          connect-go handler: StreamGossip/InspectLocal/InspectGlobal/GetClusterView
internal/observability/inspect_local.go       LocalStore scan -> InspectRow; logical-path decode; redaction; trailer completeness
internal/observability/inspect_global.go      holder directory + FetchReplica fan-out -> InspectRow; completeness metadata
internal/observability/obsservice_test.go     filter matching, ring backfill/drop, redaction default, completeness on missed holder
internal/ui/embed.go                          //go:embed dist -> embedded fs.FS (build tag for dev)
internal/ui/server.go                          SPA static handler (index fallback, cache headers) + Vite dev reverse proxy
internal/ui/server_test.go                     embedded asset served; SPA fallback; dev-proxy gated by WAVESPAN_UI_DEV
ui/package.json                                Vite + React + TS + @connectrpc/connect-web + connect-query + @tanstack/react-query
ui/vite.config.ts                              build.outDir = ../internal/ui/dist; dev server :5173
ui/tsconfig.json
ui/index.html
ui/src/main.tsx                                React root + Connect transport (createConnectTransport, same-origin)
ui/src/transport.ts                            connect-web transport against the admin port; auth header/credentials
ui/src/gen/                                    connect-es generated client stubs (buf output; gitignored or committed)
ui/src/views/GossipInspector.tsx               live table + GossipFilter controls + pause/resume + gap markers + drill-down
ui/src/views/DataBrowser.tsx                   Local/Global tabs; prefix/range search; key-detail panel; completeness banner
ui/src/views/ClusterTopology.tsx               members + liveness + latency edges + repair overlay (GetClusterView + StreamGossip)
ui/src/views/MetricsSummary.tsx                headline counters + link to /metrics
```

## Files to modify

```
proto/wavespan/v1/admin.proto                  ensure Member/LatencyEdge/HolderSummary are importable by observability.proto (M2 defined them)
cmd/wavespan-node/main.go                       register ObservabilityService handler + mount internal/ui on the admin listener
internal/membership/gossip.go                   call the gossiptap hook on send/receive/state-change (no behavior change if no subscribers)
internal/security/...                            admin-auth interceptor covers ObservabilityService; admin-role gate for include_value
Makefile                                         `ui` (npm ci && vite build -> internal/ui/dist), `ui-dev`; `proto` also runs connect-es; `build` depends on `ui`
buf.gen.yaml                                     connect-es + es plugins (see above)
docker/docker-compose.yaml                       map each node's admin port (7900) to host so the UI is reachable in the 3-node cluster
```

## Steps

1. **Proto, `proto/wavespan/v1/observability.proto`.** Author `ObservabilityService` and its
   messages exactly as specified in `design/11_api_contracts.md` "Observability service":
   `StreamGossip`/`InspectLocal`/`InspectGlobal`/`GetClusterView`, the `GossipFilter` /
   `GossipKind` / `GossipDirection` enums, `GossipEvent`/`GossipRecord`/`GossipPayloadSummary`/
   `GapMarker`, `InspectRow`/`InspectKey`/`InspectHolder`/`InspectSibling`, and
   `GetClusterViewResponse`/`RangeRepairStatus`. Import `common.proto` (M0) and `admin.proto`
   (M2) to reuse `Version`/`ResponseMeta`/`Completeness`/`ConflictState`, `Member`,
   `LatencyEdge`, `HolderSummary`. `make proto` regenerates `connect-go` cleanly.

2. **Codegen for the browser, `buf.gen.yaml`.** Add the `protocolbuffers/es` (or
   `bufbuild/es`) and `connectrpc/es` plugins, output into `ui/src/gen`. `make proto` now
   produces both `connect-go` (server) and `connect-es` (client) from the same protos; CI's
   proto-drift gate (M0) extends to the generated TS.

3. **Gossip ring buffer, `internal/observability/gossipring.go`.** A bounded ring (default
   4096, configurable) of `GossipRecord`s. `Subscribe(filter, backfill)` returns a channel:
   when `backfill`, first replay matching buffered records (oldest→newest, `backfill=true`),
   then tail live. Apply the `GossipFilter` **server-side** before enqueue. Per-subscriber
   queue is bounded; on overflow **drop oldest** and emit one `GapMarker` (`dropped_count`,
   `since_unix_ms`). Producing into the ring must never block the gossip agent
   (`design/26_*` "Performance and resource bounds").

4. **Gossip tap, `internal/observability/gossiptap.go` + `internal/membership/gossip.go`.**
   Add a tap hook the membership/gossip agent calls on ping/ack, indirect probe, liveness
   transition, holder-summary exchange, latency-edge update, and membership delta
   (`design/04_membership_latency_gossip.md`). The tap builds a `GossipRecord` with a **decoded
   payload summary only** (RTT, new state, watermark/approx-count, EWMA/p95, added/removed
   members) and `payload_size_bytes` — **never raw record bytes** (`design/15_security.md`).
   When no subscribers exist the tap still feeds the ring (so backfill is available) but does
   no extra work beyond that.

5. **Service handler, `internal/observability/obsservice.go`.** Implement the `connect-go`
   `ObservabilityServiceHandler`. `StreamGossip` wires request filter+backfill to the ring and
   streams `GossipEvent`s (header-first/trailer-last like the other streams,
   `design/11` "API requirements"). `GetClusterView` snapshots membership (roster + liveness),
   the latency graph (`LatencyEdge`s), and per-range repair pressure (`design/23_repair_engine.md`;
   stubbed/zero before M4).

6. **InspectLocal, `internal/observability/inspect_local.go`.** Map `Keyspace` +
   `prefix`/`[start,end)` to a `LocalStore` scan (`internal/storage`) over the logical keyspace
   (`design/01_architecture.md` "Internal keyspace"). For each key emit an `InspectKey`:
   decoded `logical_path`, `key_hash = base64url(blake3(namespace+key))`, `Version`,
   `ConflictState`, siblings (version + tombstone), TTL, and one self `InspectHolder` with its
   `HolderClass` (origin/durable/dynamic-cache from the holder record). **Redact values by
   default**; populate `value`/sibling bytes only when `include_value && admin role`. Header
   `ResponseMeta.source = LOCAL_DURABLE`/`LOCAL_DYNAMIC_CACHE`; trailer `final_completeness`
   `COMPLETE` unless truncated at `limit` (then `PARTIAL`). Enforce a server-side row cap.

7. **InspectGlobal, `internal/observability/inspect_global.go`.** Resolve candidate holders via
   the range directory / holder summaries (`design/04_*`), `FetchReplica`
   (`design/11`, `ReplicationService`) from reachable holders with bounded fan-out + deadlines,
   and emit one `InspectKey` per key listing **every known holder** (`InspectHolder` with
   version, class, conflict, `replication_lag_ms`, and — when `include_peer_clusters` and
   global enabled — peer-cluster presence + `global_repl_lag_ms`,
   `design/06_global_active_active_replication.md`). This is **eventual/best-effort**: header
   `source = FETCHED_CLOSEST_HOLDER`/`GLOBAL_REMOTE`; trailer `final_completeness` is `COMPLETE`
   only if every candidate answered, else `PARTIAL`/`BEST_EFFORT` with `warnings` naming
   unreachable holders, stale summaries (`design/04` "Holder summary staleness"), and skipped
   clusters. Redaction as in step 6.

8. **Auth + redaction wiring, `internal/security`.** The existing admin-auth interceptor
   (`design/15_security.md`) must cover all `ObservabilityService` RPCs at the `reader` role;
   `include_value=true` requires the `admin` role and is audit-logged, otherwise the value
   field stays empty. Honor `insecureDevMode` exactly as the rest of the admin surface does.

9. **Embed + serve, `internal/ui/embed.go` + `server.go`.** `//go:embed dist` exposes the Vite
   build as an `fs.FS`; `server.go` serves it with SPA fallback to `index.html` (`no-cache`),
   long-lived cache for content-hashed `/assets/*`. When `WAVESPAN_UI_DEV=1` (or `--ui-dev`),
   reverse-proxy asset requests to the Vite dev server (`http://localhost:5173`) instead of the
   embedded FS; RPC requests always hit the in-process handlers. A build tag keeps `dist`
   optional during early dev so `go build` works before the first `vite build`.

10. **Mount on the admin listener, `cmd/wavespan-node/main.go`.** Register the
    `ObservabilityService` handler and mount `internal/ui` on the **same admin HTTP/2 listener
    (7900)** that already serves `/admin/*` and `AdminService`. Connect content negotiation lets
    gRPC internal callers and the browser (`connect-web`) share the routes. No new port, no
    second TLS config (`design/26_*` "Port and auth").

11. **SPA scaffold, `ui/`.** Vite + React + TS. `transport.ts` builds a `connect-web` transport
    against the same origin (admin port), forwarding the admin token / credentials.
    `@connectrpc/connect-query` + `@tanstack/react-query` drive the generated client. Four views:
    - **GossipInspector** — live table bound to `StreamGossip`; filter bar mapped to
      `GossipFilter` (kind multi-select, peer picker from the roster, direction toggle,
      namespace/range); pause/resume (client-side, resume re-tails with `backfill`); inline gap
      markers; row drill-down of the decoded summary.
    - **DataBrowser** — Local/Global tabs over `InspectLocal`/`InspectGlobal`; keyspace picker,
      prefix/range input; key-detail panel (version/holders/siblings/TTL); a prominent
      **completeness banner** on the Global tab.
    - **ClusterTopology** — `GetClusterView` snapshot kept live by `StreamGossip`
      (`latency_edge`/`suspect`/`alive`/`membership_delta`): members coloured by liveness,
      directed latency edges labelled EWMA/p95, repair/under-replication overlay.
    - **MetricsSummary** — headline counters (`design/14_observability.md`) + link to `/metrics`.

12. **Build wiring, `Makefile` + `docker/docker-compose.yaml`.** `make ui` runs `npm ci &&
    vite build` into `internal/ui/dist`; `make ui-dev` starts Vite; `make build` depends on
    `ui` so the embedded assets are present (and `CGO_ENABLED=0` is preserved, `design/24_*`).
    Map each node's admin port to the host in compose so the UI is reachable across the 3-node
    cluster.

## Acceptance criteria

- `wavespan-node` **serves the UI on the admin port (7900)** behind admin auth; the SPA loads
  and its assets are served from the embedded FS (no external files).
- **Gossip is subscribable and filterable live**: in a 3-node Apple-`container` cluster
  (`design/24_*`), the Gossip Inspector shows live ping/ack/suspect/alive/holder-summary/
  latency-edge/membership-delta events; applying a kind/peer/direction filter narrows the
  stream server-side; killing a node produces visible `suspect`→`unreachable` events.
- **Local and global data are browsable**: the Data Browser lists local keys by prefix/range
  with version/holder-class/conflict/TTL; the Global tab resolves a key across the cluster
  showing per-holder versions and a completeness banner (partial when a holder is unreachable).
- **Values are redacted by default**; reveal requires admin role + explicit `include_value`
  and is audit-logged; no raw values appear in logs or the gossip ring.
- **UI assets are embedded in the scratch image** (`design/24_*`): the `FROM scratch` node
  image serves the UI with no mounted asset files.
- **Build wiring:** `buf generate` produces both `connect-go` and `connect-es`; `vite build`
  feeds `go:embed`; `make build` is reproducible with `CGO_ENABLED=0`.

## Verification

1. **Unit (Go):**
   - ring buffer: backfill replays in order; drop-oldest emits exactly one `GapMarker` with the
     correct `dropped_count`; producing never blocks when a subscriber is full.
   - `GossipFilter` matching: kind/peer/direction/namespace combinations select the right events.
   - redaction: `InspectLocal`/`InspectGlobal` omit value bytes unless `include_value` **and**
     admin role; `key_hash` is always present.
   - `InspectGlobal` completeness: simulate one unreachable holder -> trailer `PARTIAL`/
     `BEST_EFFORT` with a warning naming the holder.
   - `internal/ui/server`: embedded asset served; unknown route falls back to `index.html`;
     dev proxy engaged only when `WAVESPAN_UI_DEV=1`.
2. **Frontend:** `tsc --noEmit` and a Vite production build succeed; generated `connect-es`
   client typechecks against `observability.proto`; component tests for filter-bar -> request
   mapping and the completeness banner.
3. **Container integration (`design/24_*`, 3-node Apple-`container` / docker-compose cluster):**
   - bring up 3 nodes; `GET https://<node>:7900/` returns the SPA; assets load from the
     embedded FS.
   - open `StreamGossip` (via `wavespanctl` or the browser) and assert live events flow; apply a
     `kinds=[suspect,alive]` filter and confirm only those arrive.
   - kill one node; confirm the survivors' streams emit `suspect` then `unreachable`, and the
     topology view recolours.
   - write a key (M3), then `InspectLocal` on its holder shows it; `InspectGlobal` from a
     non-holder resolves it across holders with completeness metadata; with a holder paused the
     answer is `PARTIAL`.
4. **Proto + build gates:** `make proto && git diff --exit-code proto/ ui/src/gen/` is clean
   (Go + connect-es); `make build` with `CGO_ENABLED=0` produces a scratch image that serves the
   UI with no external asset files (`design/24_*` no-CGO/scratch gate).
