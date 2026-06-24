# gRPC Data-Plane Migration — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Move the WaveSpan data plane from Connect (connectrpc over net/http2) to **real grpc-go** for the
~1.9× transport win measured in the spike. Data port becomes a pure `grpc.Server` serving both SDK
clients and inter-node RPCs; all 9 internal client sites move to grpc-go; the **browser UI keeps Connect
(gRPC-Web) on the admin port** so it can be disabled independently. The Go SDK moves to grpc-go.

**Decision (confirmed):** full grpc-go data plane (not a separate client port). Cutover is coordinated on
this branch and validated end-to-end before merge.

## Target architecture

| Port | Server | Serves |
|---|---|---|
| **data** (`cfg.Ports.Data`) | `grpc.Server` (grpc-go) + interceptors | Kv, Collection, Cypher, Vector (clients) **and** Replication, Config, Global, cache subscribe/fetch (peers) |
| **admin** (`cfg.Ports.Admin`) | Connect `http.Server` (unchanged) | Browser UI (gRPC-Web): Observability, Cypher, Collection (already there) + the SPA |
| gossip | unchanged | gossip (its own port/transport) |

Internal node→node clients dial peers' **data** ports with grpc-go. mTLS uses the existing `serverMTLS`
(`credentials.NewTLS`); dev mode = insecure.

## Codegen (DONE in commit a072d48)
`buf.gen.yaml` now runs `protoc-gen-go-grpc`; `proto/wavespan/v1/*_grpc.pb.go` exist; grpc bumped to 1.81.

---

## Task 1: gRPC server infrastructure — interceptors + builder

**Files:** `internal/grpcsrv/server.go`, `internal/grpcsrv/auth.go`, `internal/grpcsrv/auth_test.go` (new).

Port the two net/http concerns the data `http.Server` runs into grpc interceptors:
- **Auth** (from `security.Identity.EnforceHTTP`, `internal/security/middleware.go`): derive role from the
  mTLS peer cert (`peer.FromContext(ctx)` → `credentials.TLSInfo.State.PeerCertificates[0]` → `id.roleForCert`)
  or, in `DevMode`, from incoming metadata `x-wavespan-role` (default `RoleAdmin`). Then
  `security.Allowed(role, security.SurfaceForProcedure(info.FullMethod))` — note `FullMethod` is
  `/wavespan.v1.KvService/Get`, the same shape `SurfaceForProcedure` already parses. On deny return
  `status.Error(codes.Unauthenticated|PermissionDenied, …)`. Inject the role via `security.WithRole(ctx, role)`.
  Provide BOTH a `UnaryServerInterceptor` and a `StreamServerInterceptor` (wrap the stream's context).
- **Metrics:** port the connect `metricsInterceptor` (QPS/reads/writes) — reuse the same counters via a small
  grpc unary interceptor classifying read vs write by method name (mirror the connect one in `internal/rpcopts`).

- [ ] **Step 1: failing test** `auth_test.go` — table test of the auth decision: dev-mode default→admin allowed;
  a read method with `RoleReader` allowed, a write method denied; `RoleNone` (no cert, not dev) →
  Unauthenticated. Drive a tiny fake `UnaryHandler` through the interceptor with synthesized `ctx`/`FullMethod`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3:** implement `auth.go` (the two interceptors + role derivation) and `server.go`:
  `func New(opts Options) *grpc.Server` building `grpc.NewServer(creds, chainUnary(auth, metrics), chainStream(auth))`.
  `Options{ TLS *tls.Config; DevMode bool; Roles security.Identity; Metrics ... }`.
- [ ] **Step 4:** `go test ./internal/grpcsrv/ && go build ./...` → clean.
- [ ] **Step 5:** commit.

## Task 2: gRPC service adapters — client-facing (Kv, Collection, Cypher, Vector)

**Files:** `internal/grpcsrv/kv.go`, `collections.go`, `cypher.go`, `vector.go` (+ tests). One adapter per
service implementing the generated `XServiceServer`, embedding `UnimplementedXServiceServer`, delegating to
the SAME cores the Connect `Service` does (the Connect services are thin — mirror their bodies). Reference:
the spike `internal/grpckv` (gone from main, see branch `spike/resp-proto`) showed the exact KV delegation.
- Kv: Get/Put/Delete/MultiGet + **Scan** (server-streaming: adapt `grpc.ServerStreamingServer[ScanResponse]`).
- Cypher: **Query** (server-streaming) + any scatter methods peers call.
- Collection: all SAdd/…/BulkRemove/TierInfo/AdmitLearner/ProposeForward.
- Vector: its RPCs.
- [ ] Per service: a construction test (adapter satisfies the `XServiceServer` interface) + delegation smoke
  where feasible; build; commit. Keep each service its own unit/commit.

## Task 3: gRPC service adapters — inter-node (Replication, Config/Observability, Global)

**Files:** `internal/grpcsrv/replication.go`, `config.go`, `global.go` (+ streaming: SubscribeKey, FetchRange).
Same pattern, delegating to the existing cores. These are what peers call.
- [ ] Per service: interface-satisfaction test; build; commit.

## Task 4: gRPC client helper + migrate the 9 internal client sites

**Files:** `internal/rpcopts/grpc.go` (new: a pooled `GRPCConn(addr) *grpc.ClientConn`, one cached conn per
peer, insecure or `serverMTLS` creds), then edit the 9 sites to use `wavespanv1.NewXClient(GRPCConn(addr))`
instead of `wavespanv1connect.NewXClient(H2CClient(), …)`:
`internal/cache/{fetch,subscribe}.go`, `internal/cypher/scatter.go`, `internal/replication/local/connect.go`,
`internal/replication/global/{sender,reconcile}.go`, `internal/membership/connect.go`,
`internal/collections/*` (forwarder/admitter), `internal/bench/{latency,collections}.go`.
- [ ] grpc conn-pool test (same addr → same conn; race-clean). Migrate sites; `go build ./... && go test` per
  area. Commit in logical groups (cache, replication, collections, cypher, membership, bench).
- [ ] NOTE: streaming clients (SubscribeKey, FetchRange) become grpc client streams — adapt the receive loops.

## Task 5: server bootstrap — flip data port to grpc, move UI Connect to admin

**Files:** `cmd/wavespan-node/main.go`.
- [ ] Replace the data `http.Server` (lines ~507-637: `dataMux`, `EnforceHTTP`, `maybeH2C`, `dataSrv`) with
  `grpcsrv.New(...)`, register all data-plane services, `grpcServer.Serve(dataLn)`. mTLS via `serverMTLS`.
- [ ] Ensure the admin port still serves the UI Connect services (Observability/Cypher/Collection already at
  lines ~703-709). Add Kv/Vector Connect handlers to the admin mux IF the UI calls them (check `ui/src`); the
  SPA must keep working via gRPC-Web. Keep the Connect `*Service.Handler()` for those (they already exist).
- [ ] Graceful shutdown: `grpcServer.GracefulStop()` in the shutdown path.
- [ ] `go build ./... && go vet`. Commit.

## Task 6: SDK → grpc-go (in-repo `sdk/go` + standalone `wavespan-sdk`)

**Files:** `sdk/go/*` (and mirror to the standalone repo afterwards).
- [ ] Vendor grpc stubs into the SDK (regenerate its `internal/gen` with protoc-gen-go-grpc alongside the
  message stubs; drop the connect stubs).
- [ ] `client.go`: `Dial` builds a `grpc.ClientConn` (grpc.NewClient with insecure/TLS creds) instead of the
  Connect httpc; the typed wrappers call grpc client methods. Streaming (Scan/Query) → grpc client streams.
- [ ] `go test ./...` in the SDK module; the quickstart still compiles. Commit.
- [ ] Sync to the standalone `wavespan-sdk` repo as v0.1.0 (breaking: transport change) — separate step after merge.

## Task 7: end-to-end + CI

- [ ] Spin a local 3-node cluster from this branch; verify KV Get/Put, a Cypher query, collections SAdd +
  BulkRemove, and replication all work over grpc; verify the browser UI still loads/queries via the admin port.
- [ ] CI: ensure the proto gate installs/runs `protoc-gen-go-grpc`; build/test green. Commit.

## Final verification
- [ ] `go build ./... && go test ./... -race` green; `golangci-lint run` 0 issues; `gofmt -l` clean.
- [ ] UI builds (`npm run build` + `build:bench`) and works against the admin port.
- [ ] Bench delta confirmed (grpc data path faster than the prior Connect path).
