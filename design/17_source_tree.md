# 17. Source tree

## Recommended repository layout

WaveSpan is a Go multi-module repository. The data node, gateway, CLI, and operator are all Go; the local engine `wavesdb` is brought in via a `replace` directive against the sibling checkout (no FFI, no C build).

```text
waveSpan/
  go.mod                # module github.com/yannick/wavespan, go 1.26
                        # replace wavesdb => ../wavesdb
  README.md
  docs/
    design/
      *.md
  buf.yaml              # buf module + lint/breaking config
  buf.gen.yaml          # codegen: protocolbuffers/go + connectrpc/go (server), connectrpc/es (UI)
  proto/
    wavespan/v1/common.proto
    wavespan/v1/kv.proto
    wavespan/v1/cypher.proto
    wavespan/v1/replication.proto
    wavespan/v1/admin.proto
    wavespan/v1/observability.proto   # ObservabilityService: live gossip + data inspection (doc 26)
  cmd/
    wavespan-node/      # data pod process
    wavespan-gateway/   # optional stateless gateway
    wavespanctl/        # admin/client CLI
  internal/
    config/             # YAML/env config loading and validation
    version/            # HLC, vector clocks, version comparison
    storage/            # LocalStore interface + wavesdb adapter
    membership/         # SWIM-style gossip, liveness, member metadata
    latencygraph/       # latency probes, EWMA/p95, edge expiration
    placement/          # replica candidate filtering and scoring
    replication/
      local/            # origin+1 write path, target-N fanout
      global/           # active-active streams, anti-entropy
    cache/              # dynamic key/range subscriptions, eviction
    kv/                 # public KV API, key encoding, TTL integration, scans
    conflict/           # conflict resolver interface and built-in policies
    ttl/                # TTL buckets, sweeper, tombstone emission
    graph/              # graph records, indexes, adjacency encoding
    cypher/
      parser/           # Cypher subset parser and AST
      planner/          # logical/physical plans and distributed fragments
    vector/             # raw vector store, exact search, ANN, delta index
    observability/      # metrics, tracing, logs, admin diagnostics
    security/           # mTLS, auth middleware, redaction helpers
    ui/                 # go:embed of ui/dist + Connect ObservabilityService handlers (doc 26)
      dist/             # built Vite assets, embedded into wavespan-node (gitignored; CI builds it)
  ui/                   # Vite + React + TypeScript frontend source (doc 26)
    src/                # components: gossip inspector, data browser, cluster/topology views
    gen/                # connect-es generated client stubs (buf)
    package.json
    vite.config.ts
  operator/             # SEPARATE go.mod, Kubebuilder
    go.mod              # module github.com/yannick/wavespan-operator
    api/
    controllers/
    config/
    charts/
  container/            # Apple `container` local dev/test scripts (doc 24, primary local path)
  docker/
    docker-compose.yaml # portable / Linux-CI path (doc 24)
    scripts/
  tests/
    integration/
    chaos/
    harness/            # model-aware correctness harness: workloads, nemeses, checkers, runner (doc 25)
  fixtures/
```

## Build, codegen, and UI

- **No CGO.** All binaries build with `CGO_ENABLED=0`, statically linked, shipped from `FROM scratch`
  images, multi-arch (linux/arm64, linux/amd64). The forbidden edge `any internal/ package -> cgo`
  below is enforced for this reason. See `24_container_dev_and_testing.md`.
- **Codegen is buf.** `buf generate` produces Go message + `connect-go` server stubs into the root
  module and `connect-es` client stubs into `ui/gen`. The same protos drive internal RPC and the UI.
- **UI is embedded.** `ui/` (Vite + React + TS) builds to `internal/ui/dist`, embedded via `go:embed`
  so the scratch image carries no external web assets. Served on the admin port behind admin auth.

## Language and modules

WaveSpan is a single-language Go system. There is no Rust crate tree and no C-FFI binding; `wavesdb` is a Go library imported directly. See `adr/0005_go_and_wavesdb_engine.md` for the pivot rationale.

There are two Go modules:

- the root module `github.com/yannick/wavespan` — node, gateway, CLI, and all `internal/` packages. It depends on `wavesdb` via `replace wavesdb => ../wavesdb` until `wavesdb` is published;
- `operator/` is a **separate module** (`github.com/yannick/wavespan-operator`) so the Kubebuilder/controller-runtime dependency tree does not leak into the data-plane build and so the operator can be versioned and released independently. The operator depends only on generated config/API types, never on data-node internals.

The C engine `tidesdb` is reference-only and is not vendored or built.

## Package responsibilities

### `internal/storage`

Defines the `LocalStore` interface, record envelopes, snapshots, and scan abstractions, and implements them over `wavesdb` (see `02_storage_wavesdb.md`). This is the only package that imports `wavesdb`.

### `internal/version`

HLC clock, vector clocks, and version comparison used by replication and conflict resolution (see `22_versioning_and_hlc.md`).

### `internal/membership`

SWIM-style gossip, liveness, member metadata.

### `internal/latencygraph`

Latency probes, EWMA/p95 calculations, edge expiration.

### `internal/placement`

Replica candidate filtering and scoring.

### `internal/replication/local`

Origin+1 write path, target-N fanout, StoreReplica/FetchReplica services.

### `internal/replication/global`

Active-active outbound/inbound streams, anti-entropy, global conflict application.

### `internal/cache`

Dynamic key/range subscriptions, update streams, cache eviction.

### `internal/conflict`

Conflict resolver interface and built-in policies.

### `internal/kv`

Public KV API, key encoding, TTL integration, range scans.

### `internal/ttl`

TTL buckets, sweeper, tombstone emission (works with `wavesdb` native per-key TTL; see `02_storage_wavesdb.md`).

### `internal/graph`

Graph records, indexes, adjacency encoding, graph mutation application.

### `internal/cypher/parser`

Cypher subset parser and AST.

### `internal/cypher/planner`

Logical/physical plans and distributed fragments.

### `internal/vector`

Raw vector store, exact search, ANN abstraction, delta index.

### `internal/observability`

Metrics, tracing, logs, admin diagnostics.

### `internal/security`

mTLS, auth middleware, redaction helpers.

## Dependency direction

Direction is enforced two ways: Go's `internal/` visibility keeps these packages out of external importers (including the operator module), and an import-lint check in CI rejects the forbidden edges below.

Allowed:

```text
kv -> replication/local -> placement -> membership -> storage
kv -> cache -> storage
graph -> kv, storage
cypher/planner -> graph, vector
vector -> storage
replication/global -> conflict -> storage
* -> version, observability, config
```

Forbidden:

```text
storage -> kv            (storage must not know about higher layers)
storage -> wavesdb consumers other than itself
membership -> kv
operator -> any internal/ package (operator sees only generated config/API types)
any internal/ package -> tidesdb / cgo
```

## Build artifacts

```text
wavespan-node      # data pod process
wavespan-gateway   # optional stateless gateway
wavespanctl        # admin/client CLI
wavespan-operator  # Kubernetes controller manager (built from operator/)
```

## Configuration file

All runtime config is loaded from YAML/env.

```yaml
clusterId: dev
memberId: node1
storage:
  path: /var/lib/wavespan
  engine: wavesdb   # the in-process Go library; not a swappable backend in v1
membership:
  runtime: docker
  seeds: ["node1:7700", "node2:7700"]
replication:
  policyRef: local-cache-default
security:
  insecureDevMode: true
```

`storage.engine: wavesdb` is documentation of intent and a guard against silent misconfiguration; v1 has exactly one engine and it is linked in at build time, not selected at runtime.

## Implementation rule

Every package must include:

- public interface documentation;
- unit tests;
- metrics hooks where applicable;
- fault injection hooks for integration tests.
