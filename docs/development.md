# Development

## Layout

```
waveSpan/
  cmd/
    wavespan-node/      data pod (everything runs here)
    wavespan-gateway/   stateless gateway (stub)
    wavespanctl/        admin/client CLI
  internal/
    config/             YAML + WAVESPAN_ env loading, fail-fast validation
    version/            HLC clock, Version compare, writer sequence, mutation id
    storage/            LocalStore over wavesdb + in-memory impl (one conformance suite)
    recordstore/        versioned-record primitive: keys, atomic apply, reads, range scan, TTL index
    membership/         SWIM gossip, liveness, Connect transport
    latencygraph/       EWMA/p95 latency edges, scoring, topology penalty
    placement/          replica candidate filter + scoring
    replication/local/  StoreReplica, holder directory, fanout, repair, idempotency
    cache/              holder directory (bloom), FetchReplica, dynamic cache, subscriptions, eviction, range certs
    kv/                 KvService: coordinator, reader, scanner
    ttl/                lazy TTL sweeper + read filter
    observability/      slog, Prometheus, health
  proto/wavespan/v1/    protobufs + generated Go + Connect stubs
  operator/             Kubernetes operator (separate go.mod; later milestone)
  docker/  container/   Docker Compose + Apple container scripts
  tests/integration/    docker-based integration tests (build tag: integration)
  docs/  design/  plans/ documentation, design spec, milestone plans
```

The data node imports the sibling `wavesdb` engine in-process via `replace wavesdb => ../wavesdb`.
There is no FFI and no C toolchain; everything is `CGO_ENABLED=0`.

### Dependency direction

`internal/` visibility plus an import-lint convention keep the layering honest, roughly:

```
kv → cache, replication/local → placement → membership → storage
kv, cache, replication/local → recordstore → storage
* → version, observability, config
```

`recordstore` is the only package that knows the on-disk key layout; `storage` is the only one that
imports `wavesdb`.

## Build, test, lint

```bash
make            # colorized help (default target)
make build      # static binaries into ./bin
make test       # unit tests
make test-race  # race detector (needs cgo; local only)
make lint       # golangci-lint v2
make proto      # regenerate protobuf + Connect stubs (buf)
make proto-check # fail if generated code drifted (CI gate)
make image      # build the scratch image
make test-integration   # docker integration suite
```

Binaries always build into `./bin` (an absolute path), regardless of your current directory.

## Codegen

Protobuf and Connect stubs are generated with **buf** (`buf.gen.yaml`) using the local
`protoc-gen-go` and `protoc-gen-connect-go` plugins (`make tools` installs them). The generated
`.pb.go` / `.connect.go` files are committed; CI fails if `make proto` would change them.

## Testing approach

WaveSpan is eventually consistent, so tests verify the **declared model** — convergence,
durability thresholds, idempotency, and metadata honesty — not linearizability:

- **Unit tests** per package, including a shared `LocalStore` conformance suite run against both the
  wavesdb and in-memory stores, and deterministic in-process multi-node tests (the gossip cluster,
  origin+1, repair, cache subscriptions) that are faster and more deterministic than containers.
- **Integration tests** (`-tags integration`) bring up a real 3-node Docker cluster and exercise
  the headline behaviours end-to-end. They are excluded from `make test`.

A handful of real distributed bugs were caught only by the container tests (DNS-resolvable advertise
host, non-root volume ownership) — run `make test-integration` before merging milestone work.

## Branch / milestone workflow

Work proceeds milestone by milestone (`plans/M0x_*.md`), each on its own branch, merged to `main`
once unit + lint + proto gates and the milestone's docker integration test pass. The history on
`main` is one merge commit per milestone.

## A note on `wavesdb`

The storage engine lives in the sibling `../wavesdb` repo. If you find a storage-engine bug while
building WaveSpan, fix it there with a failing test first, run its suite plus the `testing-waves`
bank harness, and commit to wavesdb's `main` — don't work around it in WaveSpan.
