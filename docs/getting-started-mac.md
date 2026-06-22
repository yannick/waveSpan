# Getting started on a Mac (single-host development)

This guide gets WaveSpan running on a single Mac for development — first as one node, then as a
local multi-node cluster. It assumes Apple Silicon (the project targets `linux/arm64` images and
Apple's `container` runtime), but Docker Desktop works too.

> WaveSpan is an eventually-consistent distributed KV store built on the `wavesdb` Go storage
> engine. See [`architecture.md`](architecture.md) for the big picture.

## 1. Prerequisites

| Tool | Why | Install |
|---|---|---|
| Go 1.26+ | builds everything | `brew install go` |
| `buf` | protobuf + Connect codegen | `brew install bufbuild/buf/buf` |
| `golangci-lint` v2 | linting | `brew install golangci-lint` |
| Docker Desktop **or** Apple `container` | multi-node clusters | `brew install --cask docker` / `brew install container` |
| `protoc-gen-go`, `protoc-gen-connect-go` | codegen plugins | `make tools` (installs into `$(go env GOPATH)/bin`) |

The data node links the **sibling `wavesdb` checkout** in-process (no FFI). The repository layout
must be:

```
storage-engines/
  wavesdb/        # the Go storage engine (separate git repo)
  waveSpan/       # this repo
```

`waveSpan/go.mod` resolves it via `replace wavesdb => ../wavesdb`, so just keep the two side by
side.

Add the Go bin dir to your PATH (for the codegen plugins and `buf`):

```bash
export PATH="$(go env GOPATH)/bin:$PATH"
```

## 2. Build

```bash
cd storage-engines/waveSpan
make tools     # one-time: install protoc-gen-go / protoc-gen-connect-go
make build     # builds bin/wavespan-node, bin/wavespan-gateway, bin/wavespanctl
make test      # unit tests
```

`make build` produces three static (`CGO_ENABLED=0`) binaries in `bin/`.

## 3. Run a single node

A lone node has no peer to replicate to, so the default `origin+1` write rule (ADR-0002) would
reject writes. For development, use the **single-node config**, which sets
`minAckNearbyReplicas: 0` (local-only writes — the origin copy is still durable):

```bash
WAVESPAN_STORAGE_PATH=/tmp/wavespan-dev \
  ./bin/wavespan-node --config config/dev-single.yaml
```

The node opens three listeners:

| Port | Purpose |
|---|---|
| `:7700` | gossip (SWIM membership) |
| `:7800` | data plane — the `KvService` (Connect/gRPC) |
| `:7900` | admin — `/healthz`, `/readyz`, `/metrics`, `/admin/membership`, `/admin/latency` |

Check it's up (in another terminal):

```bash
curl -fsS localhost:7900/healthz          # ok
curl -fsS localhost:7900/admin/membership # this node, ALIVE, with a storage UUID
```

## 4. Read and write with `wavespanctl`

`wavespanctl` talks to the data port (`:7800`) over Connect:

```bash
./bin/wavespanctl kv put default foo bar
# ok  acked_nearby_replicas=0 version=1782107862572.0@node1

./bin/wavespanctl kv get default foo
# bar
# source=LOCAL_DURABLE version=1782107862572.0@node1

./bin/wavespanctl kv put default baz qux
./bin/wavespanctl kv scan default
# mode=CACHE_FAST completeness=BEST_EFFORT
# baz	qux
# foo	bar
# rows=2 completeness=BEST_EFFORT

./bin/wavespanctl kv put default temp hello --ttl 5000   # expires in 5s
./bin/wavespanctl kv delete default foo
./bin/wavespanctl members                                # cluster membership (admin :7900)
```

Every read reports its **source** (`LOCAL_DURABLE`, `LOCAL_DYNAMIC_CACHE`,
`FETCHED_CLOSEST_HOLDER`) and every scan reports its **completeness** — WaveSpan never hides the
fact that a read may be stale or a scan partial. See [`kv-api.md`](kv-api.md).

You can also call the API directly over Connect's JSON (bytes fields are base64):

```bash
curl -sS localhost:7800/wavespan.v1.KvService/Get \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"default","key":"'"$(printf foo | base64)"'"}'
```

## 5. Run a 3-node cluster on one Mac

A multi-node cluster exercises the real behaviour: `origin+1` writes, target-N repair, dynamic
cache, and routed scans. Two ways, both on your single Mac.

### Option A — Docker Compose (works everywhere)

```bash
make docker-up          # builds the image once, starts node1/node2/node3
# admin ports are mapped: node1 :7901, node2 :7902, node3 :7903
#                         data  ports: node1 :7811, node2 :7812, node3 :7813
curl -fsS localhost:7901/admin/membership   # all three ALIVE once gossip converges

./bin/wavespanctl --addr localhost:7811 kv put default k v   # writes acked by a nearby replica
./bin/wavespanctl --addr localhost:7813 kv get default k     # node3 fetches from a holder + caches

make docker-kill        # tear down
```

With three nodes the default `config/dev.yaml` policy applies (origin+1), so a write needs a
nearby durable replica — and `acked_nearby_replicas=1` confirms it.

### Option B — Apple `container` (fast, native, Apple Silicon)

```bash
container system start          # one-time, starts the lightweight VM runtime
./container/build.sh            # build the image with Apple container
./container/up.sh 3             # start a 3-node cluster (per-node container machine)
# ... use it ...
./container/down.sh 3           # tear down
```

Apple `container` boots each node in its own micro-VM in well under a second, so `up.sh 6` is
interactive. See [`running-clusters.md`](running-clusters.md) for details and fault injection.

## 6. Watch what it's doing

```bash
curl -fsS localhost:7901/admin/membership   # liveness states (ALIVE/SUSPECT/UNREACHABLE/...)
curl -fsS localhost:7901/admin/latency      # measured RTT edges (the placement signal)
curl -fsS localhost:7901/metrics | grep kv_ # under-replication, repair queue, cache, TTL metrics
```

Kill a node (`docker kill wavespan-dev-node2-1`) and watch the survivors mark it `SUSPECT` →
`UNREACHABLE`, then watch `kv_under_replicated_keys_estimate` drain back to zero as the repair
engine restores replicas — no manual action.

## 7. Common issues

- **`insufficient nearby durable replicas for origin+1`** on a single node — expected. Use
  `config/dev-single.yaml` (or `WAVESPAN_MIN_ACK_NEARBY_REPLICAS=0`).
- **`mkdir /var/lib/wavespan: permission denied`** running the binary directly — set
  `WAVESPAN_STORAGE_PATH` to a writable dir (e.g. `/tmp/wavespan-dev`). The default path is for
  the container's mounted volume.
- **Docker build stalls / `buildx` builder in `error` state** — `docker desktop restart` clears it.
- **`buf: command not found` / proto plugins missing** — `brew install buf` and `make tools`, and
  ensure `$(go env GOPATH)/bin` is on your PATH.

## Next steps

- [`kv-api.md`](kv-api.md) — the full KV API, consistency metadata, scans, and TTL.
- [`configuration.md`](configuration.md) — config file and `WAVESPAN_*` env reference.
- [`development.md`](development.md) — build system, testing, and the integration suite.
