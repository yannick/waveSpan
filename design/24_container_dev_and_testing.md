# 24. Container dev and testing

## Goal

One OCI image, two orchestrators. Developers on Apple Silicon bring up local N-node clusters
with Apple's `container` CLI; CI on Linux brings up the same clusters with Docker/containerd.
The image is byte-for-byte the same artifact in both paths, so what passes locally is what runs
in CI and in production.

Two rules make this work and are non-negotiable:

- **No CGO.** Every Go binary is built `CGO_ENABLED=0`, fully static, into a `FROM scratch`
  (or distroless-static) image. This is a hard constraint on the whole tree: `wavesdb` is
  already pure Go, and every later component — including the M10 vector HNSW index — must stay
  pure Go with no cgo escape hatch. See `design/17_source_tree.md` "Forbidden:
  `any internal/ package -> tidesdb / cgo`".
- **Same image everywhere.** Apple `container` and Docker/containerd build and consume the
  identical multi-arch image. Only orchestration differs. Apple `container` runs only on
  macOS / Apple Silicon, so it is a developer convenience and is **never required in CI**.

This doc is the canonical container build/test spec. `design/10_docker_dev.md` covers the
seed-discovery / env / acceptance details of the docker-compose path and points here for build
and orchestration.

## Build

### Constraints

| Constraint | Value |
|---|---|
| cgo | `CGO_ENABLED=0` — static, no libc, no dynamic loader |
| base image | `scratch` (or `gcr.io/distroless/static` when a shell-free libc-free base with CA certs is wanted) |
| user | non-root (`USER 65532:65532`, the distroless `nonroot` uid) |
| arch | `linux/arm64` (primary, Apple Silicon) and `linux/amd64` (CI / prod) |
| contents | the static `wavespan-node` binary, embedded UI assets (compiled into the binary, see `design/26_node_ui_and_observability.md`), CA certificates, nothing else |
| reproducibility | trimmed paths, pinned toolchain, `SOURCE_DATE_EPOCH` |

Because the UI assets are `go:embed`-ed into `wavespan-node` (doc 26), the image needs no asset
layer — the binary is self-contained. CA certs are the only non-binary file, and only because
outbound TLS (global replication peers, object stores) needs a trust root; a pure intra-cluster
mTLS deployment can drop even those.

### Multi-stage Dockerfile

The same Dockerfile is consumed by `container build`, `docker build`, and `docker buildx`.

```dockerfile
# syntax=docker/dockerfile:1
# ---- build stage ----
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
ARG SOURCE_DATE_EPOCH
WORKDIR /src

# wavesdb is a sibling module brought in via `replace wavesdb => ../wavesdb`
# (design/17_source_tree.md). The build context must include it; see `make image`.
COPY wavesdb/ /wavesdb/
COPY waveSpan/ /src/

# Pre-warm module cache, then cross-compile a fully static binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w -X github.com/yannick/wavespan/internal/version.Build=${SOURCE_DATE_EPOCH}" \
      -o /out/wavespan-node ./cmd/wavespan-node

# ---- final stage ----
FROM scratch
# CA roots for outbound TLS (global replication, object stores).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/wavespan-node /wavespan-node
# Non-root, matches the distroless `nonroot` uid:gid.
USER 65532:65532
EXPOSE 7700 7800 7900
ENTRYPOINT ["/wavespan-node"]
```

Notes:

- `--platform=$BUILDPLATFORM` keeps the toolchain on the native host arch and cross-compiles to
  `$TARGETARCH`, so an Apple-Silicon host builds the `amd64` image without emulating the
  compiler.
- `scratch` has no shell, so health checks are HTTP against `/healthz` (doc 14), not `exec`-based
  shell probes.
- Ports: `7700` gossip, `7800` data, `7900` admin/UI (`design/04_membership_latency_gossip.md`
  "advertiseAddrs").

### Reproducible builds

`-trimpath` removes local path prefixes; pinned `golang:1.26`; `SOURCE_DATE_EPOCH` drives any
embedded build stamp so two builds of the same commit produce identical layers. Tag images by
git commit (`wavespan/node:<short-sha>`) and treat `:dev` / `:latest` as moving aliases.

### `make image`

`make image` wraps the multi-arch build so the build context includes the sibling `../wavesdb`
checkout (the `replace` directive needs it present). It must:

- default to the host arch for a fast inner-loop image, and accept `PLATFORMS=linux/arm64,linux/amd64`
  for a multi-arch manifest;
- produce an image whose entrypoint runs `wavespan-node --config dev.yaml` (M0 acceptance);
- be the single target both the local (`container`) and CI (`docker buildx`) paths call, so the
  Dockerfile is exercised identically.

## Local dev and test: Apple `container`

Apple's `container` CLI (macOS 15+, Apple Silicon) runs each Linux container in its own
lightweight VM — a "container machine" — using the `Containerization` framework. Boot is
sub-second and per-container, so a 3- or 6-node cluster comes up far faster and with less
idle overhead than a single shared Docker Desktop VM. Crucially, `container` consumes the same
`scratch`/CGO-free OCI image built above; there is no macOS-specific binary.

Apple-native helper scripts live under **`container/`**. The docker-compose / Linux path keeps
its scripts under **`docker/`** (see doc 10). The two never share scripts, only the image.

```text
container/
  build.sh        # container build --platform linux/arm64 -t wavespan/node:dev
  up.sh           # create network + run N nodes with seed env wired
  down.sh         # stop + remove the cluster and its volumes
  fault.sh        # fault-injection helpers (see "Fault injection" below)
```

### Bringing up a cluster

```bash
# 1. Build the image once (same Dockerfile as CI).
container build --platform linux/arm64 -t wavespan/node:dev -f docker/Dockerfile .

# 2. A shared network so nodes resolve each other by name.
container network create wavespan-dev

# 3. Start N nodes. Each gets its own container machine, its own data volume,
#    and the static seed list — discovery is seed-based, no Kubernetes API
#    (design/04_membership_latency_gossip.md "Docker discovery").
for i in 1 2 3; do
  container run -d \
    --name node$i \
    --network wavespan-dev \
    --volume "$PWD/data/node$i:/var/lib/wavespan" \
    --env WAVESPAN_RUNTIME=docker \
    --env WAVESPAN_CLUSTER_ID=dev \
    --env WAVESPAN_MEMBER_ID=node$i \
    --env WAVESPAN_NODE_NAME=container-node-$i \
    --env WAVESPAN_ZONE=zone-$([ $i -le 1 ] && echo a || echo b) \
    --env WAVESPAN_REGION=dev-region \
    --env WAVESPAN_GEO=dev \
    --env WAVESPAN_SEEDS=node1:7700,node2:7700,node3:7700 \
    wavespan/node:dev
done
```

`container/up.sh` parameterizes the node count (`up.sh 6` for a 6-node cluster), spreads
`WAVESPAN_ZONE`/`WAVESPAN_REGION` to give the latency graph and placement filters something to
score (doc 04 "Topology penalty"), and points each node at the same `WAVESPAN_SEEDS` static
seed list. Per-node `--volume` mounts give each node a private `wavesdb` data directory so kills
and restarts exercise real on-disk recovery.

The env contract (`WAVESPAN_RUNTIME`, `WAVESPAN_CLUSTER_ID`, `WAVESPAN_MEMBER_ID`,
`WAVESPAN_NODE_NAME`, `WAVESPAN_ZONE`, `WAVESPAN_REGION`, `WAVESPAN_GEO`, `WAVESPAN_SEEDS`) is
identical to the docker-compose path in doc 10 — same binary, same config loader
(`internal/config`, M0), so a cluster behaves the same whichever orchestrator started it.

### Why this is the primary local path

- per-container lightweight VMs boot in well under a second, so `up.sh 6` is interactive, not a
  coffee break;
- no always-on Docker Desktop VM consuming RAM between test runs;
- native arm64 execution (no emulation) on Apple Silicon, matching the primary build target;
- the image is the CI image, so "works on my machine" and "works in CI" converge by construction.

## CI path: Docker / containerd on Linux

CI runs on GitHub Actions Linux runners and never touches Apple `container`. It builds the same
image with `docker buildx` and orchestrates multi-node clusters with docker-compose (doc 10) or
plain containerd.

```bash
# Build the same multi-arch image; load the host-arch one for tests.
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t wavespan/node:ci \
  -f docker/Dockerfile \
  --load .

# Bring up the compose cluster from doc 10 and run the suites.
docker compose -f docker/docker-compose.yaml up -d
go test ./tests/integration/...     # design/16_testing_strategy.md "Integration tests"
go test ./tests/chaos/...           # harness-driven; design/25 + design/16 "Chaos tests"
```

CI builds both arches so the published manifest is multi-arch, but only needs to *run* the
runner's native arch for tests. The integration and chaos/harness suites
(`design/16_testing_strategy.md`, and the correctness harness in `design/25`) drive the cluster
through the same fault-injection interface described below.

## Parity table

| Capability | Local (Apple `container`) | CI (Docker / containerd, Linux) |
|---|---|---|
| OCI image | `wavespan/node:dev` — scratch, CGO-free | `wavespan/node:ci` — **same Dockerfile, same build args** |
| Arch | linux/arm64 (native) | linux/amd64 + linux/arm64 manifest; runs runner-native |
| Orchestrator | `container` CLI + per-node VMs | docker-compose / containerd |
| Network | `container network create` | compose network / containerd bridge |
| Discovery | static `WAVESPAN_SEEDS` | static `WAVESPAN_SEEDS` (identical) |
| Per-node data | `--volume` per node | compose volume per node |
| Suites run | smoke + integration inner loop | integration + global + chaos/harness + load |
| Apple `container` required? | yes (it *is* the local path) | **never** |

Images are identical because both paths invoke `make image` against the same Dockerfile with the
same `CGO_ENABLED=0` static build. The only differences are which orchestrator wires up the
network/volumes/env and which suites run — and the env contract is shared, so even that is thin.

## Fault injection

The harness (`design/25`) and the chaos suite (`design/16_testing_strategy.md` "Chaos tests")
drive failures through a runner abstraction that both orchestrators implement. Each fault is
either **container-native** (the orchestrator does it) or needs an **in-process toggle** exposed
by `wavespan-node` (the binary does it on command), because a `scratch` image has no shell or
`tc`/`iptables` to do it from outside.

| Fault | Mechanism | Container-native or in-process |
|---|---|---|
| kill node | `container rm -f` / `docker kill` | container-native |
| pause / resume node | `container stop`+`start` / `docker pause`+`unpause` | container-native |
| partition network | detach from / drop a network membership, or per-pair block | container-native (network ops); in-process gossip-block toggle as a portable fallback |
| inject latency | in-process delay on data/gossip RPC handlers | **in-process toggle** (no `tc` in `scratch`) |
| packet loss | in-process probabilistic drop of gossip/data frames | **in-process toggle** |
| clock skew | in-process HLC/wall offset (`internal/version` test seam) | **in-process toggle** |
| disk fill / stall | in-process storage fault hook on the `wavesdb` adapter; or a size-capped/slow volume | **in-process toggle** (primary); volume-backed variant for real ENOSPC |

The in-process toggles are the "fault injection hooks for integration tests" that
`design/17_source_tree.md` "Implementation rule" requires of every package. They are gated to
`security.insecureDevMode` / a dev-only admin endpoint so they cannot fire in production. Because
they live in the binary, they behave identically under Apple `container` and Docker — the harness
issues the same command regardless of orchestrator. Network-level partitioning is offered
container-native where the orchestrator supports it (closer to a real partition), with the
in-process gossip-block toggle as the portable fallback so the harness has one partition path that
works everywhere.

## Acceptance

- [ ] `make image` produces a `scratch`, CGO-free image whose entrypoint runs
      `wavespan-node --config dev.yaml`.
- [ ] `container build` + `container/up.sh 3` brings up a 3-node cluster locally on Apple Silicon
      and the nodes form gossip membership (doc 10 acceptance).
- [ ] `container/up.sh 6` brings up 6 nodes and the latency graph populates.
- [ ] CI builds the *same* Dockerfile on Linux with `docker buildx` (multi-arch) and runs the
      integration + chaos suites against the compose cluster.
- [ ] Every fault in the table is drivable by the harness under both orchestrators.
- [ ] No image layer pulls in cgo / libc; `file wavespan-node` reports a statically linked binary.
