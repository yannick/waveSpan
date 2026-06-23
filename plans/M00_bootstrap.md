# M00 - Bootstrap: repo skeleton, build system, config, version types, node binary

**Milestone:** M0 (roadmap `design/18_implementation_roadmap.md` "Milestone 0")
**Tickets:** TS-001, TS-002, TS-003 (`design/19_agent_work_items.md`)
**Depends on:** nothing (this is the root milestone)
**Enables:** M1 (storage), M11 (operator track can fork here)

## Context

This milestone stands up the WaveSpan repository so that every later milestone has a build,
a config loader, version/HLC primitives, structured logging, and a runnable
`wavespan-node` binary that serves `/healthz` and `/metrics`. Nothing here is distributed
yet — the goal is a compiling, testable, observable skeleton on the Go stack
(`IMPLEMENTATION_STRATEGY.md` section 1).

The local engine `wavesdb` lives at `../wavesdb` relative to the WaveSpan module root and is
imported via a `replace` directive. We do not touch it in M0; we only wire the module graph
so M1 can import it.

HLC and version types are foundational because every replicated record, mutation-log entry,
and conflict decision references a `Version` (`design/02_storage_wavesdb.md` "Local record
format"). They are defined here, in `internal/version`, against the canonical
`design/22_versioning_and_hlc.md`.

## Files to create

```
waveSpan/
  go.mod                                  module github.com/yannick/wavespan; replace wavesdb => ../wavesdb
  go.sum
  Makefile                                build / test / proto / lint / docker targets
  .golangci.yml                           lint config
  buf.yaml  buf.gen.yaml                  proto codegen (buf)
  proto/wavespan/v1/common.proto          Version, MutationId, ResponseMeta, ConflictState, enums
  cmd/wavespan-node/main.go               entrypoint: load config, init logging, serve health/metrics
  cmd/wavespan-gateway/main.go            stub entrypoint (compiles, serves /healthz)
  cmd/wavespanctl/main.go                 stub CLI (cobra root + version subcommand)
  internal/config/config.go               Config struct, YAML+env load, runtime mode, validation
  internal/config/config_test.go
  internal/version/hlc.go                 HLC clock (physical ms + logical counter)
  internal/version/version.go             Version type, Compare, writer sequence, MutationID
  internal/version/version_test.go
  internal/observability/logging.go       slog setup (JSON in k8s, text in dev)
  internal/observability/metrics.go       prometheus registry + /metrics handler
  internal/observability/health.go        /healthz, /readyz handlers
  internal/version/version.go             (gen target wires to proto common.pb.go)
  docker/Dockerfile                       multi-stage, CGO_ENABLED=0 static, FROM scratch, non-root
  docker/docker-compose.yaml              3-node dev cluster (CI/portable path; placeholder until M2)
  container/build.sh                      Apple `container` image build (linux/arm64) — see design/24
  container/up.sh container/down.sh       Apple `container` local N-node cluster up/down — see design/24
  config/dev.yaml                         sample dev config for `wavespan-node --config`
  .github/workflows/ci.yaml               buf + lint + build + test + multi-arch image workflow
```

Generated (not hand-written): `proto/wavespan/v1/common.pb.go`,
`proto/wavespan/v1/commonv1connect/` (Connect stubs).

## Steps

1. **Module + replace directive.** `go mod init github.com/yannick/wavespan`; add
   `replace wavesdb => ../wavesdb` and `require wavesdb v0.0.0` so the module graph resolves
   `../wavesdb`. Run `go mod tidy` against a trivial import to confirm the replace resolves.

2. **Makefile targets:** `build` (build all three `cmd/` binaries into `bin/` with
   `CGO_ENABLED=0`), `test` (`go test ./...`), `proto` (run `buf generate`), `lint`
   (`golangci-lint run`), `image` (multi-arch `scratch` build of `wavespan-node`, see step 8 and
   `design/24_container_dev_and_testing.md`), and convenience `docker-up`/`docker-kill` (used from
   M2). Keep targets thin; they shell out to standard tools. `make image` must default to the host
   arch and accept `PLATFORMS=linux/arm64,linux/amd64`; it is the single target both the local
   Apple `container` path and the CI `docker buildx` path call, so the Dockerfile is exercised
   identically (doc 24 "make image").

3. **Proto toolchain (buf), Go + Connect.** `buf.yaml` + `buf.gen.yaml` generating Go messages
   **and Connect (`connect-go`) service stubs** into `proto/wavespan/v1/` (the web UI talks
   ConnectRPC — see `design/26_node_ui_and_observability.md`). Configure `buf.gen.yaml` with the
   `protocolbuffers/go` and `connectrpc/go` plugins. Author `common.proto` with the `Version` and
   `MutationId` messages exactly as in `design/02_storage_wavesdb.md` "Local record format"
   (`hlc_physical_ms`, `hlc_logical`, `writer_cluster_id`, `writer_member_id`,
   `writer_sequence`, `vector_clock`), plus `ResponseMeta`, `ConflictState`, and shared
   enums from `design/03_kv_store.md` "Response metadata". `make proto` must regenerate
   cleanly; CI runs `buf generate` and fails if the working tree changes (proto drift gate).

4. **Config loader (TS-002), `internal/config`.** A `Config` struct mirroring
   `design/17_source_tree.md` "Configuration file": `clusterId`, `memberId`,
   `storage.path`, `storage.engine`, `membership.runtime` (`docker`|`kubernetes`),
   `membership.seeds`, `replication.policyRef`, `security.insecureDevMode`. Load order:
   YAML file from `--config`, then env overrides with the `WAVESPAN_` prefix
   (`design/04_membership_latency_gossip.md` "Docker discovery" lists
   `WAVESPAN_CLUSTER_ID`, `WAVESPAN_SEEDS`, etc.). Runtime mode selects docker vs
   kubernetes discovery inputs. **Validate eagerly and fail fast**: empty `clusterId`,
   empty `memberId`, no seeds in docker mode, and unknown `runtime` are hard errors with
   actionable messages (TS-002 acceptance: "invalid config fails fast").

5. **Version + HLC types (TS-003), `internal/version`.** Per
   `design/22_versioning_and_hlc.md`:
   - `HLC` clock: `Now()` returns monotonic `(physicalMs, logical)`; on each call advance
     physical to `max(wall, last.physical)` and bump logical on ties; `Update(remote)`
     merges an observed remote timestamp (Lamport-style max). Guard against clock
     regression.
   - `Version`: wraps the proto `Version`. `Compare(a, b)` implements the deterministic LWW
     order from `design/03_kv_store.md` "Conflict handling": HLC physical, then HLC logical,
     then writer cluster ID, then writer member ID, then writer sequence.
   - Writer sequence: a per-(member,key-or-partition) monotonic counter the coordinator
     stamps onto each write.
   - `MutationID`: stable ID derived from `(cluster, member, writerSequence)` or a
     client-supplied idempotency key; equal IDs must compare equal so retries collapse.
   - proto encode/decode helpers between `version.Version` and `wavespanv1.Version`.

6. **Observability skeleton, `internal/observability`.** `slog`-based structured logging:
   JSON handler when `runtime=kubernetes`, text when dev; include `clusterId`/`memberId` as
   default attributes. A prometheus registry + `/metrics` handler. `/healthz` (process up)
   and `/readyz` (config valid; later milestones gate on membership join).

7. **`wavespan-node` entrypoint, `cmd/wavespan-node/main.go`.** Parse `--config`, load and
   validate config, init logging + metrics, start an HTTP admin server exposing `/healthz`,
   `/readyz`, `/metrics` on the admin address. Block on signal; log a clean shutdown. The
   gateway and ctl entrypoints are compile-only stubs that also serve `/healthz` (gateway)
   and print version (ctl).

8. **Dockerfile + container scripts + dev config.** Author a multi-stage `docker/Dockerfile` per
   `design/24_container_dev_and_testing.md`: a `golang:1.26` build stage that cross-compiles
   `wavespan-node` with `CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH -trimpath` for the
   `$BUILDPLATFORM` toolchain, and a `FROM scratch` final stage containing only the static binary,
   CA certs, and `USER 65532:65532` (non-root). Embedded UI assets are `go:embed`-ed into the
   binary (doc 26), so there is no asset layer. Set `GOGC`/`GOMEMLIMIT` env defaults (risk: Go GC
   pauses, `IMPLEMENTATION_STRATEGY.md` section 4) — as runtime env, not a layer. Add the
   Apple-native `container/build.sh`, `container/up.sh`, `container/down.sh` helpers (doc 24
   "Local dev and test") that build the same image and bring up an N-node cluster via the
   `container` CLI with the shared `WAVESPAN_*` seed env; keep the docker-compose path under
   `docker/`. `config/dev.yaml` is a minimal valid config that
   `wavespan-node --config config/dev.yaml` accepts. The `docker-compose.yaml` is scaffolded with
   the 3 services from `design/10_docker_dev.md` but is not exercised until M2.

9. **CI, `.github/workflows/ci.yaml`.** Linux runners only; CI never uses Apple `container`. Steps:
   checkout (with `../wavesdb` available), `make proto` + git-diff check, `make lint`,
   `make build`, `make test`, then `docker buildx build --platform linux/amd64,linux/arm64` of the
   **same `docker/Dockerfile`** to prove the multi-arch `scratch` image builds on Linux. This is
   the unit-test + image gate referenced by Milestone 0 acceptance.

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 0 and the TS tickets:

- `wavespan-node --config config/dev.yaml` starts and stays up. (M0; TS-001)
- `/healthz` and `/metrics` respond `200`. (M0)
- CI runs unit tests; `make build` and `make test` succeed. (M0; TS-001)
- Invalid config fails fast with a clear error; the docker sample config starts; Kubernetes
  `WAVESPAN_*` env variables are parsed. (TS-002)
- Version ordering is deterministic; the same request/mutation ID is idempotent (equal IDs
  compare equal and collapse retries). (TS-003)
- `make proto` regenerates Go + Connect stubs with no working-tree drift. (proto gate)
- `make image` produces a `scratch`, `CGO_ENABLED=0` image whose entrypoint runs
  `wavespan-node --config dev.yaml`; the binary is statically linked (no cgo/libc). (doc 24)
- `container run` (via `container/up.sh`) brings up a node locally on Apple Silicon. (doc 24)
- CI builds the *same* `docker/Dockerfile` on Linux with `docker buildx` (multi-arch). (doc 24)

## Verification

1. **Unit:** `make test` green. `internal/version` tests assert HLC monotonicity under
   shuffled `Update` order and that `Compare` is a total deterministic order over a shuffled
   slice of versions (property 3 prerequisite). `internal/config` tests assert each
   fail-fast case and env-override precedence.
2. **Binary smoke:** `make build && ./bin/wavespan-node --config config/dev.yaml &`, then
   `curl -fsS localhost:<admin>/healthz` and `curl -fsS localhost:<admin>/metrics | head`
   succeed; an intentionally invalid config exits non-zero with a readable message.
3. **Proto gate:** `make proto && git diff --exit-code proto/` is clean (Go + Connect stubs).
4. **Image:** `make image` builds; `file bin/wavespan-node` (or extracting the layer binary)
   reports "statically linked"; the container's entrypoint runs
   `wavespan-node --config config/dev.yaml` and `/healthz` answers `200`. On Apple Silicon,
   `container/up.sh 1` starts a node via the `container` CLI.
5. **CI:** the workflow passes on a fresh checkout that also clones `../wavesdb`, including the
   `docker buildx` multi-arch build of the same Dockerfile on a Linux runner (no Apple
   `container`).

No docker-compose, multi-node, or bank-invariant run is required at M0 (no data path yet); those
gates switch on at M2/M3.
