# WaveSpan Benchmarking Web UI (`wavespan-benchui`) — Design

**Date:** 2026-06-23
**Status:** Approved (pending spec review)
**Components:** `internal/benchengine` (new), `internal/benchui` (new), `cmd/wavespan-benchui` (new),
`ui/src/bench/` (new SPA entry), `docker/Dockerfile.benchui` (new), CI jobs.

## Goal

A standalone web UI + server that drives WaveSpan benchmarks against a running cluster, with a
Linea-styled dashboard that can: select a target cluster/nodes, select workloads, start / pause /
resume / stop a run, show live throughput + latency graphs, and — when the target is a
profiling-capable build — capture and render cross-node CPU/heap/block/mutex breakdowns. Shipped as
its own container.

## Hard constraint

**The `wavespan-bench` CLI keeps running benchmarks identically.** To share logic DRY-ly (rather than
duplicate op bodies), `internal/bench` is **refactored**: its Connect-client constructors are exported
(`kvClient`→`KVClient`, `cypherClient`→`CypherClient`) and the per-op logic — currently inlined inside
worker-loop closures (`kv.go:46-68`, `multiget.go:44-57`, `query.go:70-88`, `load.go:104`) — is
extracted into **single-op functions** (`OpKVRead`/`OpKVWrite`/`OpMultiGet`/`OpCypher`). The CLI's
existing `RunKV`/`RunQueries`/`RunMultiGet`/`Load` are rewired to call these shared ops, so their
behavior and output are **unchanged**. The new `internal/benchengine` reuses the same exported clients
and op functions. Net: benches still run from pure CLI; no duplicated op bodies.

## Non-goals (YAGNI)

- Auth / multi-user. The server binds `127.0.0.1` by default; exposing it is the operator's choice.
- Persisting run history to disk/DB. Runs live in server memory; a finished run's summary is
  retrievable until the server stops (a small bounded ring of recent runs).
- Distributed/multi-host load generation. One benchui process drives one coordinator data port (same
  as the CLI today).
- Editing `.cypher` files in the browser. The Cypher suite is the committed `bench/queries/` set
  (the server reads them; a future enhancement can upload custom queries).

## Decisions (settled in brainstorming)

| Decision | Choice |
|---|---|
| Frontend | Reuse `ui/` + Linea design system as a new Vite entry (`bench.html`), embedded into the server binary. |
| Charts | **uPlot** (tiny, fast live time-series), wrapped for React. |
| Pause | **Freeze & resume** — pause halts load generation, keeps the run + timeline alive; resume continues. |
| Profiling | **Full** — detect profiling-capable nodes, capture during/after a run, render the cross-node breakdown, offer raw `.pb.gz`. |
| Engine vs CLI | `internal/bench` refactored to export clients + extract single-op funcs; CLI rewired onto the same shared ops (identical behavior); engine reuses them. |

---

## Unit 1 — `internal/benchengine`

A controllable, streaming run engine. `internal/bench` today is blocking with a final report and a
grow-forever latency slice (`Latencies`); the engine adds control + windowed streaming metrics.

### Metrics model

- **Windowed histogram.** Each workload owns a metrics collector: a ring of 1-second windows. Each
  window holds `count`, `errs`, and a fixed-bucket **log-linear latency histogram** (HDR-lite, ~µs
  to ~10s, e.g. base-2 buckets with 8 sub-buckets → ≤~5% relative percentile error) so p50/p95/p99
  are computed without retaining every sample. A cumulative histogram tracks the whole run. Because
  percentiles are bucket-approximate, the unit test (feed known latencies → assert p50/p95/p99)
  asserts within the histogram's relative-error tolerance, not exact equality.
- **Sample.** Every ~1s the engine emits a `Sample{ TimeMs, PerWorkload: map[kind]WindowStat }` where
  `WindowStat{ Tput, P50, P95, P99, Errs, Total }`. Subscribers (SSE) receive samples live.
- This fixes the CLI's grow-forever slice + per-op global mutex (the engine's hot path increments a
  bucket under a sharded/lock-light counter).

### Control

- `type Run` with state machine: `idle → running ⇄ paused → stopped|done`.
- `Start()` launches `concurrency` workers per selected workload; each worker loops
  `for !stopped { gate.wait(); doOneOp() }`.
- **Pause** flips a shared gate (a `sync.Cond` / atomic + channel) so workers block before the next
  op; **Resume** releases it; **Stop** cancels the run context and finalizes the cumulative summary.
- A bounded duration is optional (unbounded runs allowed; the user stops them).

### Workload ops (extracted into `internal/bench`, shared)

Each workload's per-op logic is extracted into an exported single-op function
`func(ctx) (latency, error)` in `internal/bench`, over the exported Connect clients
(`KVClient`/`CypherClient`). KV (read/write split by ratio), MultiGet (batch), Cypher-suite
(round-robin over loaded `.cypher` queries). The CLI's `RunKV`/etc. and the engine both call these.
`Load` (bulk dataset) is exposed as a run-to-completion action with progress, not a start/pause
workload (its op, currently `execCypher` + the KV put, is likewise exported).

### Public surface

```go
type WorkloadSpec struct { Kind string; Params map[string]any }  // kind: "kv"|"multiget"|"cypher"
type Config struct { DataAddr string; Graph string; Workloads []WorkloadSpec; Concurrency int; Duration time.Duration }
func New(cfg Config) (*Run, error)
func (r *Run) Start() ; func (r *Run) Pause() ; func (r *Run) Resume() ; func (r *Run) Stop()
func (r *Run) State() State
func (r *Run) Subscribe() (<-chan Sample, func())   // func() unsubscribes
func (r *Run) Summary() Summary                       // cumulative, valid after Stop or natural completion (done)
```

## Unit 2 — `internal/benchui` + `cmd/wavespan-benchui`

HTTP server: control API + SSE + profiling, serving the embedded SPA. Reuses `internal/profile`
(`Node{Name,AdminAddr}`, `Reachable`, `CaptureCPU`, `CaptureSnapshots`, `BuildReport`).

### HTTP API

```
GET  /api/workloads                       available workloads + param schema
POST /api/target/probe                     {dataAddr, nodes:[{name,adminAddr}]} → per-node {reachable, profiling}
POST /api/dataset/load                     bulk loader; SSE progress
POST /api/runs                             create a run (Config) → {id}
POST /api/runs/{id}/start|pause|resume|stop
GET  /api/runs/{id}                         state + cumulative summary
GET  /api/runs/{id}/stream                  SSE: per-second Sample
POST /api/runs/{id}/profile                 {cpuSeconds} capture from profiling-capable nodes → {pid}
GET  /api/profile/{pid}/report              cross-node breakdown (Report sections JSON)
GET  /api/profile/{pid}/raw/{node}.{kind}.pb.gz   raw profile download
GET  /                                      the SPA (embedded)
```

- **Run registry**: in-memory `map[id]*Run` + a bounded ring of finished summaries. One active run at
  a time is sufficient (the UI drives one); the server rejects starting a second concurrent run.
- **SSE**: `/stream` subscribes to the run and writes `data: <json sample>\n\n` per sample; closes on
  stop/done or client disconnect (ctx).
- **Profiling gating**: `/profile` and probe use `profile.Reachable`; if no node is profiling-capable,
  the probe reports `profiling:false` and the UI hides the panel.
- **`cmd/wavespan-benchui`**: flags `--addr 127.0.0.1:8088` (bind), serves the mux. `go:embed` the
  built SPA from `internal/benchui/dist` (committed `.gitkeep` placeholder so it compiles pre-build,
  mirroring `internal/ui`).

### Security

Binds localhost by default. No auth on the local bind (documented). Profiling endpoints proxy pprof
(sensitive) and only act on reachable admin ports. Input validation: workload params type-checked;
addresses are dialed, not shell-interpolated.

## Unit 3 — `ui/src/bench/` (new Vite entry)

A Linea-styled SPA, new entry `ui/bench.html` + `ui/vite.bench.config.ts` + `npm run build:bench`
(outputs to `internal/benchui/dist`). Reuses Linea + `Sparkline`/`StatCard`; adds **uPlot**
(dependency) wrapped as a React `<TimeSeries>` component.

Panels:
- **Target** — data-port addr + per-node `name=adminAddr` list; **Probe** shows reachability +
  a "profiling ✓" badge per node.
- **Workloads** — KV (concurrency, keys, read-ratio), MultiGet (concurrency, batch, keys), Cypher
  (concurrency, graph; runs the committed suite); a **Prepare dataset** action (users/follows/kv).
- **Run controls** — Start / Pause / Resume / Stop + elapsed + state.
- **Live charts** — uPlot: throughput (ops/s) and latency (p50/p95/p99 ms) over time, per workload;
  StatCards for current values; error count.
- **Profiling** (only if a profiling-capable node was probed) — Capture (CPU seconds) → render the
  cross-node breakdown (CPU / Block=latency / Mutex / Heap, each with hottest leaf) + per-node
  `.pb.gz` download links.

Data flow: Probe → configure workloads → POST /runs → Start → open SSE `/stream` → append samples to
uPlot buffers → Pause/Resume/Stop → on Stop fetch summary; optional Capture → fetch report.

## Container + CI

- **`docker/Dockerfile.benchui`** — multi-stage: ① Node stage `npm ci && npm run build:bench` →
  `internal/benchui/dist`; ② Go build stage cross-compiles `wavespan-benchui` (CGO-free,
  `$BUILDPLATFORM`) with the `wavesdb` sibling in the build context (same as the node Dockerfile);
  ③ `FROM scratch` with the binary (SPA embedded). Exposes the bind port.
- **CI (`ci.yaml`)** — a `benchui-image` job (needs `build-test`): multi-arch build + push to
  `ghcr.io/<owner>/wavespan-benchui` on main, build-only on PRs, using `WAVESDB_TOKEN` for the
  wavesdb checkout. **`release.yaml`** gets a versioned `wavespan-benchui` image on `v*` tags.
- The bench SPA build is added to the existing `build-test`/`binaries`/Docker UI-build steps as
  needed (the benchui binary embeds `internal/benchui/dist`, so its image's Node stage builds it).

## Testing

- **`internal/benchengine`**: state transitions (start/pause/resume/stop), windowed-percentile
  correctness (feed known latencies → assert p50/p95/p99), pause-stops-issuing (deterministic with a
  fake op fn injected), unbounded + bounded runs.
- **`internal/benchui`**: handler/lifecycle tests with a stub run (create→start→sample→stop), SSE
  emits samples, reject second concurrent run, profiling probe with a stub `Reachable`, 404s.
- **Frontend**: `npm run build:bench` succeeds (CI); light component test for the uPlot wrapper +
  the target-probe reducer.
- **CI**: builds the benchui image (Dockerfile validation), plus the unit suites above.

## Files (high level)

| Path | Change |
|---|---|
| `internal/benchengine/{engine.go,metrics.go,workloads.go,*_test.go}` | new — controllable engine + windowed metrics |
| `internal/benchui/{server.go,handlers.go,sse.go,profile.go,embed.go,dist/.gitkeep,*_test.go}` | new — HTTP server + embedded SPA |
| `cmd/wavespan-benchui/main.go` | new — server entrypoint |
| `ui/bench.html`, `ui/vite.bench.config.ts`, `ui/package.json` (script + uPlot dep) | new — Vite entry |
| `ui/src/bench/**` | new — React dashboard (target, workloads, controls, charts, profiling) |
| `docker/Dockerfile.benchui` | new — multi-stage container |
| `.github/workflows/ci.yaml`, `release.yaml` | benchui image jobs |
| `internal/bench/{latency.go,kv.go,multiget.go,query.go,load.go}` | refactor: export `KVClient`/`CypherClient`; extract `OpKVRead/OpKVWrite/OpMultiGet/OpCypher` (+ load op); rewire `RunKV`/`RunQueries`/`RunMultiGet`/`Load` onto them (behavior + output identical; CLI unchanged). |
| `.gitignore` | add `/internal/benchui/dist/*` + `!/internal/benchui/dist/.gitkeep` (the root `/dist/` rule does not match it) |
| `Makefile`/`justfile` | `build:bench`, a `benchui` run target (optional) |
