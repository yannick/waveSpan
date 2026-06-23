# WaveSpan Benchmarking Web UI — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone `wavespan-benchui` server + Linea-styled web dashboard that drives WaveSpan benchmarks against a cluster (select target/workloads, start/pause/resume/stop, live throughput+latency graphs, and cross-node profiling when available), shipped as a container — while the `wavespan-bench` CLI keeps working.

**Architecture:** Refactor `internal/bench` to export its Connect clients + single-op functions (CLI rewired, behavior identical). A new `internal/benchengine` adds a controllable run engine with windowed (HDR-lite) streaming metrics. A new `internal/benchui` HTTP server drives the engine, streams samples over SSE, exposes profiling via `internal/profile`, and embeds a React SPA (new Vite entry in `ui/`, uPlot charts). `cmd/wavespan-benchui` is the entrypoint; `docker/Dockerfile.benchui` + CI jobs ship the container.

**Tech Stack:** Go 1.26 (net/http, `connectrpc.com/connect`, `internal/rpcopts` H2C), React 18 + Vite (Linea design system), uPlot, Docker buildx, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-06-23-bench-webui-design.md`

---

## Conventions / guardrails (apply throughout)

- **CLI must stay working:** after the Task-1 refactor, `go test ./internal/bench/...` and `go build ./cmd/wavespan-bench` must pass; `RunKV`/`RunQueries`/`RunMultiGet`/`Load` keep identical behavior/output.
- **No CGO**, no new heavy deps in Go. Frontend adds only `uplot`.
- Every Go task is TDD (failing test → implement → pass → commit). Run `gofmt -w` on edited files.
- Security: the server binds `127.0.0.1` by default; validate workload params; never shell-interpolate addresses.
- After each task: `go build ./...` stays green.

---

## File structure

| Path | Responsibility |
|---|---|
| `internal/bench/latency.go` | export `KVClient`/`CypherClient` (was `kvClient`/`cypherClient`); keep `Latencies` |
| `internal/bench/ops.go` (new) | single-op funcs: `OpKVRead/OpKVWrite/OpMultiGet/OpCypher/OpCreateNode/OpCreateEdge` |
| `internal/bench/{kv,query,multiget,load}.go` | rewired onto the ops (behavior identical) |
| `internal/benchengine/metrics.go` (new) | `Hist` (HDR-lite), `Window`, `Sample`, percentile math |
| `internal/benchengine/engine.go` (new) | `Run` state machine, workers, pause gate, subscribe |
| `internal/benchengine/workloads.go` (new) | map `WorkloadSpec` → op closures (uses `internal/bench` ops) |
| `internal/benchui/server.go` (new) | mux, run registry, `New(...)`, `Handler()` |
| `internal/benchui/handlers.go` (new) | REST handlers (workloads, runs, target probe, dataset load) |
| `internal/benchui/sse.go` (new) | SSE streaming of samples + load progress |
| `internal/benchui/profile.go` (new) | profiling probe/capture/report/raw via `internal/profile` |
| `internal/benchui/embed.go` (new) | `//go:embed all:dist` SPA + `dist/.gitkeep` |
| `cmd/wavespan-benchui/main.go` (new) | flags + serve |
| `ui/bench.html`, `ui/vite.bench.config.ts`, `ui/src/bench-entry.tsx` | new Vite entry |
| `ui/src/bench/**` | dashboard: api client, target, workloads, controls, charts, profiling |
| `docker/Dockerfile.benchui` | multi-stage container |
| `.github/workflows/{ci,release}.yaml` | benchui image jobs |
| `.gitignore` | `/internal/benchui/dist/*` + `!.gitkeep` |

---

## Task 1: Refactor `internal/bench` — export clients + extract single-op funcs

**Files:** Modify `internal/bench/latency.go`, `kv.go`, `query.go`, `multiget.go`, `load.go`; Create `internal/bench/ops.go`, `internal/bench/ops_test.go`.

- [ ] **Step 1: Write failing test** `internal/bench/ops_test.go`:

```go
package bench

import "testing"

// The exported clients and single-op functions must exist with these signatures, so both the CLI
// runners and internal/benchengine can share them.
func TestExportedSurface(t *testing.T) {
	// compile-time assertions: these must be exported and have the expected shapes.
	_ = KVClient
	_ = CypherClient
	var _ func(ctxArg, KvServiceClientType, string, []byte) error = nil // placeholder; see note
	_ = OpKVRead
	_ = OpKVWrite
	_ = OpMultiGet
	_ = OpCypher
}
```

> Note: the exact op signatures are defined in Step 3. Replace the placeholder assertion with real ones once defined; the point of this test is that the symbols exist and the package compiles. Prefer simple behavioral tests where cheap (e.g. `OpCypher` strips/forms a request) over signature placeholders — see Step 3b.

- [ ] **Step 2: Run** `go test ./internal/bench/ -run TestExportedSurface` → FAIL (undefined `KVClient` etc.).

- [ ] **Step 3: Implement.** In `latency.go`, rename and export the constructors:

```go
// KVClient builds an H2C KV client for addr (host:port of a data port).
func KVClient(addr string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(rpcopts.H2CClient(), "http://"+addr)
}

// CypherClient builds an H2C Cypher client for addr.
func CypherClient(addr string) wavespanv1connect.CypherClient {
	return wavespanv1connect.NewCypherClient(rpcopts.H2CClient(), "http://"+addr)
}
```

Create `internal/bench/ops.go` with the per-op logic extracted verbatim from the current inlined loops (so behavior is identical). Each returns `(error)`; the *caller* times it (the engine and the CLI both wrap with `time.Now()`):

```go
package bench

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// OpKVRead does one Get (matches kv.go's read branch).
func OpKVRead(ctx context.Context, c wavespanv1connect.KvServiceClient, ns, key string) error {
	_, err := c.Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)}))
	return err
}

// OpKVWrite does one origin+1 Put (matches kv.go's write branch).
func OpKVWrite(ctx context.Context, c wavespanv1connect.KvServiceClient, ns, key string, value []byte) error {
	_, err := c.Put(ctx, connect.NewRequest(&wavespanv1.PutRequest{
		Namespace: ns, Key: []byte(key), Value: value, RequireOriginPlusOne: true,
	}))
	return err
}

// OpMultiGet fetches a batch (matches multiget.go).
func OpMultiGet(ctx context.Context, c wavespanv1connect.KvServiceClient, ns string, keys [][]byte) error {
	_, err := c.MultiGet(ctx, connect.NewRequest(&wavespanv1.MultiGetRequest{Namespace: ns, Keys: keys}))
	return err
}

// OpCypher runs one query and drains the stream (matches query.go / load.go execCypher).
func OpCypher(ctx context.Context, c wavespanv1connect.CypherClient, graph, query string) error {
	stream, err := c.Query(ctx, connect.NewRequest(&wavespanv1.CypherRequest{GraphId: graph, Query: query}))
	if err != nil {
		return err
	}
	for stream.Receive() { //nolint:revive // drain rows
	}
	return stream.Err()
}

// OpCreateNode / OpCreateEdge wrap OpCypher for the loader's CREATE statements (load.go).
func OpCreateNode(ctx context.Context, c wavespanv1connect.CypherClient, graph string, i int, city string) error {
	return OpCypher(ctx, c, graph, fmt.Sprintf("CREATE (:User {id:'user-%d', name:'User %d', age:%d, city:'%s'})", i, i, 18+i%60, city))
}
```

Then rewire `kv.go`, `multiget.go`, `query.go`, `load.go` to call these ops inside their existing worker loops (replacing the inline `client.Get/Put/...` bodies). Replace `kvClient(`/`cypherClient(` call sites with `KVClient(`/`CypherClient(`. Keep the loops, latency timing, error counting, and reporting exactly as they are — only the op body moves into the shared function.

- [ ] **Step 3b: Add a behavioral test** in `ops_test.go` that doesn't need a server: e.g. construct `KVClient("127.0.0.1:1")` and assert `OpKVRead(ctx, c, "ns", "k")` returns a non-nil error against a dead address (proves the op wires a request + surfaces transport errors). Use a short `context.WithTimeout`.

- [ ] **Step 4: Run** `go test ./internal/bench/... && go build ./cmd/wavespan-bench` → PASS. Confirm `RunKV`/`RunQueries`/`RunMultiGet`/`Load` still compile and behave (their tests, if any, pass).

- [ ] **Step 5: Commit**

```bash
git add internal/bench/
git commit -m "refactor(bench): export KVClient/CypherClient + extract single-op funcs (CLI unchanged)"
```

---

## Task 2: `internal/benchengine` metrics — HDR-lite histogram + windowed samples

**Files:** Create `internal/benchengine/metrics.go`, `internal/benchengine/metrics_test.go`.

- [ ] **Step 1: Failing test** (`metrics_test.go`): feed a known latency distribution into a `Hist`, assert percentiles within tolerance.

```go
package benchengine

import (
	"testing"
	"time"
)

func TestHistPercentiles(t *testing.T) {
	h := NewHist()
	for i := 1; i <= 1000; i++ { // 1ms..1000ms uniform
		h.Record(time.Duration(i) * time.Millisecond)
	}
	approx := func(got, want time.Duration) bool {
		lo, hi := time.Duration(float64(want)*0.95), time.Duration(float64(want)*1.05)
		return got >= lo && got <= hi
	}
	if p := h.Percentile(0.50); !approx(p, 500*time.Millisecond) {
		t.Fatalf("p50=%v want ~500ms", p)
	}
	if p := h.Percentile(0.99); !approx(p, 990*time.Millisecond) {
		t.Fatalf("p99=%v want ~990ms", p)
	}
}

func TestHistCountAndMerge(t *testing.T) {
	a, b := NewHist(), NewHist()
	a.Record(5 * time.Millisecond)
	b.Record(7 * time.Millisecond)
	a.Merge(b)
	if a.Count() != 2 {
		t.Fatalf("count=%d want 2", a.Count())
	}
}
```

- [ ] **Step 2: Run** → FAIL (`NewHist` undefined).

- [ ] **Step 3: Implement** `metrics.go`. A log-linear bucket histogram (HDR-lite): bucket index from `bits.Len` of the duration in microseconds, with sub-buckets, value reconstructed as the bucket midpoint. Provide `Record`, `Count`, `Percentile`, `Merge`, and a `Snapshot` of `{Tput?, P50,P95,P99,Errs,Total}` — note throughput is computed by the window, not the hist.

```go
package benchengine

import (
	"math/bits"
	"time"
)

const subBucketBits = 3 // 8 sub-buckets per power of two → ≤~7% relative error

// Hist is a fixed-bucket log-linear latency histogram (HDR-lite): cheap Record, approximate
// percentiles, no per-sample retention. Microsecond resolution, ~1µs..hours.
type Hist struct {
	buckets []uint64
	count   uint64
}

func NewHist() *Hist { return &Hist{buckets: make([]uint64, 64<<subBucketBits)} }

func bucketIndex(us uint64) int {
	if us == 0 {
		return 0
	}
	hi := bits.Len64(us) - 1 // power of two
	sub := 0
	if hi >= subBucketBits {
		sub = int((us >> (hi - subBucketBits)) & ((1 << subBucketBits) - 1))
	}
	return hi<<subBucketBits | sub
}

func bucketValueUS(idx int) uint64 { // midpoint reconstruction
	hi := idx >> subBucketBits
	sub := idx & ((1 << subBucketBits) - 1)
	if hi < subBucketBits {
		return uint64(idx)
	}
	base := uint64(1) << hi
	step := base >> subBucketBits
	return base + uint64(sub)*step + step/2
}

func (h *Hist) Record(d time.Duration) {
	us := uint64(d.Microseconds())
	i := bucketIndex(us)
	if i >= len(h.buckets) {
		i = len(h.buckets) - 1
	}
	h.buckets[i]++
	h.count++
}

func (h *Hist) Count() uint64 { return h.count }

func (h *Hist) Merge(o *Hist) {
	for i := range o.buckets {
		h.buckets[i] += o.buckets[i]
	}
	h.count += o.count
}

func (h *Hist) Percentile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	target := uint64(float64(h.count) * q)
	var cum uint64
	for i, c := range h.buckets {
		cum += c
		if cum >= target {
			return time.Duration(bucketValueUS(i)) * time.Microsecond
		}
	}
	return 0
}
```

Also define the per-second window + sample types:

```go
// WindowStat is one workload's stats over a window (or cumulative).
type WindowStat struct {
	Tput        float64       `json:"tput"`        // ops/sec
	P50, P95, P99 time.Duration `json:"-"`
	P50Ms, P95Ms, P99Ms float64 `json:"p50Ms,p95Ms,p99Ms"`
	Errs        uint64        `json:"errs"`
	Total       uint64        `json:"total"`
}

// Sample is one tick across all running workloads.
type Sample struct {
	TimeMs      int64                  `json:"timeMs"`
	PerWorkload map[string]WindowStat  `json:"perWorkload"`
}
```

- [ ] **Step 4: Run** `go test ./internal/benchengine/ -run TestHist` → PASS.
- [ ] **Step 5: Commit** `git add internal/benchengine/metrics.go internal/benchengine/metrics_test.go && git commit -m "feat(benchengine): HDR-lite latency histogram + window/sample types"`

---

## Task 3: `internal/benchengine` — controllable run engine

**Files:** Create `internal/benchengine/engine.go`, `internal/benchengine/workloads.go`, `internal/benchengine/engine_test.go`.

- [ ] **Step 1: Failing test** — drive the engine with an **injected fake op** (no network), assert the state machine + that pause stops issuing and samples flow.

```go
package benchengine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngineLifecycleWithFakeOp(t *testing.T) {
	var ops atomic.Int64
	r := newRunForTest(func(ctx context.Context) error { ops.Add(1); return nil }, 4 /*workers*/)

	if r.State() != StateIdle { t.Fatalf("state=%v", r.State()) }
	ch, unsub := r.Subscribe()
	defer unsub()
	r.Start()
	if r.State() != StateRunning { t.Fatal("not running") }

	// receive at least one sample
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no sample within 3s")
	}

	r.Pause()
	if r.State() != StatePaused { t.Fatal("not paused") }
	before := ops.Load()
	time.Sleep(200 * time.Millisecond)
	if grew := ops.Load() - before; grew > int64(4) { // allow in-flight ops to finish, not a flood
		t.Fatalf("ops kept growing while paused: +%d", grew)
	}
	r.Resume()
	time.Sleep(100 * time.Millisecond)
	if ops.Load() == before { t.Fatal("resume did not continue") }

	r.Stop()
	if r.State() != StateDone && r.State() != StateStopped { t.Fatalf("state=%v", r.State()) }
}
```

> `newRunForTest` is an internal constructor that builds a `Run` whose single workload calls the injected op (bypassing real clients). Expose it in `engine.go` (lowercase, test-only via same package).

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `engine.go`: the `Run` struct, `State`, sampler goroutine (per-second tick: snapshot each workload's window hist → emit `Sample`, then rotate window), pause gate (a `chan struct{}` that is closed when running and replaced when paused, or a `sync.RWMutex` held during pause), `Stop` via `context.CancelFunc`. Worker loop:

```go
for {
    if ctx.Err() != nil { return }
    g.wait()                       // blocks while paused
    start := time.Now()
    err := op(ctx)
    d := time.Since(start)
    if err != nil { w.recordErr() } else { w.record(d) }
}
```

`workloads.go`: `func opsFor(spec WorkloadSpec, dataAddr, graph string) (op func(context.Context) error, label string, err error)` mapping `"kv"`/`"multiget"`/`"cypher"` to closures over `bench.KVClient`/`bench.CypherClient` + the `bench.Op*` funcs (KV uses a per-worker RNG + read-ratio; cypher round-robins the loaded `bench.LoadQueries` set; multiget builds a batch). Public `New(Config)` validates and builds workloads; `Start/Pause/Resume/Stop/State/Subscribe/Summary` per the spec.

- [ ] **Step 4: Run** `go test ./internal/benchengine/ -race` → PASS (race-clean — this is concurrent).
- [ ] **Step 5: Commit** `git add internal/benchengine/ && git commit -m "feat(benchengine): controllable run engine (start/pause/resume/stop) + SSE samples"`

---

## Task 4: `internal/benchui` server skeleton + run registry

**Files:** Create `internal/benchui/server.go`, `internal/benchui/handlers.go`, `internal/benchui/server_test.go`, `internal/benchui/embed.go`, `internal/benchui/dist/.gitkeep`; Modify `.gitignore`.

- [ ] **Step 1:** `.gitignore` — add (root `/dist/` does not match nested):

```
/internal/benchui/dist/*
!/internal/benchui/dist/.gitkeep
```

Create `internal/benchui/dist/.gitkeep` (empty) and `embed.go`:

```go
package benchui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

func spaFS() fs.FS { sub, _ := fs.Sub(distFS, "dist"); return sub }
```

- [ ] **Step 2: Failing test** (`server_test.go`): `GET /api/workloads` returns the list; creating + starting a run transitions state; a 2nd concurrent run is rejected.

```go
func TestWorkloadsAndRunLifecycle(t *testing.T) {
	srv := New(Options{}) // no real cluster needed for create/state; Start may no-op against bad addr
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/api/workloads")
	if resp.StatusCode != 200 { t.Fatalf("workloads status=%d", resp.StatusCode) }
	// POST /api/runs with a kv workload against an unroutable addr; expect 200 + an id.
	// POST /api/runs again while one is active → 409.
}
```

- [ ] **Step 3: Implement** `server.go` (`Options{Addr}`, `Server{mux, mu, active *benchengine.Run, finished ring}`, `New`, `Handler()` returns the mux with routes + SPA file server fallback) and `handlers.go` (`/api/workloads`, `POST /api/runs` → `benchengine.New` + store as active, `409` if one active; `/api/runs/{id}` state JSON; `start/pause/resume/stop`). Use `http.ServeMux` (Go 1.22+ pattern routing). SPA: serve `spaFS()` for non-`/api` paths, falling back to `index.html`.

- [ ] **Step 4: Run** `go test ./internal/benchui/` → PASS. `go build ./...` (the `.gitkeep` lets `all:dist` compile).
- [ ] **Step 5: Commit** `git add internal/benchui .gitignore && git commit -m "feat(benchui): HTTP server, run registry, embedded SPA placeholder"`

---

## Task 5: `internal/benchui` SSE + dataset load progress

**Files:** Create `internal/benchui/sse.go`, `internal/benchui/sse_test.go`.

- [ ] **Step 1: Failing test:** start a run, open `GET /api/runs/{id}/stream`, read at least one `data:` line within a few seconds.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** SSE: set `Content-Type: text/event-stream`, subscribe to the run, write `data: <json>\n\n` + flush per sample, close on ctx done / run stop. `POST /api/dataset/load` runs `bench.Load` in a goroutine and streams its `progress(string)` callback as SSE lines.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(benchui): SSE sample stream + dataset-load progress"`

---

## Task 6: `internal/benchui` profiling endpoints

**Files:** Create `internal/benchui/profile.go`, `internal/benchui/profile_test.go`.

- [ ] **Step 1: Failing test:** `POST /api/target/probe` with a node whose adminAddr is dead returns `{reachable:false, profiling:false}` (uses `profile.Reachable`); `/api/profile/...` 404s for an unknown pid.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement:** `/api/target/probe` → for each node, `profile.Reachable(ctx, profile.Node{Name,AdminAddr})`; `POST /api/runs/{id}/profile` → `profile.CaptureCPU` + `CaptureSnapshots` over reachable nodes, `profile.BuildReport(...)`, store `*profile.Report` under a generated `pid`, return it. `GET /api/profile/{pid}/report` → serialize the report's `Sections`/`FuncStat`s to JSON. `GET /api/profile/{pid}/raw/{node}.{kind}.pb.gz` → write the stored raw bytes with `Content-Type: application/gzip`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `git commit -am "feat(benchui): profiling probe/capture/report/raw endpoints"`

---

## Task 7: `cmd/wavespan-benchui` entrypoint

**Files:** Create `cmd/wavespan-benchui/main.go`.

- [ ] **Step 1:** Implement: flag `--addr 127.0.0.1:8088`; `srv := benchui.New(benchui.Options{Addr: *addr})`; `http.ListenAndServe(*addr, srv.Handler())`; log the URL. (No test beyond build; it's wiring.)
- [ ] **Step 2: Run** `go build ./cmd/wavespan-benchui && go vet ./cmd/wavespan-benchui` → clean.
- [ ] **Step 3:** Add `wavespan-benchui` to the `CMDS`/`cmds` lists in `Makefile` + `justfile` and to `scripts/build-binaries.sh` `cmds=(...)`.
- [ ] **Step 4: Commit** `git add cmd/wavespan-benchui Makefile justfile scripts/build-binaries.sh && git commit -m "feat(benchui): wavespan-benchui server entrypoint"`

---

## Task 8: Frontend — Vite entry + API client + uPlot dep

**Files:** Create `ui/bench.html`, `ui/vite.bench.config.ts`, `ui/src/bench-entry.tsx`, `ui/src/bench/api.ts`; Modify `ui/package.json` (add `uplot` dep + `build:bench` script).

- [ ] **Step 1:** `ui/package.json`: add `"uplot": "^1.6.31"` to dependencies and script `"build:bench": "vite build --config vite.bench.config.ts"`. `ui/vite.bench.config.ts` mirrors `vite.docs.config.ts` but `outDir: "../internal/benchui/dist"`, `input: resolve(__dirname, "bench.html")`, `base: "./"`. `ui/bench.html` mirrors `ui/docs.html` (script → `/src/bench-entry.tsx`). `bench-entry.tsx` mounts `<ThemeProvider><BenchApp/></ThemeProvider>` with the three theme CSS imports (copy the imports from `docs-entry.tsx`).
- [ ] **Step 2:** `ui/src/bench/api.ts` — typed `fetch` wrappers for every endpoint + an `openSampleStream(runId, onSample)` using `EventSource`. Pure functions; export types `Sample`, `WorkloadSpec`, `ProbeResult`, `Report`.
- [ ] **Step 3: Verify build** `cd ui && npm ci && npm run build:bench` → outputs `internal/benchui/dist/` with `bench.html`+assets. (Add a placeholder `<BenchApp>` returning a heading so the build succeeds; real UI lands in Tasks 9–11.)
- [ ] **Step 4: Commit** `git add ui/bench.html ui/vite.bench.config.ts ui/src/bench-entry.tsx ui/src/bench/api.ts ui/package.json ui/package-lock.json && git commit -m "feat(bench-ui): vite entry, api client, uplot dep"`

---

## Task 9: Frontend — Target + Workloads panels

**Files:** Create `ui/src/bench/Target.tsx`, `ui/src/bench/Workloads.tsx`, `ui/src/bench/BenchApp.tsx` (compose).

- [ ] **Step 1:** `Target.tsx`: inputs for data addr + a list of `name=adminAddr` rows; **Probe** button calls `api.probe(...)`; renders per-node `Badge` (reachable / profiling ✓) using Linea `Badge`/`Panel`/`Input`/`Button`. `Workloads.tsx`: `Checkbox`+`Input` rows for KV (concurrency/keys/read-ratio), MultiGet (concurrency/batch/keys), Cypher (concurrency/graph), plus a **Prepare dataset** action (users/follows/kv → `api.loadDataset` with progress). State lifts into `BenchApp`.
- [ ] **Step 2: Verify** `npm run build:bench` succeeds; `npm run typecheck` clean.
- [ ] **Step 3: Commit** `git commit -am "feat(bench-ui): target + workloads panels"`

---

## Task 10: Frontend — Run controls + live uPlot charts

**Files:** Create `ui/src/bench/TimeSeries.tsx` (uPlot React wrapper), `ui/src/bench/RunControls.tsx`, `ui/src/bench/Charts.tsx`.

- [ ] **Step 1:** `TimeSeries.tsx`: a small wrapper that creates a `uPlot` instance in a ref'd div, exposes an imperative `push(t, values)` (ring buffer, `setData`), disposes on unmount. `RunControls.tsx`: Start/Pause/Resume/Stop buttons wired to `api`, elapsed timer, state chip. `Charts.tsx`: two `TimeSeries` (throughput; p50/p95/p99) fed from the SSE sample stream opened in `BenchApp` on Start; `StatCard`s for current tput/p99/errs.
- [ ] **Step 2:** Wire in `BenchApp`: Start → `api.createRun` → `api.start` → `openSampleStream` → push to charts; Pause/Resume/Stop call the matching endpoints; Stop → fetch summary.
- [ ] **Step 3: Verify** `npm run build:bench` + `typecheck` clean.
- [ ] **Step 4: Commit** `git commit -am "feat(bench-ui): run controls + live uPlot throughput/latency charts"`

---

## Task 11: Frontend — Profiling panel

**Files:** Create `ui/src/bench/Profiling.tsx`.

- [ ] **Step 1:** Shown only when a probed node reports `profiling:true`. A **Capture (N s)** button → `api.captureProfile(runId, seconds)` → `api.profileReport(pid)`; render the four sections (CPU / Block=latency / Mutex / Heap) as Linea `Table`s with the hottest-leaf callout; per-node `.pb.gz` download links (`<a href={api.rawProfileURL(pid,node,kind)}>`).
- [ ] **Step 2: Verify** build + typecheck clean.
- [ ] **Step 3: Commit** `git commit -am "feat(bench-ui): profiling panel (breakdown + raw download)"`

---

## Task 12: Container — `docker/Dockerfile.benchui`

**Files:** Create `docker/Dockerfile.benchui`.

- [ ] **Step 1:** Multi-stage, mirroring `docker/Dockerfile`'s context (workspace root with `waveSpan/`+`wavesdb/` siblings) but with a Node stage first:

```dockerfile
# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM node:20 AS ui
WORKDIR /ui
COPY waveSpan/ui/package.json waveSpan/ui/package-lock.json ./
RUN npm ci
COPY waveSpan/ui/ ./
RUN npm run build:bench           # → /waveSpan/internal/benchui/dist (relative outDir)

FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY wavesdb/ ./wavesdb/
COPY waveSpan/ ./waveSpan/
COPY --from=ui /waveSpan/internal/benchui/dist/ ./waveSpan/internal/benchui/dist/
WORKDIR /src/waveSpan
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/wavespan-benchui ./cmd/wavespan-benchui

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/wavespan-benchui /wavespan-benchui
EXPOSE 8088
ENTRYPOINT ["/wavespan-benchui"]
CMD ["--addr", "0.0.0.0:8088"]
```

> The Node stage's `outDir: ../internal/benchui/dist` writes relative to `/ui` → `/waveSpan/internal/benchui/dist`; adjust the `COPY --from=ui` path to wherever the vite outDir resolves (verify the WORKDIR math; simplest is to set the Node stage WORKDIR to `/waveSpan/ui` so the `../internal/benchui/dist` resolves to `/waveSpan/internal/benchui/dist`). The container binds `0.0.0.0` (inside the container) — document that operators put it behind their own network controls.

- [ ] **Step 2: Verify locally** (build context = parent of waveSpan):

```bash
cd /Volumes/HOME/code/storage-engines
docker buildx build -f waveSpan/docker/Dockerfile.benchui -t wavespan-benchui:test --load .
```
Expected: builds; `docker run --rm -p 8088:8088 wavespan-benchui:test` serves the SPA at `:8088`.

- [ ] **Step 3: Commit** `git add docker/Dockerfile.benchui && git commit -m "feat(benchui): multi-stage container (node UI build + scratch binary)"`

---

## Task 13: CI/release — build + push the benchui image

**Files:** Modify `.github/workflows/ci.yaml`, `.github/workflows/release.yaml`.

- [ ] **Step 1:** Add a `benchui-image` job to `ci.yaml` mirroring the existing `image` job (checkout wavespan + wavesdb with `WAVESDB_TOKEN`, qemu+buildx, GHCR login on non-PR, metadata-action, build-push) but `file: waveSpan/docker/Dockerfile.benchui` and `images: ghcr.io/${{ github.repository_owner }}/wavespan-benchui`, `needs: build-test`. Add a matching versioned job to `release.yaml`.
- [ ] **Step 2: Validate** `actionlint .github/workflows/*.yaml`; YAML parses.
- [ ] **Step 3: Commit** `git commit -am "ci: build + push wavespan-benchui image (ghcr) on main + tags"`

---

## Final verification
- [ ] `go build ./... && go test ./internal/bench/... ./internal/benchengine/... ./internal/benchui/...` green (`-race` for benchengine/benchui).
- [ ] `go test ./...` full suite green; `golangci-lint run` 0 issues; `gofmt -l` clean.
- [ ] `cd ui && npm run build:bench && npm run typecheck` clean.
- [ ] `wavespan-bench` CLI unchanged: `go run ./cmd/wavespan-bench kv --help` works; CLI behavior identical.
- [ ] Manual smoke: `go run ./cmd/wavespan-benchui` → open `http://127.0.0.1:8088`, probe a local docker cluster (`make docker-up`, data `:7811`), run a KV workload, see live charts; if profiling cluster, capture a profile.
- [ ] Push branch; CI green incl. the new `benchui-image` job.
