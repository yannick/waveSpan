# Benchmarking Collections in benchui — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Add benchmark workloads for the `CollectionService` APIs to `wavespan-benchui` — set / hash /
sorted-set primitives, the atomic-counter special functions (`HIncrBy`/`HIncrByFloat`), and **`BulkRemove`**,
with the centerpiece being **"remove a specific element from a huge number of sets"** (up to ~1M) at cluster scale.

**Architecture:** Mirror the existing kv/cypher workload path. `internal/bench` gets a collections H2C client
+ single-op funcs; `internal/benchengine` gets `set`/`hash`/`zset` closed-loop workloads plus a dedicated
**bulk-remove job** (seed → time the fan-out → sets/sec, with an N-sweep) and a closed-loop **`bulkremove`**
workload; `internal/benchui` advertises them + adds seed/bulk-remove endpoints; the React dashboard adds the
workload rows and a Bulk-Remove panel.

**Decisions (confirmed):** ship **both** bulk-remove modes (full-namespace one-shot + N-sweep, and closed-loop
batched); seed ceiling **1M** sets; benchmark **all three families** (set/hash/zset) **+ counters**.

**Cost model (why):** `BulkRemove(ns, collections=[], members=[X])` = *every collection in the namespace*
(`internal/collections/bulk.go:32-52`): one read per shard to list, then **one Raft propose per collection,
sequentially** → O(N) write fan-out. The sweep visualizes that scaling.

---

## File structure

| Path | Responsibility |
|---|---|
| `internal/bench/collections.go` (new) | `CollectionsClient(addr)` + single-op funcs (set/hash/zset/counter/bulk) |
| `internal/benchengine/workloads.go` | add `set`/`hash`/`zset`/`bulkremove` cases to `opsFor` |
| `internal/benchengine/collections_seed.go` (new) | concurrent seeder for N sets (+ a re-add helper) |
| `internal/benchengine/bulkremove.go` (new) | full-namespace remove job + N-sweep (one-shot, not closed-loop) |
| `internal/benchui/collections.go` (new) | catalog entries + `POST /api/collections/seed` (SSE) + `POST /api/collections/bulk-remove` (result/sweep, SSE) |
| `internal/benchui/handlers.go` | register the new routes + catalog |
| `ui/src/bench/Workloads.tsx` | set/hash/zset param rows |
| `ui/src/bench/BulkRemove.tsx` (new) | seed + full-namespace run + sweep chart |
| `ui/src/bench/api.ts`, `BenchApp.tsx` | wire endpoints + panel |
| `bench/README.md` | document the collections workloads |

---

## Task 1: `internal/bench/collections.go` — H2C client + single-op funcs

**Files:** Create `internal/bench/collections.go`, `internal/bench/collections_test.go`.

The ops mirror `sdk/go/collections.go`'s request construction but over `internal/bench`'s shared H2C client.
Each returns `error` (caller times it); `BulkRemove` also returns the collection count for sets/sec.

- [ ] **Step 1: failing test** — symbol existence + a dead-addr transport-error test (mirror `ops_test.go`'s `TestOpKVReadDeadAddr`): `c := CollectionsClient("127.0.0.1:1")`, `OpSAdd(ctx, c, "ns", []byte("s"), []byte("m"))` returns non-nil with a 1s context.

- [ ] **Step 2: run** → FAIL.

- [ ] **Step 3: implement** `collections.go`:

```go
package bench

import (
	"context"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
	"github.com/yannick/wavespan/internal/rpcopts"
)

// CollectionsClient builds an H2C CollectionService client for addr (a data port).
func CollectionsClient(addr string) wavespanv1connect.CollectionServiceClient {
	return wavespanv1connect.NewCollectionServiceClient(rpcopts.H2CClient(), "http://"+addr)
}

// --- set ---
func OpSAdd(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SAdd(ctx, connect.NewRequest(&wavespanv1.SAddRequest{Namespace: ns, Collection: coll, Members: members}))
	return err
}
func OpSRem(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SRem(ctx, connect.NewRequest(&wavespanv1.KeysRequest{Namespace: ns, Collection: coll, Keys: members}))
	return err
}
func OpSIsMember(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.SIsMember(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member}))
	return err
}
func OpSCard(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte) error {
	_, err := c.SCard(ctx, connect.NewRequest(&wavespanv1.CardRequest{Namespace: ns, Collection: coll}))
	return err
}

// --- hash (incl. HIncrBy atomic counter) ---
func OpHSet(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field, value []byte) error {
	_, err := c.HSet(ctx, connect.NewRequest(&wavespanv1.HSetRequest{Namespace: ns, Collection: coll,
		Fields: []*wavespanv1.FieldValue{{Field: field, Value: value}}}))
	return err
}
func OpHGet(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field []byte) error {
	_, err := c.HGet(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: field}))
	return err
}
func OpHIncrBy(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field []byte, delta int64) error {
	_, err := c.HIncrBy(ctx, connect.NewRequest(&wavespanv1.HIncrByRequest{Namespace: ns, Collection: coll, Field: field, Delta: delta}))
	return err
}

// --- sorted set ---
func OpZAdd(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte, score float64) error {
	_, err := c.ZAdd(ctx, connect.NewRequest(&wavespanv1.ZAddRequest{Namespace: ns, Collection: coll,
		Members: []*wavespanv1.ScoredMember{{Member: member, Score: score}}}))
	return err
}
func OpZScore(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.ZScore(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member}))
	return err
}

// --- bulk remove --- returns the number of collections the fan-out touched (for sets/sec) + total removed.
func OpBulkRemove(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, colls, members [][]byte) (colls int, removed uint64, err error) {
	resp, e := c.BulkRemove(ctx, connect.NewRequest(&wavespanv1.BulkRemoveRequest{Namespace: ns, Collections: colls, Members: members}))
	if e != nil {
		return 0, 0, e
	}
	for _, r := range resp.Msg.GetResults() {
		removed += r.GetRemoved()
	}
	return len(resp.Msg.GetResults()), removed, nil
}
```

> Verify the exact connect method names + result getters against `proto/.../wavespanv1connect/collections.connect.go` and `collections.pb.go` (e.g. `BulkRemoveResult.GetResults()`, `BulkRemoveEntry.GetRemoved()`). Adapt if a getter name differs.

- [ ] **Step 4: run** `go test ./internal/bench/... && go build ./... && go vet ./internal/bench/ && gofmt -l internal/bench` → clean.
- [ ] **Step 5: commit** `git add internal/bench/ && git commit -m "feat(bench): collections H2C client + single-op funcs (set/hash/zset/counter/bulk)"`

## Task 2: benchengine closed-loop workloads — set / hash / zset

**Files:** `internal/benchengine/workloads.go`, `internal/benchengine/workloads_test.go` (extend).

- [ ] **Step 1: failing test** — `opsFor(WorkloadSpec{Kind:"set", Params:{...}}, cfg)` returns a non-nil op + label "set"; same for "hash"/"zset"; an unknown kind errors. (Pure construction; don't hit a server.)
- [ ] **Step 2: run** → FAIL.
- [ ] **Step 3: implement** — add cases to `opsFor` over `bench.CollectionsClient(cfg.DataAddr)`, picking a random collection `fmt.Sprintf("set/%d", rand.IntN(collections))` and a random member each call (use `math/rand/v2`, goroutine-safe):
  - `set`: params `collections`, `members`, `writeRatio` → SAdd vs (SIsMember|SCard) read.
  - `hash`: params `collections`, `fields`, `writeRatio`, `counterRatio` → HSet vs HGet, with `counterRatio` of writes routed to `OpHIncrBy` on a hot field (exercises the atomic counter).
  - `zset`: params `collections`, `members`, `writeRatio` → ZAdd(random score) vs ZScore.
  Each uses namespace param (default "bench-collections"). Reuse `intParam`/`floatParam`/`strParam`.
- [ ] **Step 4: run** `go test ./internal/benchengine/ -race && go build ./...` → PASS.
- [ ] **Step 5: commit** `git commit -am "feat(benchengine): set/hash/zset closed-loop workloads (incl. HIncrBy counter)"`

## Task 3: benchengine seeder + bulk-remove job (+ closed-loop bulkremove)

**Files:** Create `internal/benchengine/collections_seed.go`, `internal/benchengine/bulkremove.go`, tests.

- [ ] **Step 1: failing tests** (use an injectable op so no server needed):
  - seeder calls the per-set add op exactly N times (count with an atomic);
  - `bulkRemoveOnce` returns `{Sets, Removed, WallMs, SetsPerSec}` computed from an injected op that reports a collection count;
  - the sweep runs once per N in the list and returns a point per N.
- [ ] **Step 2: run** → FAIL.
- [ ] **Step 3: implement**:
  - `collections_seed.go`: `SeedSets(ctx, dataAddr, ns string, n, filler int, member []byte, conc int, progress func(done, total int))` — a worker pool (mirror `bench.Load`'s `runPool`) that for each `i` does `OpSAdd(set/i, [member, filler bytes...])`; streams progress. A `ReAddMember(ctx, ..., colls [][]byte, member)` helper re-seeds a batch (for repeat/closed-loop).
  - `bulkremove.go`:
    - `type BulkRemoveResult struct { Sets int; Removed uint64; WallMs int64; SetsPerSec float64; Errors int }` (json-tagged camelCase).
    - `RunFullNamespaceRemove(ctx, dataAddr, ns string, member []byte) (BulkRemoveResult, error)` — `start := now; colls, removed, err := bench.OpBulkRemove(ctx, c, ns, nil, [member]); wall := since(start)` → SetsPerSec = colls/wall.s.
    - `Sweep(ctx, dataAddr, ns string, member []byte, ns []int, filler, conc int, progress) ([]SweepPoint, error)` — for each N: clear/new sub-namespace, `SeedSets(N)`, `RunFullNamespaceRemove`, append `{N, SetsPerSec, WallMs}`.
  - Closed-loop `bulkremove` workload in `opsFor` (Task 2 file or here): params `batch` (K sets/call), over a pre-seeded pool; each op = `OpBulkRemove(ns, [K explicit set keys], [member])` then re-add to those K. (Use explicit collections to avoid the full-namespace scan per call.)
- [ ] **Step 4: run** `go test ./internal/benchengine/ -race && go build ./...` → PASS.
- [ ] **Step 5: commit** `git commit -am "feat(benchengine): collections seeder + full-namespace bulk-remove job + N-sweep"`

## Task 4: benchui endpoints + catalog

**Files:** Create `internal/benchui/collections.go`, `internal/benchui/collections_test.go`; modify `handlers.go`/`server.go` to register routes + catalog.

- [ ] **Step 1: failing test** — `GET /api/workloads` now includes `set`/`hash`/`zset`/`bulkremove`; `POST /api/collections/bulk-remove` with a bad/unroutable addr returns a structured error (not a panic); seed endpoint streams SSE.
- [ ] **Step 2: run** → FAIL.
- [ ] **Step 3: implement**:
  - extend `workloadCatalog` with the four kinds + their params.
  - `POST /api/collections/seed` `{dataAddr, namespace, sets, filler, member, concurrency}` → run `benchengine.SeedSets` in a goroutine, stream `done/total` as SSE (mirror dataset-load SSE).
  - `POST /api/collections/bulk-remove` `{dataAddr, namespace, member, sweep:[]int?}` → if `sweep` set, run `Sweep` and stream/return the points; else run `RunFullNamespaceRemove` and return `BulkRemoveResult`. Use a generous request context (the fan-out over 1M sets is long-running) — stream progress so the client isn't left hanging; cap with `http.MaxBytesReader` on the body.
- [ ] **Step 4: run** `go test ./internal/benchui/ -race && go build ./...` → PASS.
- [ ] **Step 5: commit** `git commit -am "feat(benchui): collections workloads catalog + seed/bulk-remove endpoints"`

## Task 5: frontend — workload rows + Bulk-Remove panel

**Files:** `ui/src/bench/api.ts`, `ui/src/bench/Workloads.tsx`, `ui/src/bench/BulkRemove.tsx` (new), `ui/src/bench/BenchApp.tsx`.

- [ ] **Step 1:** `api.ts` — add types + wrappers for the new endpoints (`seedCollections` SSE, `bulkRemove`/`bulkRemoveSweep`), and `PARAM_HINTS` for set/hash/zset.
- [ ] **Step 2:** `Workloads.tsx` — `PARAM_HINTS` entries: `set:["concurrency","collections","members","writeRatio"]`, `hash:["concurrency","collections","fields","writeRatio","counterRatio"]`, `zset:["concurrency","collections","members","writeRatio"]`. (The server catalog drives the rest.)
- [ ] **Step 3:** `BulkRemove.tsx` (Linea) — namespace + sets(N, up to 1e6) + filler + member inputs; **Seed** button (SSE progress bar); **Remove from all sets** button → shows headline **`sets/sec`** + wall-clock + errors (`StatCard`s); a **Scaling sweep** toggle (N = 1k,10k,100k,1M) → a uPlot chart of sets/sec vs N (log-x). Big-N warning on seed.
- [ ] **Step 4:** wire `BulkRemove` into `BenchApp` (a full-width panel below the charts). `npm run build:bench && npm run typecheck` clean.
- [ ] **Step 5: commit** `git add ui/src/bench && git commit -m "feat(bench-ui): collections workload rows + Bulk-Remove panel (seed/run/sweep)"`

## Task 6: end-to-end against a live cluster

- [ ] Start a local cluster (or use the running one); via the UI or `curl`: seed e.g. **50k** sets (`member="doomed"`), run the full-namespace BulkRemove, confirm `sets/sec` is reported and `errors==0`; run a small sweep (1k,10k,50k) and confirm sets/sec-vs-N trends down (the O(N) cost). Also run the `set`/`hash`/`zset` closed-loop workloads briefly and confirm live charts move. Record the numbers in the commit message.
- [ ] Commit any fixes found.

## Task 7: docs

- [ ] `bench/README.md` — add a "Collections" section: the set/hash/zset workloads, the HIncrBy counter, and the bulk-remove (full-namespace + sweep) with a note on the O(N) fan-out cost. Commit.

## Final verification
- [ ] `go build ./... && go test ./... -race` (relevant pkgs) green; `golangci-lint run` 0 issues; `gofmt -l` clean.
- [ ] `cd ui && npm run build:bench && npm run typecheck` clean; CLI (`wavespan-bench`) still unaffected.
- [ ] Push branch; CI green incl. the benchui image build.
