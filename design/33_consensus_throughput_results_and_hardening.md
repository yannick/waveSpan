# 33 — Consensus tier: speedup levers, overload hardening, and staging results

Status: implemented + verified on the cwire ovh-stag cluster (`main-43cb4ea`).
Companion to [32 — consensus tier throughput plan](32_consensus_tier_throughput.md) (the plan); this
doc records what was built, a crash discovered under load and how it was hardened, and the measured
numbers.

## 1. What was implemented (the levers from doc 32)

All within `internal/collections` + env wiring in `cmd/wavespan-node/main.go`. Storage/IO untouched
(separate rewrite effort).

- **A1 — kill consensus-path allocations** (the profiled 63%-GC ceiling). `command.go`: a `sync.Pool`
  of encode buffers + a decode-into-scratch path. `statemachine.go`: reused per-apply scratch across
  entries. Effect: concurrent SAdd **58 → 24 allocs/op**.
- **QW2 — batching/pipelining proposer** (`proposer.go`, new). A per-shard queue coalesces concurrent
  single-op writes arriving within a small window (200µs / 256 ops) into ONE `opBatch` Raft entry,
  results routed back per op. `manager.go` routes coalescable data-shard mutations through it. Effect:
  single-client concurrent SAdd **10.5ms → 1.24ms/op (~8.5×)** in-process; on-cluster it scales
  throughput with concurrency at flat latency (the coalescing signature).
- **QW3 — tiered reads.** Reads default to STALE/local (linearizable is opt-in per call), served off
  any local replica incl. demand-filled spot learners. `read()` self-heals a stale/missed range
  directory (refresh-on-miss + a 5s refresh ticker in `control.go`).
- **D1 — static hash pre-split into N data shards** (`directory.go` `HashDirectory`, `control.go`
  `BootstrapN`). Default **N=4** (`WAVESPAN_COLLECTIONS_DATA_SHARDS`), routed by `hash((ns,coll))` —
  4 independent Raft groups / leaders / apply loops. Leaders distribute across the 3 core voters.

## 2. Overload hardening — "never bring it down"

A flood (~800 concurrent writes) **crash-looped all 3 voters**: under load a pooled encode buffer (A1)
was reused before dragonboat copied it → a corrupted committed Raft entry → the on-disk SM's `Update`
returned `collections: short command` → **dragonboat treats ANY `Update` error as fatal and panics**
→ on recovery the poison entry replays → permanent crash-loop. Four fixes (each with `-race` tests):

1. **SM never errors/panics on a committed entry** (`sm_fatal.go`, `statemachine.go`). Each entry
   applies under `recover()` with `updateCtx.snapshot()/restore()`; a decode/corruption error or panic
   truncates that entry's partial ops, rolls back the overlay maps, logs once, and skips it
   deterministically (all replicas skip the same entry → consistent). Only genuine storage faults
   (`fatalErr`) propagate. This breaks the crash-loop class entirely.
2. **Buffer-reuse race killed** (`proposer.go`). `Propose` copies the command bytes into a
   proposer-owned buffer at enqueue (caller lifetime fully decoupled from the async flush), and drops
   jobs whose ctx is already cancelled so one expired deadline can't poison a whole batch.
3. **Admission control / load shedding** (`grpcsrv/server.go`, `proposer.go`, `manager.go`). Bounded
   gRPC `MaxConcurrentStreams` (2048) + an in-flight unary limiter (4096) returning
   `ResourceExhausted`; the proposer's per-shard enqueue is non-blocking → `ErrBusy` when full;
   dragonboat `ErrSystemBusy`/`ErrShardNotReady` map to the transient `ErrBusy` (not fatal, not
   forwarded). Floods are rejected, not absorbed into corruption.
4. **`BatchRC` apply** (`statemachine.go`, `meta.go`, `base_sm.go`). The deterministic SM apply wrote
   shard-prefixed keys via Snapshot-isolation `Batch`; with N=4 shards applying concurrently to the
   shared store the SI write-write check **spuriously** aborted with `ErrConflict` → fatal → node
   restart under heavy mixed load (quorum held, but a 2-of-3 simultaneous hit risks downtime). The
   apply is authoritative and orders its own same-key writes, so it needs no conflict check: switched
   to `BatchRC` (ReadCommitted) so independent shards commit in parallel.

**Verified on the live cluster:** a **2,800-concurrent flood** and the **heavy mixed benchmark** both
ran with **0 core restarts** and quorum intact. The invariant holds: no input (malformed, flooding,
corrupt) crashes a node.

## 3. Staging results (3-voter core StatefulSet, N=4, RTT 5ms, core CPU 4)

Single-client baseline (conc 64) → multi-client aggregate (4 benchui):

| workload | baseline (pre-levers) | aggregate (post-levers, hardened) |
|---|---:|---:|
| consensus write (SADD) | ~2,000 ops/s | **~10,400** ops/s (dedicated) / ~3–9k under mixed load |
| consensus write (HSET/HINCRBY) | ~2,080 ops/s | ~9,100 ops/s |
| consensus write (ZADD) | ~1,760 ops/s | ~8,100 ops/s |
| set/hash reads (stale) | ~16,000 ops/s | **30,000–62,000** ops/s |
| KV (90% read, local tier) | ~13,000 ops/s | **~47,000** ops/s |

- Reads: **30–62k ops/s** (collections), 47k KV — ~1–14ms, served locally (QW3).
- Writes: **~8–10k ops/s aggregate, ~5× the single-client baseline** — batching (QW2) + distributed
  N=4 leaders (D1) + fewer allocs (A1). The latency floor under load is the ~60–80ms 3-replica Raft
  round-trip.
- Coalescing is real: single-client SADD scales **1,936 → 4,127 → 6,044 ops/s** (conc 64→128→256)
  with p50 flat at ~22–27ms.

## 4. Remaining bottlenecks / further work

- **Write latency floor** = the 3-replica Raft commit round-trip (~22ms idle, ~60–80ms loaded). To go
  faster: fewer round-trips per op is already done (batching); the next lever is reducing the commit
  fan-out cost (e.g. follower pipelining, larger in-mem log) — but the IO rewrite owns the fsync path.
- **Forward hop**: writes via a spot (`wavespan-local`) forward per-op to the shard leader. Batching
  forwarded writes (design/32 D2) would cut that.
- **Headroom**: cores ran at ~2.7–3.0 of 4 — write throughput is not yet CPU-bound at these client
  counts; more clients / higher per-client concurrency push it further.
- **More shards**: N>4 adds parallel Raft groups (now that the BatchRC conflict is gone) at the cost of
  more raft overhead per node — worth sweeping.

## 5. Operational notes

- **Topology**: stable core = `wavespan-core` StatefulSet (3 voters, anti-affinity, high-speed PVC,
  PDB maxUnavailable 1, per-ordinal DNS = stable raft identity, binds `:8800` / advertises DNS). Spot
  tier = `wavespan-node` DaemonSet (ephemeral, non-voting learners). Gossip seeds from the stable core.
- **Knobs**: `WAVESPAN_COLLECTIONS_DATA_SHARDS` (default 4); RTT 5ms / heartbeat 50ms / election 500ms
  (intra-region — too-short election timeouts thrash leadership); gRPC `MaxConcurrentStreams` /
  in-flight limiter; proposer coalesce window/maxOps.
- **Known rough edge**: a one-at-a-time StatefulSet rolling update can leave the data shards in
  election churn (no leader); a simultaneous core restart re-elects cleanly. A roll that preserves
  quorum-aware ordering is future work.

## 6. GC tuning — the write path was GC-bound (addendum)

Re-profiling a core under sustained write load showed **~88% of CPU in `runtime.gcDrain`** — the cores
were burning ~2.3 of ~2.7 used cores collecting garbage, with only ~0.3 cores doing real work. Root
cause: the core pods set **no `GOGC`** (default 100 = collect when the heap doubles), while a fast-
churning write path doubles the heap constantly. A1's per-op alloc cut helped but the gRPC/proto +
forward-path allocations dominate.

Fix (config only): `GOGC=600` + `GOMEMLIMIT=3GiB` on the core (it uses <1Gi of its 4Gi, so the heap
can grow far between collections). Result: **core CPU under the same load dropped from ~3.2 to ~1.0
cores** and the write ceiling moved **off the cores** — the cluster now has large headroom and write
throughput scales with client concurrency (conc 400→700 → ~6.4k→8.3k aggregate, 0 restarts).

The remaining write ceiling is now the **per-op forward hop** (client → spot → shard leader) and
client/gRPC concurrency — not core CPU. Next levers: a shard-aware client that routes each write to
its shard leader (eliminating the forward), and batching forwarded writes (doc 32 §D2).
