# M04 - Target-N fanout, holder directory, and repair engine

**Milestone:** M4 (`design/18_implementation_roadmap.md` "Milestone 4")
**Tickets:** TS-033, TS-034, TS-035 (`design/19_agent_work_items.md`)
**Depends on:** M3 (origin+1 coordinator, StoreReplica, placement)
**Enables:** M5 (cache fetch needs the holder directory), M6 (routed scans), M7 (global
fanout reuses the same async machinery)

## Context

M3 acknowledges after origin+1. M4 makes the system **converge to the configured target
nearby replica count `N`** and keep it there under spot churn (`design/README.md` hard rule 8;
`design/05_special_cache_replication.md` "Repair loop"). It delivers three things: asynchronous
target-N fanout (the background fill that runs after ACK), the **holder directory** so a read
miss can find a holder without broadcasting (`design/README.md` hard rule 3;
`design/04_membership_latency_gossip.md` "Range directory"/"Holder summaries"), and the
**repair engine** — a priority queue with a rate limit and churn backpressure
(`design/23_repair_engine.md`) that restores replicas when holders die.

This milestone is the most exposed to the top risk in the register: **spot churn outpacing
repair** (`IMPLEMENTATION_STRATEGY.md` section 4). The chaos "kill every 10-60s" scenario must
converge after this lands.

## Files to create

```
internal/replication/local/fanout.go        async target-N fill worker (post-ACK)
internal/replication/local/holder_dir.go     holder records (local) + gossiped HolderSummary integration
internal/membership/holder.go                EXTEND: populate range directory from holder summaries
internal/replication/local/repair.go         repair engine: priority queue, rate limit, churn backpressure
internal/replication/local/repair_queue.go   under-replication priority queue (severity-ordered)
internal/observability/metrics.go            EXTEND: under-replication + fill-lag + repair metrics
internal/replication/local/fanout_test.go    target reached after ACK; failures queued for repair
internal/replication/local/holder_dir_test.go  miss resolves a holder without broadcast
internal/replication/local/repair_test.go    dead holder -> replacement replica; convergence
tests/integration/repair_test.go             3-node: kill holder -> repair converges, no manual action
tests/chaos/spot_churn_test.go               kill-every-10-60s convergence (nightly)
```

## Steps

1. **Target-N fanout (TS-033), `internal/replication/local/fanout.go`.** Implement the
   background worker the M3 coordinator enqueued at `FANOUT_TARGET_N`
   (`design/05` write state machine). After ACK, asynchronously `StoreReplica` to additional
   candidates (from M3 placement) until `targetNearbyReplicaCount` distinct durable holders
   exist. On a candidate failure, **do not fail the write** (already ACKed) — record the gap
   for repair (`design/05` failure paths: "FANOUT_TARGET_N failure -> ACK already returned;
   schedule repair"). TS-033 acceptance: target count reached after ACK; failures queued for
   repair.

2. **Holder directory (TS-034), `internal/replication/local/holder_dir.go`.** Record each
   known durable holder per `(namespace, range, key)` locally
   (`/kv/{ns}/holder/{range_id}/{key_hash}/{member_id}` from `design/01_architecture.md`
   "Internal keyspace"), and roll these up into the compact `HolderSummary` (bloom filter +
   key count + watermarks) that gossip already carries (M2). Populate the **range directory**
   (`design/04` "Range directory") from incoming summaries so a get miss can ask the directory
   for likely holders. TS-034 acceptance: a get miss can find a holder **without broadcast**
   (`design/README.md` hard rule 3). The directory need not be perfectly accurate — a stale
   holder just triggers fallback (`design/04` "Range directory").

3. **Repair engine (TS-035), `internal/replication/local/repair.go` + `repair_queue.go`.**
   Per `design/23_repair_engine.md`:
   - **Priority queue** keyed by under-replication **severity** (how far below target, age,
     whether a tombstone is needed for convergence). Most-under-replicated keys/ranges drain
     first.
   - **Inputs** (`design/05` "Repair loop"): holder summaries, dead/suspect members (M2
     liveness), mutation logs, random key sampling, TTL tombstones (M6), global lag (M7) —
     wire the inputs that exist now; stub the later ones.
   - **Action:** when `durable_holder_count(key) < targetNearbyReplicaCount`, select a new
     nearby candidate (M3 placement), copy the latest winning record + siblings/tombstone via
     `StoreReplica`, and update the holder directory.
   - **Rate limit + churn backpressure** (`design/23`): bound repair concurrency and bytes/s;
     when churn (suspect/dead rate) spikes, back off new repairs so repair traffic does not
     amplify instability. Do **not** repair expired keys unless a tombstone is needed for
     convergence (`design/05` "Repair loop").

4. **Metrics, `internal/observability/metrics.go`.** Add `kv_write_target_n_fill_lag_ms`,
   `kv_under_replicated_keys_estimate`, repair queue depth, and repair throughput/backpressure
   gauges (`design/05` "Metrics"; `design/14_observability.md`). `kv_under_replicated_keys_estimate`
   is the alerting signal for the spot-churn risk.

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 4 and the TS tickets:

- Target-N is reached after ACK; fanout failures are queued for repair. (M4; TS-033)
- A get miss finds a holder without broadcast. (TS-034)
- Killing one holder triggers replacement-replica creation and the system converges to target
  with no manual action. (M4; TS-035)
- Under-replication metrics are exposed. (M4)

## Verification

1. **Unit:** `fanout_test` asserts target-N is reached and that an injected candidate failure
   enqueues a repair item (TS-033); `holder_dir_test` asserts a miss resolves a holder via the
   directory with zero broadcast RPCs (TS-034); `repair_test` builds an under-replicated key,
   marks a holder dead, and asserts the engine creates a replacement and converges to target,
   and that the priority queue drains most-under-replicated first and respects the rate limit
   (TS-035, `design/23`).
2. **Docker integration (`tests/integration/repair_test.go`)** on the 3-node cluster
   (`design/16_testing_strategy.md` "target-N fanout fills asynchronously",
   "killing one holder triggers repair"):
   - Put a key with `targetNearbyReplicaCount=3`; poll holder directories until 3 distinct
     durable holders exist (async fill).
   - `make docker-kill` a holder; poll until the engine restores 3 holders on the survivors
     with no manual action (M4/TS-035; `design/10_docker_dev.md` convergence acceptance).
3. **Chaos (`tests/chaos/spot_churn_test.go`, nightly, layer 4):** kill a random container
   every 10-60s for a sustained window while writing; then stop churn, quiesce, and assert
   `kv_under_replicated_keys_estimate` drains to zero and every key reaches target on distinct
   nodes — i.e. **repair keeps up with churn** (top risk; property 1/2 in
   `IMPLEMENTATION_STRATEGY.md` section 3).
4. **Bank invariant:** re-run the adapted `testing-waves` bank workload **with** holder kills
   during the run; at quiescence, balance is still conserved (durability survives churn because
   repair restored replicas).
