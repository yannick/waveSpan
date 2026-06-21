# 23. Repair engine

## Goal

The repair engine continuously restores the durable replica target (doc 05) for keys whose
holder count has fallen below `targetNearbyReplicaCount` — primarily because spot nodes
vanish (doc 00). This document specifies the repair scheduler concretely: how work is
prioritized, rate-limited, bounded, made churn-resilient, and made self-healing if the
repair worker itself fails.

The repair loop's inputs (holder summaries, dead/suspect members, mutation logs, key
sampling, TTL tombstones, global replication lag) are listed in doc 05. Here we specify the
scheduler that consumes them.

## Worker pool and rate limiting

Repair runs on a **bounded worker pool**, never an unbounded goroutine fan-out. Two limits
bound resource use:

```yaml
repair:
  maxRepairWorkers: 8           # bounded worker pool
  maxInFlightRepairs: 64        # cap on concurrent repair operations
  repairBytesPerSec: 50Mi       # token-bucket byte budget across all workers
```

- the **worker pool** bounds CPU/connection concurrency;
- `maxInFlightRepairs` caps the number of repair operations outstanding at once (a worker
  may have several in flight against different holders);
- a **token-bucket rate limiter** of `repairBytesPerSec` throttles total repair bytes so
  repair never starves foreground traffic. A repair copy acquires tokens proportional to
  the record bytes it moves and blocks when the bucket is empty.

## Priority queue

Repair work is ordered by a priority queue, highest priority first. The order reflects how
much each class reduces durability/availability risk:

```text
1. single-holder keys      // exactly one durable holder left: one more loss = data loss
2. recent writes           // freshly written, not yet fanned out to target N
3. active-subscriber keys   // keys with live cache subscribers depending on freshness
4. hot keys                // high read/write rate; under-replication amplifies impact
5. range coverage          // restore range-level coverage for cache-complete scans
6. cold keys               // everything else; background convergence
```

Single-holder keys are first because they are one failure away from loss. Recent writes are
next because they have the largest gap between current and target replica count. Cold keys
are repaired only when higher classes are drained.

## Churn backpressure

Under heavy spot churn, many keys go under-replicated at once and naive repair can chase a
moving target or overwhelm the cluster. When the **suspect-member rate** exceeds a
threshold, the scheduler adapts rather than thrashes:

```yaml
repair:
  churnSuspectRateThreshold: 0.15   # fraction of members suspect within a window
```

```text
if suspect_member_rate > churnSuspectRateThreshold:
    temporarily raise target N (over-replicate) so churn-induced losses stay above target;
    widen the candidate filter (relax soft latency/topology penalties, keep hard filters
        like distinct-node and compliance boundary) to find healthy holders faster;
    when churn subsides, decay target N and the filter back to configured values.
```

Raising target N during churn means a subsequent node loss is less likely to drop a key to
zero healthy holders. The candidate filter is widened only on **soft** constraints; hard
constraints (distinct node for durable replicas, compliance boundary) are never relaxed.

## TTL and expired keys

Repair **never touches expired keys** to recreate live data — re-replicating an
expired-but-not-yet-swept value would resurrect data that should be gone. The one exception
is convergence: if a tombstone is required so that all holders agree the key is deleted,
repair may propagate that **tombstone** (doc 03, doc 06). Repair copies tombstones, not
expired live values.

## Supervision and restart recovery

The repair worker is **supervised and restartable**. If a worker panics, stalls, or the
repair subsystem is restarted (process restart, lease loss), it must not silently stop
repairing and it must not lose its work — the queue is in-memory and would otherwise be
gone.

On (re)start, the scheduler **rebuilds its queue from durable, observable state**:

```text
on repair worker start/restart:
    1. scan local + gossiped holder summaries to estimate per-range holder counts;
    2. scan the mutation log for recent writes not yet confirmed at target N;
    3. re-derive single-holder / under-replicated keys from the above;
    4. re-enqueue by the priority order above;
    5. resume token-bucket-limited repair.
```

This means a crashed repair worker resumes from reconstructed state rather than a lost
in-memory queue, closing the "repair worker itself stalls/fails" gap. A supervisor restarts
the worker on failure; a stuck worker (no progress past a deadline) is killed and restarted,
which triggers the rebuild above.

## Alerts

```text
RepairQueueStuck   // queue depth not draining / no repair progress past a deadline
```

`RepairQueueStuck` fires when the priority queue stays non-empty with no completed repairs
over an interval — the signal that the worker has stalled and supervision/restart is needed.

## Metrics

```text
under_replicated_keys_estimate     // estimated keys below target N (also surfaced in doc 05)
repair_throughput_bytes_per_sec    // actual repair bytes moved
repair_lag_seconds                 // age of the oldest unrepaired under-replication
repair_queue_depth                 // pending items by priority class
repair_in_flight                   // concurrent repair operations vs maxInFlightRepairs
repair_target_n_raised             // 1 while churn backpressure has raised target N
repair_tombstones_propagated_total // tombstone-only repairs for convergence
```
