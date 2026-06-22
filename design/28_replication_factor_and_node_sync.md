# 28. Per-namespace replication factor & node sync

## Goal

By default a key is held by `origin + targetNearbyReplicas` nearby nodes, chosen by the latency
graph (doc 05). This document specifies the **per-namespace replication factor**, including the two
"replicate to every node" modes — `all` (the local cluster) and `global` (every cluster) — and the
**node-sync / bootstrap protocol** that a joining node uses to backfill those namespaces. It also
specifies how this composes with global active-active replication (doc 06).

The feature exposes two **orthogonal axes** that were previously conflated:

| axis | question | controlled by |
|---|---|---|
| **intra-cluster spread** | how many nodes of *this* cluster hold the key? | the replication factor |
| **global boundary** | does the write leave this cluster for peers? | the replication factor + global config (doc 06) |

## Replication factors

`namespaces[].replicationFactor` (or env) selects the mode:

| value | intra-cluster spread | crosses to peer clusters | joining node backfills |
|---|---|---|---|
| `""` (unset) | `origin + targetNearbyReplicas` nearby | follows the cluster's global config | no |
| `"N"` (integer) | override the target holder count to `N` | follows the cluster's global config | no |
| `"all"` | **every alive node of the current cluster** | **never** (local-only) | yes |
| `"global"` | **every alive node of every cluster** | **always** (shipped to all peers) | yes |

`all` and `global` are the same *within* a cluster — both spread to every node and both backfill a
joiner — and differ only at the global boundary: `all` suppresses cross-cluster shipping, `global`
forces it.

### Configuration

```yaml
namespaces:
  - name: ref
    replicationFactor: all       # every node of THIS cluster, never leaves it
  - name: feature_flags
    replicationFactor: global    # every node of EVERY cluster
  - name: hot
    replicationFactor: "5"       # override the target holder count
  - name: events                 # "" => default nearby target-N
```

Equivalent environment overrides:

```
WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES=ref          # => all
WAVESPAN_REPLICATE_GLOBAL_NAMESPACES=feature_flags    # => global
```

Code surface (`internal/config`): `NamespaceConfig.ReplicationFactor`,
`NamespaceConfig.ReplicateEverywhere()` (true for `all` and `global`), `NamespaceConfig.Scope()`
(`GlobalScopeDefault | GlobalScopeLocalOnly | GlobalScopeGlobal`), `Config.EverywhereNamespaces()`,
`Config.LocalOnlyNamespaces()`.

## Write path

A KV write to an `all`/`global` namespace follows the normal origin path, then spreads wider:

1. **Origin + ack.** The coordinator (`internal/kv/coordinator.go`) does the local `recordstore.Apply`
   and gets the origin+1 durable ack. This is unchanged — **the client-observed write latency does not
   grow with the replication factor**, because the fan-to-all happens after the ack.
2. **Fanout (`internal/replication/local/fanout.go`).** `Fill` checks `isEverywhere(namespace)`. If
   set, `fillEverywhere` replicates the record (`StoreReplica`) to **every alive member except self**
   that does not already hold it, recording each in the holder directory. Any member it could not
   reach is handed to repair.
3. **Repair (`internal/replication/local/repair.go`).** For an everywhere namespace the
   `effectiveTargetFor(namespace)` is the **full alive-member count**, and `repairCandidates` returns
   **every alive member** (not the latency-selected nearby subset). So a node that was unreachable
   during the original write is filled in on a later repair pass, and a node that comes back from a
   partition is brought up to date.

`N`-factor and default namespaces are untouched by the everywhere path (`isEverywhere` is false), so
their behaviour is exactly as before.

### Cost

Because the origin+1 ack is unchanged and the fan-to-all is asynchronous background work,
client-observed write throughput drops only **~10 %** for `all`/`global` vs. the default (measured
14.6k → 13.2k puts/s on a 3-node docker cluster). The node does ~N× the *background* replication
work, so an everywhere namespace should hold **small, slowly-changing reference data** (flags, config,
routing tables), not a high-churn primary dataset.

## Node sync — bootstrap & backfill

Historically a joining node started empty and acquired data only passively: new writes for which
placement picked it, repair of under-replicated keys, and read-on-miss cache fills. Keys already at
target elsewhere never migrated to it. That is acceptable for the default factor, but an
`all`/`global` namespace **requires** a joiner to hold the full set — so it must stream the existing
records on join.

### The Backfill RPC

`ReplicationService.Backfill` (`proto/wavespan/v1/replication.proto`) pages a holder's full records
for a namespace:

```proto
message BackfillRequest  { string namespace = 1; bytes cursor = 2; uint32 limit = 3; }
message BackfillResponse { repeated StoredRecord records = 1; bytes next_cursor = 2; }
```

The server (`ReplicaServer.Backfill`) is built on `recordstore.ScanRecordsFrom(namespace, cursor,
limit)`: it returns up to `limit` (server-capped at 1024) full winning records at/after the cursor,
plus the cursor to resume strictly after the last one (empty `next_cursor` = end of namespace). It
streams **full `StoredRecord`s** (version, tombstone, expiry, siblings) — not just values — so the
joiner can apply them correctly.

### The Bootstrapper

`internal/replication/local/bootstrap.go`. On startup, for each `all`/`global` namespace, the
`Bootstrapper`:

1. waits (poll loop) until at least one **alive peer** exists;
2. pages through `Backfill` from the first reachable peer (`bootstrapPage = 512` records/page),
   following `next_cursor` until the namespace is exhausted;
3. applies each record via `recordstore.Apply` (**idempotent LWW** — re-running on a restart, or
   racing with a concurrent live write, is harmless: the higher HLC version wins);
4. exits after one full pass — ongoing fanout + intra-cluster anti-entropy (doc 13) keep it current.

Because apply is idempotent, a *restarted* node (volume intact) re-syncing is also safe — it just
re-confirms what it already has. A *wiped or fresh* node ends up holding the complete namespace,
provable locally (not via read-on-miss) with `ScanLocal`.

A node with no live peer (e.g. the first node of a brand-new cluster) applies nothing and does not
block; it becomes the source once peers join.

## Global multi-cluster composition

`all`/`global` is a **per-cluster** policy ("every node of a cluster that configures it"); it composes
with the separate global active-active layer (doc 06):

- **Outbound (`globalTap`).** The coordinator ships writes to the per-peer out-log. The tap now
  consults `Config.LocalOnlyNamespaces()`: an **`all`** namespace is **skipped** (never crosses the
  boundary), while `global` and default namespaces ship as before. This is the single guard that
  separates "current cluster" from "all clusters".
- **Inbound (`Applier.onApply`).** A cross-cluster write arrives via `PushGlobal` and the `Applier`
  applies it on the **one** receiving node. It then fires `SetOnApply`, which re-enters the **local
  fanout** for everywhere namespaces — so a `global` record spreads to **every node of the receiving
  cluster**, exactly like a locally-originated write. Without this hook a `global` write would sit on
  the single receiving node and silently violate "everywhere".

End to end, for a namespace marked `global` in clusters A and B:

```
write @ A.node1 ── origin+1 ack
   ├─ fanout → A.node2 … A.nodeN      (everywhere in A)
   └─ out-log → B
                 └─ B.nodeX applies, onApply → fanout → B.node1 … B.nodeM   (everywhere in B)
```

A new node joining B backfills from a **B** peer (backfill is always intra-cluster; the global layer
carries records *between* clusters, the bootstrapper spreads them *within* one).

Cross-cluster conflicts are still resolved by the namespace's conflict policy (HLC-LWW or
keep-siblings, doc 06). The replication factor changes **where** a record lives, never **how**
concurrent versions merge. `globalDurabilityRequired` and out-log backpressure (doc 06) are
independent of the factor.

## Correctness properties

- **Eventual full replication.** After writes stop and faults heal, every alive node of every cluster
  that configures the namespace `all`/`global` holds the winning record (fanout + repair + bootstrap +
  anti-entropy converge).
- **No silent boundary crossing.** An `all` write never appears in a peer cluster.
- **Idempotent backfill.** Bootstrap can run repeatedly (restart, retry, overlap with live writes)
  without corrupting state — LWW apply is the merge.
- **Honest holdings.** `ScanLocal` reflects what a node *physically* holds (no fetch), so backfill is
  verifiable independently of the read-on-miss path.

## Failure modes

| event | behaviour |
|---|---|
| a node is down during an everywhere write | recorded as a gap → repair fills it when the node returns |
| a node is wiped and rejoins | bootstrap streams the namespace back from a peer |
| no peer is reachable at boot | bootstrap no-ops and retries; the node serves what it has |
| a peer cluster is unreachable (`global`) | the out-log buffers per doc 06; anti-entropy reconciles on reconnect |
| concurrent same-key write during backfill | LWW: the higher HLC version wins, regardless of order |

## Observability

Holder counts and under-replication are already surfaced (doc 14); an everywhere namespace simply
raises the effective target to the alive-member count, so existing repair-queue and
under-replication signals apply. `ScanLocal` row counts give a direct "does node X hold all of
namespace Y" probe (used by the tests).

## Testing

- **Unit** (`internal/replication/local`, `internal/config`): fanout/repair everywhere targeting;
  `Bootstrapper` streams + applies; no-peer no-op; `Applier.onApply` fires once per new apply;
  factor → scope mapping (`all`/`global`/`N`/default).
- **Integration** (`tests/integration/everywhere_test.go`, `TestReplicateEverywhereAndBackfill`):
  writes land on all 3 nodes; node3 is wiped (container + volume) and rejoins; backfill restores all
  records, verified via `ScanLocal`.
- **Harness** (`tests/harness`, `TestPRGateEverywhereBackfill`): the same under the fault-injection
  harness (doc 25).
- **Perf** (`wavespan-bench kv --namespace ref` vs `--namespace default`): the ~10 % everywhere-write
  cost above.

## Limitations & future work

- **Cost scales with cluster size.** Everywhere namespaces are O(nodes) write amplification; intended
  for small reference data. A future "N copies, hash-placed" mode would sit between `N`-nearby and
  `all`.
- **Backfill is full-scan per join.** Fine for small namespaces; a large everywhere namespace would
  want range-hash diffing (like the global anti-entropy, doc 06) to backfill only the delta.
- **No readiness gate.** A joining node serves an everywhere namespace before backfill completes
  (reads may miss until filled); acceptable under the eventual model, but a per-namespace "ready after
  backfill" gate could be added.

See `docs/replication_modes.md` for the operator-facing quick reference.
