# Replication modes & node sync

WaveSpan replicates each key to a set of **holders**. By default that set is `origin + target-N`
nearby replicas (latency-graph placement). A namespace can override this with a **replication
factor**, including **"everywhere"** — every node holds every record — which in turn requires a
**bootstrap/backfill** so a joining node receives the existing records.

## Per-namespace replication factor

Configure in `namespaces` or via env:

```yaml
namespaces:
  - name: ref
    replicationFactor: all     # every node holds every record (reference / "everywhere" data)
  - name: hot
    replicationFactor: "5"     # override the target holder count for this namespace
  - name: default              # "" => the cluster's nearby target-N (origin+1 default)
```

```
WAVESPAN_REPLICATE_EVERYWHERE_NAMESPACES=ref,config
```

| factor | meaning |
|---|---|
| `""` (unset) | origin + `targetNearbyReplicas` nearby holders (the default) |
| `"N"` | override the target holder count for this namespace |
| `"all"` | **every alive node** holds every record |

### How "all" works on the write path

- **Fanout** (`Fanout.fillEverywhere`): after the origin+1 ack, the async fanout replicates the record
  to **every alive member**, not just nearby candidates.
- **Repair** (`RepairEngine`): the effective target for an everywhere namespace is the **full alive
  set**, and repair pushes to every missing alive member — so a node that was down catches up.
- **Cost:** because the origin+1 ack is unchanged and the fan-to-all happens in the background,
  client-observed write throughput drops only ~10% vs. the default (measured: 14.6k → 13.2k puts/s
  on a 3-node docker cluster); the node does ~N× the background replication work.

## Node sync / adding a node (bootstrap & backfill)

**Without "everywhere":** a joining node starts empty and acquires data passively — new writes for
which placement selects it, repair of under-replicated keys, and read-on-miss cache fills. Keys
already at target-N elsewhere never migrate to it. (This is unchanged.)

**With "everywhere":** because every node must hold every record, a joining node **streams the
existing records on join**:

1. `ReplicationService.Backfill(namespace, cursor, limit)` pages a holder's full records (built on
   `recordstore.ScanRecordsFrom`).
2. The `Bootstrapper` runs at startup: for each everywhere namespace it picks an alive peer and pages
   through `Backfill`, applying each record via `recordstore.Apply` (idempotent LWW — safe to re-run
   on a restart).
3. Ongoing fanout + intra-cluster anti-entropy keep the node current after the initial pass.

So a **fresh or wiped node rejoining** ends up holding the complete everywhere namespace, proven
locally (not via read-on-miss) with `ScanLocal`.

## Local cluster vs. global multi-cluster

"everywhere" is a **per-cluster** policy — it means *every node of the cluster that has the namespace
configured*. The two replication layers compose:

- **Local cluster:** the fanout/repair spread the record to every node of *this* cluster.
- **Global (active-active multi-cluster):** an orthogonal layer. A write also enters the per-peer
  out-log (`globalTap`) and is shipped to peer clusters; the peer's `Applier` applies it to its local
  record store under the conflict policy (HLC-LWW / keep-siblings).

Putting them together, for a namespace configured `replicationFactor: all` **in every cluster**:

1. Write in cluster A → origin+1 ack → fanout to **all nodes of A** → shipped to peer clusters.
2. Cluster B receives it via `PushGlobal`; the `Applier` applies it on the **receiving B node** and
   fires `onApply`, which **re-enters B's fanout** → the record spreads to **all nodes of B**.
3. A new node joining B bootstraps from a B peer (intra-cluster `Backfill`) — backfill is always
   *local*; cross-cluster propagation is the global layer's job, not the joining node's.

So the record ends up on **every node of every cluster** that configures the namespace as "all".

Key points / caveats:
- The "everywhere" set is **per-cluster config**. If cluster B does *not* mark the namespace "all",
  B applies the record on the receiving node and replicates it per B's own policy (e.g. target-N).
- The `Applier.SetOnApply` hook is what makes "everywhere" hold across the global boundary — without
  it an inbound cross-cluster write would sit on the single receiving node. (Locally-originated
  writes always fanned out; inbound ones did not until this hook.)
- Conflicts across clusters are still resolved by the namespace conflict policy (LWW or
  keep-siblings); "everywhere" changes *where* a record lives, not *how* concurrent versions merge.
- Global durability (`globalDurabilityRequired`) and the out-log/anti-entropy backpressure are
  unchanged and independent of the replication factor.

## Reads: MultiGet

Unrelated to replication but the companion read-throughput feature: `KvService.MultiGet` reads many
keys of a namespace in one round-trip, amortizing the per-request RPC/HTTP-2/serialization overhead
(`wavespan-bench multiget --batch N`).

## Testing

- **Unit:** `fanout`/`repair` everywhere targeting; `Bootstrapper` streams & applies; no-peer no-op.
- **Integration (docker):** `TestReplicateEverywhereAndBackfill` — writes land on all nodes; a wiped
  node3 rejoins and backfills (verified via `ScanLocal`).
- **Harness (Jepsen-style):** `TestPRGateEverywhereBackfill` — same, under the fault-injection harness.
- **Perf:** `wavespan-bench kv --namespace ref` vs `--namespace default`.
