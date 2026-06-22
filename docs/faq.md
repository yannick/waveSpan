# WaveSpan FAQ

Architecture questions and answers, with citations into `design/` and the source tree.

## Which consensus algorithm does WaveSpan use?

**None — by deliberate design.** WaveSpan is eventually consistent and runs no global
consensus protocol (no Raft/Paxos/ZAB).

- "WaveSpan is **eventually consistent by default**. It deliberately does not run a global
  consensus" (`docs/consistency-and-replication.md:3`); "No cross-cluster consensus is used
  in the hot path" (`design/06_global_active_active_replication.md:14`).
- Rationale (ADR 0001): "Linearizable writes would require stable quorum ownership and often
  wider coordination. Global linearizability would add WAN latency and conflict with the
  active-active requirement." (`design/adr/0001_eventual_consistency.md:11`).
- Conflict resolution instead of consensus: **HLC last-write-wins** (`hlc-last-write-wins`,
  default), ordered by HLC physical → HLC logical → writer cluster id → writer member id →
  writer (`docs/consistency-and-replication.md:24-28`). v1 ships two resolvers,
  `hlc-last-write-wins` and `keep-siblings`; CRDT resolvers are interface-only, deferred
  post-v1 (`design/06_global_active_active_replication.md:130-144`).
- Propagation is gossip + fanout (+ cache subscriptions), not a coordinated log
  (`design/01_architecture.md:54`, `docs/consistency-and-replication.md:59,93`).

## Don't we need cluster consensus to distribute keyspans for the graph store?

No. Keyspan distribution is split into two parts, neither needing agreement:

1. **Key → partition is a deterministic pure function.** Graph partitioning is a fixed
   FNV-1a hash mod a fixed count, computed identically on every node with no coordination:
   `NumPartitions = 256`, `Partition(graphID, nodeID) = hash % 256`
   (`internal/graph/partition.go:5-15`). Edges partition by start node so a node and its
   outgoing edges co-locate.
2. **Partition → holders is soft, gossiped state — not an authoritative assignment.** Each
   node keeps an eventually consistent **range directory** built from gossiped **holder
   summaries**; it "does not need perfect accuracy. If a selected holder misses, fallback to
   another holder or repair source" (`design/04_membership_latency_gossip.md:149-221`). The
   graph planner routes fragments by range-directory affinity and, when affinity is unknown
   or stale, **fans out** to candidate holders bounded by `maxRemoteFragments` (default 128)
   then anti-entropy (`design/07_graph_cypher.md:250-294`). Replica placement is a per-write
   **local** decision by the coordinator over the latency graph
   (`internal/placement/placement.go:53`). Membership is SWIM-style gossip, not Raft
   (`design/04_membership_latency_gossip.md:66-95`).

It works without consensus because the system never needs a single authoritative
"who owns partition N": stale summary → fresh alternate holder or anti-entropy; unrouted
fragment → bounded fan-out; replica deficit → repair. Convergence comes from HLC-LWW +
anti-entropy, not from agreeing the keyspan map up front.

**Caveat:** this gives eventual placement convergence, not linearizable single-owner
semantics. During churn two coordinators can briefly pick different replica sets for the
same key (fine under HLC-LWW + repair). Strict single-owner-per-range (a lease/primary)
*would* need a consensus group; the current design provides none.

## What happens to the K/V store under heavy spot-node churn?

Heavy spot fluctuation is the designed-for normal case
(`design/00_assumptions_and_product_contract.md:21`).

- **Durability invariant:** every acknowledged write lands on two distinct nodes before
  success — origin + 1 nearby durable replica (`design/00:7,123`,
  `design/03_kv_store.md:38`); fanout then continues to `targetNearbyReplicaCount`
  (`design/03_kv_store.md:55`). A single spot kill never loses acknowledged data.
- **On disappearance** (`design/13_failure_model.md:63`): gossip marks suspect/unreachable
  (phi-accrual, `design/04:82`) → placement stops selecting it
  (`internal/placement/placement.go:62`) → repair estimates under-replicated keys → new
  nearby replicas created from survivors → lost subscriptions expire → scan completeness
  degrades until repair catches up.
- **Repair priority** (`design/23_repair_engine.md:39`): single-holder keys first, then
  recent writes, active-subscriber, hot, range-coverage, cold. Bounded by worker pool (8),
  in-flight cap (64), and a `repairBytesPerSec` token bucket so foreground isn't starved
  (`design/23:20-32`).
- **Churn backpressure** (`design/23:53-73`): when suspect-member rate exceeds
  `churnSuspectRateThreshold` (0.15), the scheduler temporarily **raises target-N**
  (over-replicate) and **widens soft filters** (never hard ones: distinct-node, compliance
  geo), then decays back when churn subsides.
- **Graceful drain** when k8s gives notice (`design/04:236`), but "assumes no notice in the
  common case" (`design/04:232`).

**Degradations / tradeoffs:** write availability can drop under local-geo pressure — if
origin+1 can't be met within `writeTimeout` (2s) it fails with `InsufficientLocalReplicas`
or spills only if `allowSpilloverForDurability=true` (`design/00:90-101`); reads may briefly
miss on a stale directory then resolve via anti-entropy (`design/13:44`); scans degrade
until repair restores coverage (`design/13:71`); replaced nodes rejoin **empty** as new
members and refill via repair (`design/13:159`). **The one real data-loss risk:** losing
*both* holders of a key before target-N repair completes is explicitly not guaranteed
against in v1 (`design/13:20`) — churn backpressure exists to shrink that window.
