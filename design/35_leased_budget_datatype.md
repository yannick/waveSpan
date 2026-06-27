# LeasedBudget — Hierarchical Escrow Counter for waveSpan

**Status:** Proposed (design/35). Supersedes the provisional "pacing #2 = CP `HIncrBy`" answer in `design/34` Phase 3.
**Owners:** storage-engine team. **Consumers:** vires ad-serving (frequency capping, pacing/budget).
**Depends on:** `internal/collections` (consensus tier), `design/30_replicated_collections.md`, `design/22_versioning_and_hlc.md`, `design/25_correctness_harness.md`.

> ⚠️ **SAFETY STATUS — the STRICT "never overspend, even transiently" guarantee does NOT hold as written.**
> An independent adversarial re-verification (2026-06-25) found 11 code-grounded holes (see **§16**), most
> critical, all in the **time-based reclaim / recall / hierarchy / cross-region machinery** (the guard-band
> §6.5 assumes a *monotonic* grantor clock the engine does not provide; in-flight grant time is charged to
> nobody; nested TTLs don't telescope; recall has double-credit bugs; Split/leader-change adds a third clock).
> §16 lists 10 prioritized fixes. **The single-cluster Stage-1 core (define/grant/report/return, no time, no
> recall, no hierarchy, single shard, explicit return) is unaffected and its conservation math is sound** —
> the holes live entirely in Stage 2–4 machinery and must be closed in the doc before those stages are built.

## 1. Summary

LeasedBudget is a new first-class distributed datatype: a cluster- and cross-cluster-capable **escrow counter / budget primitive** that replaces naive distributed increments for ad-serving (frequency capping, pacing, money budgets). Instead of merging counts (which last-write-wins silently corrupts), it **partitions a fixed quantity into disjoint leases that are never merged**. A holder spends locally against its lease with zero coordination; consensus is touched once per *lease-worth* of spend, not per increment — breaking the hot-counter write ceiling. The quantity is escrowed through a 3-level hierarchy (global authority → per-cluster broker Raft group → in-memory adserver node lease), steered centrally by cap `N` + pacing rate `R` with epoch generations and short lease TTLs, and recalled in emergencies by a separate invalidate-now/reclaim-after-settle protocol. It supports two configurable modes: **STRICT** (intended: `sum(spent) ≤ cap` always, even transiently — for money) and **RELAXED** (bounded transient overshoot that converges — for frequency caps). A first adversarial pass folded in five fixes (expiry/clock races, dedup-ring eviction, recall reclamation, cap-cuts, RELAXED oscillation); a second **independent** pass then showed the STRICT guarantee still does **not** hold as written and listed 11 remaining holes plus 10 fixes — see **§16**. The single-cluster Stage-1 core is unaffected.

---

## 2. Motivation

### 2.1 Why escrow, not a counter

The natural way to track "spend so far" is a distributed counter incremented per event. waveSpan already has `HIncrBy` (`internal/collections/hincr.go`), which does a read-add-write of a decimal-string field *inside one Raft entry* (`applyHIncrInt`, `hincr.go:38`) so concurrent increments are exact within a single shard. That is correct but has two fatal problems for ad-serving budget at scale:

1. **The hot-counter write ceiling.** A line-item's daily-spend field is a single key on a single shard leader. Every impression across every adserver in every region is a write to that one Raft group. `design/30` §19 documents this directly: *"reads scale with replicas, writes do not"* and the hot-collection write ceiling is *"acceptable for centrally-written data"* — impressions are anything but centrally written. At ad QPS this saturates one Raft leader.

2. **Cross-cluster merge loses increments (LWW).** Today the only cross-cluster path is `internal/replication/global/applier.go`, which applies inbound KV records through the conflict resolver with four outcomes — `KindWinner` / `KindTombstone` / `KindSiblings` / `KindReject` — defaulting to **HLC last-write-wins** (`design/06`: *"active-active… converge asynchronously. No cross-cluster consensus is used in the hot path"*). Collections are **not wired into the global layer at all** (`design/30` §19 lists cross-cluster collections as FUTURE: "leader-per-region + async mirror"). If we mirrored a per-region counter through that LWW path, the region that wrote last *wins* and the other region's increments are **silently discarded**. For a money counter that is a direct double-counting / loss of real revenue accounting. HLC-LWW's own documentation says it is *"unsafe for counters, sets, append logs, and business-critical updates."*

### 2.2 The escrow insight

Escrow sidesteps both problems by never merging counts. A fixed quantity `N` (the cap) is **partitioned** into disjoint leases. Granting a lease atomically moves quantity between disjoint buckets — `available -= amount; leasedOut += amount` — in one Raft entry (the exact same single-entry read-modify-write discipline `applyHIncrInt` uses). A holder then spends *its own* lease in memory with **zero coordination**. Consensus is touched once per lease-worth of spend (on refill), not per impression: a 600-unit lease that covers ~60s of one cluster's spend turns millions of impressions into one Raft entry every 60s.

Because leases are disjoint and never merged, there is **nothing for LWW to clobber** — cross-cluster budget moves via an explicit *request/grant RPC* (a remote grant against the authority's Raft group), not a mirrored record. And because the only way quantity leaves a node is a committed grant, a partition or lost message can only **strand** quantity (→ underspend), never duplicate it (→ overspend). That asymmetry is the entire safety story.

---

## 3. Concepts & Data Model

| Concept | Definition |
|---|---|
| **Budget** | A named escrow pool, addressed `(namespace string, budget []byte)` exactly like a collection so range-sharding and leader-routing (`WithForwarder`, `collections.go:41`) work unchanged. One budget lives on exactly one shard leader. |
| **Pool state** | Per level: `cap N`, `available`, `leasedOut`, `spent`, `pendingReclaim`, `epoch`, `mode`, pacing `(rate R, burst, tokens)`. |
| **Lease** | The disjoint partition unit: `(id, holder, amount, spent, epoch, expires_at)`. Disjoint, **never merged**. A holder spends locally against `amount − spent`. |
| **Epoch** | Steering generation (uint64). Bumped on every steer/recall. A lease/draw with `epoch < current` is rejected. Epoch **invalidates future draws**; it does **not by itself reclaim quantity** (Fix 1/Recall, §8). |
| **Mode** | `STRICT` (`sum(spent) ≤ cap` always, even transiently) or `RELAXED` (bounded overshoot, converges). |
| **Meter** | Two-part: a hard quantity cap `N` (escrow ceiling) **and** a token-bucket pacing rate `R` (units/sec) with `burst` ceiling. Grants are gated by *both* `available` and accrued pace tokens, so spend is smoothed over time, not burned instantly. Central steers both `N` and `R`. |

### 3.1 The conservation buckets

Every unit of `cap` is in exactly one of **five** disjoint buckets at every level:

```
cap == available + leasedOut + pendingReclaim + spent      (exact equality, STRICT)
```

- `available` — un-leased, drawable now (token-bucket head, ≤ `min(burst, cap−spent)`).
- `leasedOut` — sum of `amount` of currently-outstanding, *valid* (current-epoch) leases.
- `pendingReclaim` — quantity of **recalled** leases that has been invalidated for future draws but not yet provably freed (NEW — Fix 1, §8). This is what makes recall safe and keeps the conservation equality exact during a recall.
- `spent` — reconciled consumed total (accounting).

`pendingReclaim` is the load-bearing addition from adversarial verification: it gives in-flight, recalled-but-maybe-still-spending quantity a *home* in the equation, so the equality never breaks and recall never double-credits `available`.

---

## 4. Safety & Liveness Model

### 4.1 Invariants

**INV-LOCAL (per level, exact, apply-time post-condition of every escrow op):**
```
cap == available + leasedOut + pendingReclaim + spent
```

**INV-GLOBAL (STRICT, at all times across all clusters):**
```
Spent ≤ N(e)          // the money invariant — ALWAYS, even transiently
```
where `Spent` = total locally-committed spend across all holders at the live epoch. Since every conservation term is non-negative and they sum to `cap`, `Spent` alone can never exceed it. **A partition or lost message can only shrink the reachable `available`/lease quantity a holder can use; it can never inflate `Spent`.** Worst case = UNDERSPEND.

### 4.2 Inductive argument over the lease lifecycle

Base case: fresh budget has `available = cap`, all other buckets `0`. ✓

- **GRANT(amount).** One Raft entry, predicate evaluated *inside apply* (deterministic, no wall-clock/RNG): `if amount ≤ paceTokens && amount ≤ available && epoch == current { available −= amount; leasedOut += amount }`. `available + leasedOut` unchanged ⇒ INV-LOCAL holds. The `amount ≤ paceTokens` gate makes a grant a *paced* draw, not an instant burn.
- **SPEND(δ).** Local, in-memory, **touches no Raft**: `lease.amount −= δ; lease.spent += δ`. Moves quantity from in-flight-lease to `Spent` (both outside `available/leasedOut`); INV-LOCAL at the parent untouched.
- **RETURN/EXPIRE(lease).** One entry: `leasedOut −= lease.amount; available += (lease.amount − spent); spent += spent`. Unchanged sum ⇒ INV-LOCAL holds.
- **RECALL.** Moves lease quantity `leasedOut → pendingReclaim` and bumps epoch. **Does not credit `available`.** INV-LOCAL holds (quantity just changed bucket). Quantity leaves `pendingReclaim` only on settle (§8). Safety-neutral: cannot create quantity.

**Compositional (telescoping) step.** A broker is a child of root and a parent of nodes. Its grant-to-node draws from `available(broker)`, which *is* the lease it holds from root, gated by the same `amount ≤ available` predicate — it can never grant more than it holds. Substituting `INV-LOCAL(node)` into `INV-LOCAL(broker)` into `INV-LOCAL(root)` telescopes: every unit is in exactly one place at exactly one level. Therefore `Spent ≤ N` holds globally **regardless of how the tree is partitioned**. This holds *only because leases are disjoint and never merged* — the verification confirmed the proof is valid precisely when disjointness is preserved, and the fixes in §6/§8 are what preserve it under the expiry, dedup, and recall races.

### 4.3 Failure model (per tier, per mode)

The unifying rule: **a lost grant or report can never raise `Spent`; it can only strand quantity (recovered on expiry/settle) or delay convergence.**

| Failure | STRICT | RELAXED |
|---|---|---|
| **Leaf node crash** | In-memory lease lost; quantity stranded in `leasedOut` until expiry, then reclaimed on the holder-stop/guard-band settle (§6, Fix 1). Unreported spend is lost from accounting (under-counts — safe). UNDERSPEND. | Same, plus carried overdraft is gone; parent reconciles from the last cumulative report. Bounded by `D`. |
| **Broker leader change** | Dragonboat re-elects; uncommitted grants retried via `WithForwarder`; committed grants survive (replicated log). Dedup cache is in `CFReplData` (Raft-replicated, `dedup.go:62`), so it survives failover and a retry is exactly-once. No invariant impact. | Identical. |
| **Root unavailable** | Brokers can't refill; each spends down its outstanding lease and **stalls** → cluster-wide UNDERSPEND. Auto re-grant on recovery. | Brokers/nodes ride bounded overdraft `D`, then stall. Overshoot ≤ `Σ D`, reconciled on recovery. |
| **Partition leaf↔broker** | Leaf stalls when lease drains. Broker reclaims only after holder-stop guard (§6) → no double-grant. UNDERSPEND only. | Leaf rides overdraft, keeps serving; reconciles on heal. Overshoot ≤ `D` per leaf. |
| **Partition broker↔root** | Broker is an island: spends its lease, stalls. No overspend. | Bounded overdraft if enabled. |
| **Duplicate grant** | Idempotent: settled-lease tombstone + dedup ring (§6, Fix 2) returns the cached grant, never re-debits `available`. | Same. |
| **Late grant retry (past TTL/ring eviction)** | Settled-lease tombstone (retained ≥ replay window) catches it → "already settled", never a fresh grant. Closes the H2/H1 double-grant hole. | Same. |
| **Duplicate/lost report** | Reports are **cumulative-per-lease**, folded with `max` (§9, Fix 2) → duplicate = no-op, loss self-heals on next report. No drift. | Same — robustness to loss is why overdraft convergence works. |
| **Recall vs in-flight spend** | Recall = invalidate-now (epoch) + reclaim-after-settle (§8, Fix 1). STRICT does **not** re-grant `pendingReclaim` until settled → underspend stall, never overspend. | RELAXED may re-grant `pendingReclaim` optimistically and reconcile via the debt path. |
| **Cap decrease** | Mandatory RECALL of old-epoch leases + block new grants until `leasedOut ≤ newCap − spent` (§8, Fix 3). Hard, not soft. Underspend stall accepted. | Soft drain allowed. |
| **Clock skew / slow holder** | Holder hard-stops at `deadline − selfGuard`; grantor reclaims only at `deadline + 2·skew + maxPauseBudget` (§6, Fix 1+4). Windows provably non-overlapping. Pause beyond budget ⇒ holder self-fences. No overspend. | Slow-clock residual folds into `D`, reconciled. |

---

## 5. The 3-Level Hierarchy & Cross-Cluster Grant Protocol

### 5.1 The three levels

| Level | Who | Consensus | Talks to parent |
|---|---|---|---|
| **L0 — Root authority** | One Raft group in the home region (the line-item master budget) | LeasedBudget datatype on the dragonboat tier | n/a (root) |
| **L1 — Cluster broker** | One Raft group **per cluster** (3–5 voters) | Same datatype, local shard | Cross-region grant RPC to L0, ~once per refill (seconds–tens of seconds) |
| **L2 — Node lease** | Each stateless adserver node | None (in-memory token bucket) | In-cluster sub-lease RPC to its L1 broker, ~once per refill |

L0 and L1 run the *same* state machine; only the grantee differs (L0 grants block-leases to brokers; L1 sub-leases chunks to nodes). L2 is not a Raft member — it holds one in-memory `Lease` and spends with zero coordination.

### 5.2 Why cross-cluster is request/grant, NOT the LWW mirror

Mirroring `available`/`leasedOut` through `global/applier.go`'s HLC-LWW path would let the last-writing region's record win and erase the other region's grant/spend deltas → double-spend of real money. So cross-cluster budget movement is a **new synchronous-on-demand RPC** (`BudgetService.BudgetDraw` / `BudgetReport` / `BudgetReturn`, §10) — conceptually a remote grant against the L0 Raft group, routed to its leader by the *same* `WithForwarder` machinery as a collection write (`collections.go:41`), committed as one Raft entry via `proposeCmd` (`collections.go:48`). The home-region L0 group is the **single writer** of the master budget; other-region brokers are **clients** of it, not replicas. No change to the global applier.

### 5.3 Block-lease sizing (refill amortization)

A broker sizes each block so it lasts ~one refill interval under current demand, with hysteresis prefetch:
```
demand_rate      = EWMA of this broker's sub-lease grant rate to its nodes (units/sec)
target_block     = clamp(demand_rate * T_refill_target, min_block, max_block)
refill when      available_L1 < refill_watermark * last_block       // async prefetch
requested_amount = target_block - available_L1                      // top up, don't double-buy
```
Cross-region RPC rate ≈ `peak_spend_rate / target_block`. Example: €10/s in one EU cluster, `target_block = 60s` → one ~€600 grant per cluster per ~60s. L0 sees `N_clusters` grants/min, not millions of impressions/sec. Near STRICT exhaustion the block size is deliberately *shrunk* toward `min_block` so trapped underspend at leaves stays bounded by `Σ min_block`.

### 5.4 Partition behavior (the no-overspend proof under partition)

A partition cannot create a grant, and only a committed grant moves quantity, so `Σ spent ≤ N` is preserved; the only casualty is throughput. Per level: L2↔L1 split → node spends its in-memory lease to its monotonic deadline, then stalls; L1↔L0 split → broker sub-leases from its already-held block then stalls; L0 quorum loss → no new block-leases, outstanding leases keep working (already satisfy `Σ amount ≤ N`).

---

## 6. Server-Side Raft State Machine & Command Kinds

LeasedBudget is implemented as a **new datatype handled by the existing `shardSM`** in `internal/collections/`, registered as `collType` value `4` (`typeBudget`) — exactly how Hash/ZSet were bolted on (`statemachine.go:36-39`, `command.go`). It is **not** a separate dragonboat state machine. This inherits, for free: range-sharded `(ns,coll)` placement and leader routing (`WithForwarder`); the one-entry read-modify-write primitive (`applyHIncrInt`, `hincr.go:38`); idempotency (`dedup.go`); type-checking (`ensureType`, `statemachine.go:156`); the per-entry `recover()`-guarded rollback (`statemachine.go:203`); and snapshot/restore over the shard prefix (`base_sm.go`).

### 6.1 Key layout (sub-scopes under `collScope`, continuing `statemachine.go:36-39`)

```go
const (
    typeBudget collType = 4

    scopeBudCfg   byte = 0x05 // config: mode, epoch, cap, rate, burst
    scopeBudState byte = 0x06 // accounting: available, leasedOut, pendingReclaim, spent, lastRefillMs, tokens
    scopeBudLease byte = 0x07 // <leaseID> -> leaseRec (outstanding-lease table)
    scopeBudExp   byte = 0x08 // be(expiresMs)|<leaseID> -> empty (expiry-ordered sweep index)
    scopeBudTomb  byte = 0x09 // <leaseID> -> settled tombstone {status,spent,epoch,result} (Fix 2)
)
```

**Quantities are stored as 8-byte BE int64** (micro-units: micro-currency or milli-impressions), not decimal strings. Rationale: budgets are compared/saturated arithmetically on every grant; BE int64 keeps compares cheap and replica-deterministic. (HIncr uses decimal strings only so `HGet` returns them verbatim — irrelevant for an internal pool counter.) Money exactness is preserved by integer micro-units. INV-LOCAL is now a 5-term equality including `pendingReclaim`.

**leaseRec** (`scopeBudLease|<leaseID>`): `holder(chunk) | amount(8 BE) | spent(8 BE) | epoch(8 BE) | grantedMs(8 BE) | expiresMs(8 BE) | recalled(1)`.
**Settled tombstone** (`scopeBudTomb|<leaseID>`, Fix 2): `status(1) | finalSpent(8 BE) | epoch(8 BE) | resultBytes`, swept by the existing TTL machinery (`ttl.go`) at `expiresMs + maxDedupRetryWindow` (e.g. +30s, configurable, ≥ any transport replay window).

### 6.2 Op kinds (continuing the `opKind` enum past `opBatch=15`, `command.go`)

```go
const (
    opBudInit     opKind = 16 // create/configure (cap,R,burst,mode); epoch=1
    opBudSteer    opKind = 17 // re-steer cap/R/burst/mode; bumps epoch; cap-cut triggers recall (§8)
    opLeaseGrant  opKind = 18 // atomic create-if-absent: check pace+available+epoch, decrement, emit lease
    opLeaseReport opKind = 19 // cumulative-per-lease spent fold (max); may settle (final)
    opLeaseReturn opKind = 20 // release unspent; book spent; write tombstone
    opLeaseRecall opKind = 21 // invalidate-now (epoch bump, leasedOut->pendingReclaim); NO immediate credit
    opLeaseExpire opKind = 22 // leader-swept: settle an expired lease after holder-stop guard
    opLeaseSettle opKind = 23 // free pendingReclaim once holder-stop/guard or authoritative return proven
)
```

`typeForOp`: all return `typeBudget` except `opBudInit` (creates the type, like `opHSet`). `mutates()`: all true (respect subrange freeze). `coalescable()` (`manager.go:221`): add `opLeaseGrant`, `opLeaseReport`, `opLeaseExpire` (high-rate, batchable); keep `opBudInit/opBudSteer/opLeaseRecall/opLeaseReturn/opLeaseSettle` **un-coalescable** (control-plane, must stay atomic/ordered).

### 6.3 LEASE-GRANT as one Raft entry — with settled-lease idempotency (Fix 2)

Slotted into `applyOne` (`statemachine.go:266`) before the generic apply, mirroring `applyHIncrInt`'s shape. The grant is **idempotent by construction**: `leaseID = hash(holder_id, budget, client_draw_nonce)`, so a retried Draw is structurally the same key. Idempotency is checked against the **lease table and tombstone FIRST** (durable, survives ring eviction), then the dedup ring as a fast path.

```go
// in updateCtx; leaseID is c.Idem = hash(holder, budget, draw_nonce)
func (u *updateCtx) applyLeaseGrant(c command, holder []byte, amount, grantedMs, expiresMs int64) (granted int64, data []byte, err error) {
    // 0a. Settled tombstone? Already returned/expired -> "already settled", NEVER re-grant (closes H1/H2).
    if tb, found, _ := u.budTomb(c.NS, c.Coll, c.Idem); found {
        return 0, encodeAlreadySettled(tb), nil
    }
    // 0b. Live lease row for this leaseID? Retried draw before settle -> return the original lease (extends
    //     the idempotency window beyond the 4096 dedup ring AND beyond ring eviction time).
    if lr, found, _ := u.budLease(c.NS, c.Coll, c.Idem); found {
        return lr.Amount, encodeLeaseResult(lr.Amount, lr.Epoch, lr.ExpiresMs), nil
    }
    // 0c. Dedup ring fast path keyed on (draw_nonce, epoch) so a post-recall retry is a MISS (Fix 3).
    cfg, _ := u.budCfg(c.NS, c.Coll)
    if cached, cdata, found, _ := u.s.dedupGet(epochScopedKey(c.Idem, cfg.epoch)); found {
        return int64(cached), cdata, nil
    }

    st, _ := u.budState(c.NS, c.Coll)
    if cfg.frozen {
        return 0, budFrozen, nil // granted=false sentinel
    }

    // 1. token-bucket replenish up to min(burst, cap-spent) using LEADER-STAMPED grantedMs
    u.refill(&st, cfg, grantedMs)

    // 2. decide grant amount (paced by tokens AND bounded by available)
    grant := min64(amount, st.tokensAvail(), st.available)
    if grant <= 0 && cfg.mode == modeSTRICT {
        return 0, budNoCapacity, nil // zero-grant; caller backs off / retry_after
    }
    if cfg.mode == modeRELAXED && grant < amount {
        grant = amount // RELAXED may overdraft; available can go negative (debt path, §9)
    }

    // 3. atomic disjoint move
    st.available     -= grant
    st.leasedOut     += grant
    st.tokens        -= float64(grant)
    u.setBudState(c.NS, c.Coll, st)

    // 4. emit lease + expiry index (one row, current epoch)
    lr := leaseRec{Holder: holder, Amount: grant, Spent: 0, Epoch: cfg.epoch, GrantedMs: grantedMs, ExpiresMs: expiresMs}
    u.setLease(c.NS, c.Coll, c.Idem, lr)
    u.addLeaseExp(c.NS, c.Coll, expiresMs, c.Idem)

    data = encodeLeaseResult(grant, cfg.epoch, expiresMs)
    u.dedupRecord(epochScopedKey(c.Idem, cfg.epoch), uint64(grant), data) // ring fast path, Fix 3
    return grant, data, nil
}
```

All steps land in one committed entry (one `u.ops` flush, `statemachine.go`). Two concurrent grants are serialized by Raft into two entries; the second sees the first's decrement via the overlay (`u.budState` checks the in-batch overlay first, like `fieldVal` in `hincr.go`), so STRICT can never over-grant.

### 6.4 Token-bucket math against Raft-applied logical time (NO `time.Now` in apply)

The SM is deterministic and must never read wall-clock. The **leader stamps** `grantedMs` (for grants) and `sweepNowMs` (for the expiry sweep) into the entry *before* propose — the exact contract `SAddTTL` relies on (`collections.go:159`: `expiry := time.Now().UnixMilli() + ttlMs`, stamped pre-propose) and that `opExpire` relies on (`statemachine.go:368` compares the stamped `it.ExpiryMs`, never apply-time wall-clock). The SM treats the stamp as monotone logical time:

```
elapsedMs    = max(0, nowMs - st.lastRefillMs)        // clamp >=0: regressed stamp accrues 0, never subtracts
accrued      = floor(cfg.rate * elapsedMs / 1000.0)   // integer-exact, replica-deterministic
capRemain    = cfg.cap - st.spent
ceil         = min(cfg.burst, capRemain)
st.available = min(ceil, st.available + accrued)
st.lastRefillMs = max(st.lastRefillMs, nowMs)         // monotone forward only (HLC physical can briefly regress)
```

`rate == 0` ⇒ no pacing: `available = min(burst, capRemain)` filled instantly on init/steer. `floor()` drops the sub-token remainder per entry (conservative under-grant, never over). `opBudSteer` resets `lastRefillMs = sweepNowMs` so a new rate accrues from the steer point.

### 6.5 Reclamation = invalidate-now + reclaim-after-settle (Fix 1 — the load-bearing safety fix)

The verification found that crediting a recalled/expired lease's quantity back to `available` *while the holder may still be spending it* causes STRICT overspend (the same quantity lives in two live leases). The canonical model (reconciling the two contradictory facets to the **monotonic relative-TTL** model):

- **Holder side (single clock).** The grant carries `ttl_ms` (a *duration*) and `self_guard_ms`. The holder stamps its *own monotonic* clock at receipt: `deadline_local = monotonicReceipt + ttl_ms − self_guard`, with `self_guard ≥ maxClockSkewMs` (500ms default, `design/22:87`). It hard-stops spending at `deadline_local`. Only one clock is used, so cross-domain skew is irrelevant to the holder's stop decision.
- **Grantor side (reclaim late).** The grantor does **not** reclaim on a bare timer. `opLeaseExpire`/`opLeaseSettle` free quantity only when:
  ```
  grantorMonotonic > grantedAtGrantorClock + ttl_ms + 2*maxClockSkewMs + maxPauseBudget
  ```
  OR an explicit authoritative `opLeaseReturn`/final report has landed. `maxPauseBudget` bounds the VM-pause/GC window (configurable, e.g. a few seconds). A holder paused longer than `maxPauseBudget` **self-fences**: on resume, before any spend, if its observed monotonic gap exceeds `maxPauseBudget`, it treats its lease as presumed-dead, drops it, and re-draws under the current epoch.
- **Epoch backstop.** Every reclaim of a still-outstanding lease bumps the epoch. A mis-clocked late spend/report against the reclaimed lease is rejected as stale-epoch at the broker on its next Report/Return — the holder learns it must stop.

The asymmetry (holder stops early, grantor reclaims late, gap = `2·skew + maxPauseBudget`) makes the two windows **provably non-overlapping** for skew+pause within budget, and self-fencing handles pauses beyond budget. This is what preserves lease disjointness — the precondition the inductive proof depends on.

### 6.6 RECALL apply — `opLeaseRecall` (Fix 1 for the recall path, §8)

```go
// invalidate-now, reclaim-after-settle: NEVER credits available directly.
func (u *updateCtx) applyLeaseRecall(c command, epochFloor uint64, target []byte) {
    cfg, _ := u.budCfg(c.NS, c.Coll)
    cfg.epoch++                                  // bump => future draws under old epoch rejected
    u.setBudCfg(c.NS, c.Coll, cfg)
    for lr, id := range u.scanLeases(c.NS, c.Coll, target) {
        if lr.Epoch >= epochFloor { continue }
        st, _ := u.budState(c.NS, c.Coll)
        st.leasedOut      -= lr.Amount
        st.pendingReclaim += lr.Amount           // home for in-flight quantity; equality stays exact
        u.setBudState(c.NS, c.Coll, st)
        lr.Recalled = true
        u.setLease(c.NS, c.Coll, id, lr)         // keep the row (do NOT delete) so a late RETURN can settle it
    }
}
```

`opLeaseSettle` later moves `pendingReclaim → available + spent` once the holder-stop guard (§6.5) passes or an authoritative return arrives. **A late RETURN always books `spent` even at a stale epoch** (it settles `pendingReclaim`); only DRAW/refill is rejected under a stale epoch.

### 6.7 Lease-expiry sweep (second pass in the existing `sweepLoop`)

Mirroring the TTL sweeper (`manager.go:312` `sweepOnce`, leader-gated, `time.Now().UnixMilli()` stamped pre-propose). A `budExpiryDueQuery{NowMs, Limit}` Lookup scans `scopeBudExp` for `be(expiresMs) ≤ NowMs` (exact copy of the TTL `scanDue`, `ttl.go`). For each due lease, the leader proposes `opLeaseExpire` with the swept-due time stamped. `applyLeaseExpire` re-checks staleness like `opExpire` (`statemachine.go:368`) — skip if the lease was renewed/returned — then applies the §6.5 guard before freeing quantity and writes a settled tombstone (§6.1). Same ticker, same leader gate, no new lifecycle.

### 6.8 Invariant probe (correctness harness, `design/25`)

A `budCheckQuery` Lookup (mirroring `cardCheckQuery`, `statemachine.go`) reads one consistent snapshot and asserts, in STRICT mode:
```
available + leasedOut + pendingReclaim + spent == cap_at_grant_epoch
sum(lease.amount for current-epoch leases) == leasedOut
spent <= cap_at_grant_epoch                    // the real money invariant (Fix 3)
```
Crucially, the conservation equality is expressed against **the epoch the leases were granted under**, not a freshly-lowered cap — so a cap-cut in flight does not fire a false FATAL (the verification's Hole 3). The money invariant (`spent ≤ cap`) is the one that pages.

---

## 7. Node-Side Lease Cache & Local Spend Path

This runs inside each adserver process on the wavespan-sdk `*Client`. It is the leaf of the escrow; it never talks to L0, only to its broker. **It is the sole node-facing surface** (the raw `Draw/Report/Return` client is demoted to an internal/broker client — Fix from the API verification: one canonical node API).

```go
package leasedbudget

type Mode uint8
const ( ModeStrict Mode = iota; ModeRelaxed )

// lease is the leaf copy of the disjoint partition. NEVER merged with any other node's lease.
type lease struct {
    id          uint64
    epoch       uint64
    granted     int64 // immutable, for return accounting (micro-units)
    remaining   int64 // decremented locally with zero coordination
    deadlineMon int64 // monotonic-clock deadline = monotonicReceipt + ttl - selfGuard (Fix 1/4)
}

type budgetCell struct {
    mu        sync.Mutex // guards lease swap + token bucket; NEVER held across an RPC
    key       BudgetKey
    mode      Mode
    cur, next *lease     // double-buffer for hysteresis refill

    // token bucket (pacing meter)
    rate, burst, tokens float64
    lastRefillMon       int64

    refilling atomic.Bool
    refillErr atomic.Pointer[refillState]
    overdraft int64 // RELAXED only, bounded by maxOverdraft

    epoch      atomic.Uint64
    selfGuard  int64 // ns; >= maxClockSkewMs, carried on grant (Fix 4)
    maxPauseNs int64 // self-fence threshold (Fix 1)
    lastSeenMon int64
}
```

### 7.1 Spend — zero-coordination fast path

```go
// Spend consumes n micro-units locally. No RPC, no Raft on the fast path.
func (cell *budgetCell) Spend(n int64) error {
    now := nowMono()
    cell.mu.Lock()

    // Self-fence: a long pause (VM migrate / GC) means our lease may have been reclaimed. Drop & re-draw. (Fix 1)
    if cell.lastSeenMon != 0 && now-cell.lastSeenMon > cell.maxPauseNs {
        cell.cur, cell.next = nil, nil
    }
    cell.lastSeenMon = now

    cell.accrueTokensLocked(now) // tokens += rate*dt, capped at burst

    // Pacing gate: even with escrow remaining, never burn faster than R.
    if cell.tokens < float64(n) {
        cell.mu.Unlock()
        return ErrPacingThrottled
    }
    // Drop expired/stale-epoch current lease. Stop at deadline-selfGuard (Fix 4: was `now >= expiresAt`).
    if cell.cur != nil && (now >= cell.cur.deadlineMon || cell.cur.epoch != cell.epoch.Load()) {
        cell.cur = nil // abandon remainder; broker reclaims after its late guard
    }
    if (cell.cur == nil || cell.cur.remaining == 0) && cell.next != nil &&
        now < cell.next.deadlineMon && cell.next.epoch == cell.epoch.Load() {
        cell.cur, cell.next = cell.next, nil
    }

    avail := int64(0)
    if cell.cur != nil { avail = cell.cur.remaining }

    switch {
    case avail >= n:
        cell.cur.remaining -= n
        cell.tokens -= float64(n)
        low := cell.cur.remaining <= cell.lowWatermark
        cell.mu.Unlock()
        if low { cell.triggerRefill() }
        return nil
    case cell.mode == ModeRelaxed && cell.overdraft+(n-avail) <= cell.maxOverdraft:
        if cell.cur != nil { cell.cur.remaining = 0 }
        cell.overdraft += n - avail
        cell.tokens -= float64(n)
        cell.mu.Unlock()
        cell.triggerRefill()
        return nil
    default: // STRICT empty, or RELAXED past overdraft bound
        cell.mu.Unlock()
        cell.triggerRefill()
        return ErrBudgetUnavailable
    }
}

func (cell *budgetCell) triggerRefill() {
    if !cell.refilling.CompareAndSwap(false, true) { return } // single-flight
    go func() { defer cell.refilling.Store(false); cell.refillOnce() }()
}
```

### 7.2 Refill — cumulative reporting, stable per-refill leaseID (Fixes 2 & 3)

```go
func (cell *budgetCell) refillOnce() {
    ctx, cancel := context.WithTimeout(context.Background(), cell.refillTimeout) // small, e.g. 200ms
    defer cancel()

    // STABLE leaseID for THIS logical refill: retries reuse the SAME id (create-if-absent + tombstone
    // make that exactly-once regardless of ring eviction). A fresh nonce is minted ONLY for a new refill.
    leaseID := cell.pendingDrawNonce() // generated once per logical refill, reused across retries

    grant, err := cell.broker.Draw(ctx, DrawArgs{
        Namespace:  cell.key.Namespace, Budget: cell.key.Budget, HolderID: cell.nodeID,
        Amount:     cell.leaseChunk, Epoch: cell.epoch.Load(),
        ReportSpentCumulative: cell.cumulativeSpent(), // cumulative-per-lease (Fix 2): NOT a drained delta
        LeaseID:    leaseID,
    })
    if err != nil { cell.refillErr.Store(&refillState{at: time.Now(), err: err}); return }
    if grant.AlreadySettled { cell.rotateDrawNonce(); return } // retry hit tombstone; mint fresh next time
    if grant.Denied { cell.refillErr.Store(&refillState{at: time.Now(), deny: true}); return }

    cell.mu.Lock()
    cell.applyGrantTunablesLocked(grant) // rate, burst, epoch, ttl, selfGuard ride on the grant
    l := &lease{
        id: grant.LeaseID, epoch: grant.Epoch, granted: grant.Amount, remaining: grant.Amount,
        deadlineMon: nowMono() + grant.TTLNanos - cell.selfGuard, // single monotonic clock (Fix 1/4)
    }
    if cell.overdraft > 0 { // RELAXED: pay down before spendable
        pay := min64(cell.overdraft, l.remaining); l.remaining -= pay; cell.overdraft -= pay
    }
    if cell.cur == nil || cell.cur.remaining == 0 { cell.cur = l } else { cell.next = l }
    cell.rotateDrawNonce() // this refill committed; next refill uses a fresh nonce
    cell.mu.Unlock()
}
```

### 7.3 Crash & waste bound

The cache is in-memory only. On crash, unspent `cur+next` is lost but **not** double-spent and **not** lost globally — it returns to the broker after the §6.5 guard. Waste ≤ `cur.remaining + next.remaining ≤ 2·leaseChunk`; expected ≈ one chunk; reclaim latency ≤ `ttl + guard`. Tune `leaseChunk` so a lease lasts ~one refill interval at the node's spend rate; refill RPC rate ≈ `1/refillInterval` per active budget per node, **independent of impression QPS** — the write-ceiling break, made measurable.

---

## 8. Dynamic Steering, Recall, Epochs

Central steers `(N, R, burst)` via `opBudSteer`, which bumps `epoch`. Default propagation is **lease expiry** (short TTLs, seconds): leases re-draw under the new epoch within ~one TTL. Two emergency ops layer on top.

**RECALL (`BudgetRecall`).** Emergency best-effort claw-back. Per Fix 1 (§6.6) it is **invalidate-now + reclaim-after-settle**: bumps epoch (future draws under old epoch rejected immediately) and moves matched leases' quantity `leasedOut → pendingReclaim`, **without** crediting `available`. Quantity is freed only on authoritative return or after the holder-stop guard. STRICT does **not** re-grant `pendingReclaim` until settled (underspend stall, never overspend); RELAXED may re-grant optimistically and reconcile via the debt path. A `BudgetWatch` stream (§10) lets the cache learn of a recall sub-TTL instead of waiting for its next Draw/Report — giving STRICT money a sub-second revocation path without per-spend coordination.

**FREEZE/THAW.** `BudgetFreeze` sets `frozen` (new grants return `granted=false`); `drain=true` also recalls unspent. `BudgetThaw` lifts it.

**Cap-cut enforcement (Fix 3 — honor "never exceed even transiently").** A `BudgetSteer` that lowers `N` below `leasedOut + spent` is the only STRICT path that could transiently overshoot the *new* cap. It is therefore **hard, not soft**: `opBudSteer` with `N' < leasedOut + spent` mandatorily triggers a RECALL of old-epoch leases whose granted-but-unspent portion would push `(spent + outstanding-unspent)` above `N'`, and **blocks new grants** until reclaimed unspent brings `leasedOut ≤ N' − spent`. The resulting underspend stall is the correct STRICT behavior (already-spent quantity under the old epoch stands — you cannot un-spend money; the money invariant is `spent ≤ cap_at_grant_epoch`). If product wants soft drains, that budget must be RELAXED for the duration of the cut.

**Cross-facet contradiction resolved.** The canonical recall semantics are the hierarchy-xcluster guard-band model + `pendingReclaim`; the "move full amount back immediately" and "reject-late-report" answers are **deleted**. Recall = invalidate-now + reclaim-after-settle; a late return **always books spent**.

---

## 9. STRICT vs RELAXED Modes

**STRICT.** `Spent ≤ N(e)` always (modulo the explicitly-closed slow-clock guard band, §6.5, and forward-only cap-cut handled hard, §8). CP: gives up availability under partition (stall = UNDERSPEND). For money.

**RELAXED.** May transiently overshoot, converges. AP: preserves liveness via bounded overdraft. For frequency caps.

### 9.1 Overdraft bound — the honest bound (Fixes 1+3 from liveness verification)

A leaf gets, beside its real lease, an overdraft allowance `D`. Per-node max unreconciled overshoot = `D`. The honest global bound includes **unreported in-flight spend**, not just `D`:
```
maxOvershoot <= Σ_leaf (D + L_report_lag) + intermediate_overdraft
```
where `L_report_lag` = max spend between two reports. Shrink it by: (a) **event-driven reporting** — a node forces a report the instant it enters overdraft, so detection lag → RTT, not heartbeat interval; (b) the broker **debits its grantable headroom by a holder's last-reported outstanding overdraft at grant time** (overdraft rides on the Draw request), so cross-tier over-commit cannot accumulate. Size `D_node = f · cap / n_leaf` so `Σ D` stays within an acceptable fraction `f` of cap. Overdraft is **leaf-only by default** (no cross-tier multiplication).

### 9.2 Reconciliation — proportional throttle, NOT stop-all (Fix 2 from liveness verification: kills the limit cycle)

Reports are **cumulative-per-lease**, folded with `max`: `lease.spent = max(lease.spent, reported_cumulative)`; global `spent = Σ lease.spent`. This is idempotent and self-healing under loss/reorder/retry (a duplicate or stale report is a no-op). The node SDK reports the lease's running total, **not** a drained delta — eliminating the double-count hole the fresh-key+drained-delta combination caused.

When reports push `available` negative (overdraft detected), the parent enters debt recovery but does **NOT** stop-all-then-grant-all (which produced a stall↔burst relaxation oscillator of amplitude `N·D`). Instead it **throttles proportionally**: keep granting, but cap each grant to accrued pace tokens minus a per-holder deficit share, so aggregate granted throughput stays `== R` while the deficit is paid down monotonically:
```
target_grant_i = max(0, accrued_tokens / active_holders - per_holder_deficit_share)
```
Delivery stays smooth at rate `R`; the deficit converges monotonically instead of oscillating. Convergence time ≈ one report interval (detection) + one TTL (in-flight drain); steady-state error → 0.

---

## 10. Proto Service & Messages

```proto
syntax = "proto3";
package wavespan.v1;
import "wavespan/v1/common.proto"; // ResponseMeta, Version

// BudgetService is the LeasedBudget escrow API: a quantity partitioned into disjoint, never-merged
// leases. Granting atomically does available-=amount, leasedOut+=amount in ONE Raft entry; a holder
// spends locally with zero coordination; consensus is touched once per lease-worth. Mutations are
// linearizable through the owning shard leader (like proposeCmd/HIncrBy); Stat/Leases default to
// bounded-stale (linearizable=true for a quorum read). Cross-cluster grant is an explicit request/
// grant RPC routed via WithForwarder to the authority's leader, NOT the LWW global mirror.
service BudgetService {
  // --- controller surface ---
  rpc BudgetDefine (BudgetDefineRequest) returns (BudgetStatResult);
  rpc BudgetSteer  (BudgetSteerRequest)  returns (BudgetStatResult); // cap-cut => mandatory recall+block (§8)
  rpc BudgetRecall (BudgetRecallRequest) returns (BudgetRecallResult); // invalidate-now + reclaim-after-settle
  rpc BudgetFreeze (BudgetFreezeRequest) returns (BudgetStatResult);
  rpc BudgetThaw   (BudgetThawRequest)   returns (BudgetStatResult);
  rpc BudgetStat   (BudgetStatRequest)   returns (BudgetStatResult); // rollup=true => best-effort global view
  rpc BudgetLeases (BudgetLeasesRequest) returns (BudgetLeasesResult);
  rpc BudgetWatch  (BudgetWatchRequest)  returns (stream BudgetEvent); // epoch/freeze/recall push (sub-TTL)

  // --- holder surface (broker-internal & node-cache-internal) ---
  rpc BudgetDraw   (BudgetDrawRequest)   returns (BudgetDrawResult);   // create-if-absent by lease_id
  rpc BudgetReport (BudgetReportRequest) returns (BudgetReportResult); // cumulative-per-lease (max fold)
  rpc BudgetReturn (BudgetReturnRequest) returns (BudgetStatResult);   // release unspent; always books spent
}

enum BudgetMode { BUDGET_MODE_UNSPECIFIED = 0; BUDGET_MODE_STRICT = 1; BUDGET_MODE_RELAXED = 2; }

message BudgetParent { string parent_namespace = 1; bytes parent_budget = 2; uint32 level = 3; } // 0=global,1=broker,2=node

message BudgetDefineRequest {
  string namespace = 1; bytes budget = 2;
  int64  cap_units = 3;            // N (micro-units)
  double rate_units_per_sec = 4;   // R
  int64  burst_units = 5;
  BudgetMode mode = 6;
  int64  default_lease_ttl_ms = 7; // steering channel; seconds-scale
  int64  default_lease_amount = 8;
  int64  self_guard_ms = 9;        // >= maxClockSkewMs; carried to holders (Fix 1/4)
  int64  max_pause_budget_ms = 10; // holder self-fence threshold (Fix 1)
  int64  dedup_retry_window_ms = 11; // tombstone retention >= max transport replay (Fix 2)
  optional BudgetParent parent = 12; // empty => root authority
  optional string idempotency_key = 13;
}

message BudgetSteerRequest {
  string namespace = 1; bytes budget = 2;
  optional int64  cap_units = 3; optional double rate_units_per_sec = 4; optional int64 burst_units = 5;
  optional BudgetMode mode = 6;
  optional uint64 expected_epoch = 7; // guards against racing a recall; mismatch => Aborted
  bool   hard_cap_cut = 8;            // STRICT default true: cap decrease recalls + blocks (§8)
  optional string idempotency_key = 9;
}

message BudgetDrawRequest {
  string namespace = 1; bytes budget = 2;
  string holder_id = 3;
  int64  amount_units = 4;
  optional int64 ttl_ms = 5;
  uint64 known_epoch = 6;
  bytes  lease_id = 7;                 // STABLE per logical refill: hash(holder,budget,draw_nonce); retries reuse it
  int64  report_spent_cumulative = 8;  // accounting rides on the draw (cumulative-per-lease, Fix 2)
  int64  carried_overdraft = 9;        // RELAXED: debit broker headroom at grant time (§9.1)
}
message BudgetDrawResult {
  ResponseMeta meta = 1;
  bool   granted = 2;          // false => exhausted/frozen (STRICT underspend, not an error)
  bool   already_settled = 3;  // retry hit tombstone: NOT a fresh grant (Fix 2)
  Lease  lease = 4;            // amount may be < requested (PARTIAL)
  bool   partial = 5;
  uint64 epoch = 6;
  int64  available_after = 7;
  int64  retry_after_ms = 8;   // backpressure hint, surfaced to the caller (API fix)
  // steering tunables ride on every grant (at-most-TTL propagation for free):
  double rate_units_per_sec = 9; int64 burst_units = 10; int64 ttl_ms = 11;
  int64  self_guard_ms = 12; int64 max_pause_budget_ms = 13;
}

message Lease {
  bytes  lease_id = 1; string holder_id = 2;
  int64  amount_units = 3; int64 spent_units = 4;
  uint64 epoch = 5; int64 ttl_ms = 6; // RELATIVE ttl (holder stamps its own monotonic deadline, Fix 1)
  uint32 level = 7;
}

message BudgetReportRequest {
  string namespace = 1; bytes budget = 2; bytes lease_id = 3; uint64 epoch = 4;
  int64  spent_cumulative = 5; // ALWAYS cumulative-per-lease; folded with max (Fix 2). No DELTA default.
  bool   final = 6;            // also returns the lease
  optional string idempotency_key = 7;
}
message BudgetReportResult {
  ResponseMeta meta = 1; bool accepted = 2; // false => lease unknown/expired (NOT for stale epoch on spent)
  int64  lease_remaining = 3; uint64 epoch = 4; bool over_cap = 5; // RELAXED advisory, surfaced (API fix)
}

message BudgetReturnRequest {
  string namespace = 1; bytes budget = 2; bytes lease_id = 3; uint64 epoch = 4;
  int64  spent_cumulative = 5; // late/stale-epoch return STILL books spent (settles pendingReclaim, Fix 1)
  optional string idempotency_key = 6;
}

message BudgetRecallRequest {
  string namespace = 1; bytes budget = 2; repeated string holder_ids = 3; // empty => all
  optional string idempotency_key = 4;
}
message BudgetRecallResult {
  ResponseMeta meta = 1; uint64 new_epoch = 2;
  int64  moved_to_pending_reclaim = 3; // NOT credited to available yet (Fix 1)
  uint32 leases_invalidated = 4; repeated string unreachable_holders = 5;
}

message BudgetFreezeRequest { string namespace = 1; bytes budget = 2; bool drain = 3; optional string idempotency_key = 4; }
message BudgetThawRequest   { string namespace = 1; bytes budget = 2; optional string idempotency_key = 3; }

message BudgetWatchRequest { string namespace = 1; bytes budget = 2; string holder_id = 3; }
message BudgetEvent { uint64 epoch = 1; bool frozen = 2; bool recalled = 3; int64 new_cap_units = 4; double new_rate = 5; }

message BudgetStatRequest  { string namespace = 1; bytes budget = 2; bool linearizable = 3; bool rollup = 4; }
message BudgetLeasesRequest{ string namespace = 1; bytes budget = 2; bool linearizable = 3; int32 limit = 4; }

message BudgetStatResult {
  ResponseMeta meta = 1; bool exists = 2;
  int64  cap_units = 3; double rate_units_per_sec = 4; int64 burst_units = 5;
  BudgetMode mode = 6; uint64 epoch = 7; bool frozen = 8;
  int64  available_units = 9; int64 leased_out_units = 10;
  int64  pending_reclaim_units = 11; int64 spent_units = 12; int64 overdraft_units = 13;
  uint32 outstanding_leases = 14; double utilization = 15;
  int64  rollup_as_of_unix_ms = 16; // for rollup=true: staleness of the best-effort global view (API fix)
}
message BudgetLeasesResult { ResponseMeta meta = 1; repeated Lease leases = 2; }
```

**Consistency:** all mutations are **linearizable** (one Raft entry on the owning shard leader, like `proposeCmd`/`HIncrBy`); `BudgetStat`/`BudgetLeases` are **bounded-stale by default**, `linearizable=true` for a quorum read. STRICT safety never relies on a read — only on the atomic grant.

**Error contract** (matching `wavespan-sdk/errors.go`, which carries only a `codes.Code` via `wrapErr`/`CodeOf` — no typed sentinels): exhaustion is signalled **one way** — `BudgetDraw` returns `ResourceExhausted` with `retry_after_ms` in status details (not also a `granted=false` success). Exported classifiers: `IsBudgetExhausted` (ResourceExhausted), `IsPacingThrottled` (ResourceExhausted + detail), `IsStaleEpoch` (Aborted), `IsBudgetFrozen` (FailedPrecondition), `IsWrongType` (FailedPrecondition / WRONGTYPE from `ensureType`). `ErrBusy`/load-shed → `ResourceExhausted` with backoff; `ErrFrozen` subrange split → transient retry.

---

## 11. Go SDK Client Surface

```go
// Node surface — the ONLY node-facing API (Fix: single canonical surface).
func (c *Client) LeasedBudget() *LeasedBudgetClient

func (lb *LeasedBudgetClient) Acquire(ctx context.Context, key BudgetKey, opts ...AcquireOpt) (*Budget, error)

type Budget struct{ /* wraps *budgetCell; subscribes to BudgetWatch at Acquire */ }
func (b *Budget) Spend(n int64) error                            // zero-coordination fast path (§7.1)
func (b *Budget) SpendBlocking(ctx context.Context, n int64) error // STRICT opt-in wait for `next`
func (b *Budget) Remaining() int64
func (b *Budget) Return(ctx context.Context) error               // graceful shutdown
// Multi-budget atomicity for daily+total (API fix #6): all-or-nothing local reserve/commit.
func (b *Budget) Reserve(amounts ...BudgetAmount) (*Reservation, error)
func (r *Reservation) Commit() error
func (r *Reservation) Rollback() // compensating local uncommit; no impression served => no drift

// Controller surface — on a separate BudgetClient.
func (c *Client) Budget() *BudgetClient
func (bc *BudgetClient) WithIdempotencyKey(key string) *BudgetClient
func (bc *BudgetClient) Define(ctx, ns, budget, spec) (BudgetStat, error)
func (bc *BudgetClient) Steer(ctx, ns, budget, cap *int64, rate *float64, expectedEpoch uint64, hardCut bool) (BudgetStat, error)
func (bc *BudgetClient) Recall(ctx, ns, budget, holders ...string) (RecallResult, error) // struct, not naked tuple (API fix)
func (bc *BudgetClient) Freeze(ctx, ns, budget, drain bool) (BudgetStat, error)
func (bc *BudgetClient) Thaw(ctx, ns, budget) (BudgetStat, error)
func (bc *BudgetClient) Stat(ctx, ns, budget, linearizable, rollup bool) (BudgetStat, error) // rollup => global view
func (bc *BudgetClient) Leases(ctx, ns, budget, limit int, linearizable bool) ([]Lease, error)
```

`Quantity` is a validated `int64` micro-unit (rejecting non-decimal money at the boundary — matching `HIncrBy`'s typed int64 discipline, not a free-form string). Each controller method mirrors `wavespan-sdk/collections.go`: build the request with `IdempotencyKey: bc.idemPtr()`, call the stub, `return ..., wrapErr("<RPC>", err)`. Result structs (`DrawResult`/`RecallResult`/`ReportResult`) carry `retry_after_ms`/`available_after`/`over_cap` so wire hints survive to the caller.

---

## 12. Worked End-to-End Example

A vires line-item `LI-42` daily budget of **€500** (500,000,000 micro-€), STRICT, paced over a 10-hour delivery window (`R ≈ 13,888 µ€/s`), spent by adservers in **cluster EU** and **cluster US**.

### (a) Central controller defines + steers

```go
bc := client.Budget().WithIdempotencyKey("define-LI-42-2026-06-25")
const day = "2026-06-25"
ns, budget := "pacing", []byte("li/LI-42/daily/"+day)

// L0 root authority owns the master cap. parent=nil => root.
stat, err := bc.Define(ctx, ns, budget, wavespan.BudgetSpec{
    Cap:             500_000_000,            // €500 in micro-€
    RatePerSec:      13_888,                 // paced over 10h
    Burst:           5_000_000,              // ~€5 burst headroom
    Mode:            wavespan.ModeStrict,    // money => STRICT, never overspend even transiently
    DefaultLeaseTTL: 3 * time.Second,        // short TTL = steering propagates within ~3s
    DefaultLeaseAmt: 600_000,                // ~€0.60 block (sized later by broker auto-tune)
    SelfGuardMs:     500,                    // >= maxClockSkewMs (design/22)
    MaxPauseBudgetMs:2000,                   // holder self-fences if paused > 2s (Fix 1)
})
// One Raft entry on L0's shard leader. available=500M, leasedOut=0, pendingReclaim=0, spent=0, epoch=1.

// Two hours in, delivery is behind pace. Controller speeds up (R only; cap unchanged => no cap-cut).
newRate := 18_000.0
_, _ = bc.WithIdempotencyKey("steer-LI-42-speedup-1").
    Steer(ctx, ns, budget, nil, &newRate, /*expectedEpoch*/1, /*hardCut*/false)
// opBudSteer bumps epoch -> 2, resets lastRefillMs. Outstanding 3s-TTL leases re-draw under epoch 2
// within one TTL, adopting the faster rate. No recall needed for a rate change.
```

### (b) Adserver draws + spends + refills (in cluster EU)

```go
b, _ := client.LeasedBudget().Acquire(ctx, leasedbudget.BudgetKey{Namespace: ns, Budget: budget})
// Acquire opens a budgetCell, subscribes to BudgetWatch, and triggers the first refill:
//   node -> EU broker (L1) BudgetDraw{amount=600k, lease_id=hash(node,budget,nonce1), epoch=2}
//   EU broker has block-lease from L0 (drawn earlier); it sub-leases 600k as ONE Raft entry on the
//   EU broker shard. Node installs cur = {remaining:600k, deadlineMon: monoNow + 3s - 500ms}.

for impression := range impressions { // millions/sec across the fleet
    if err := b.Spend(1_200); err != nil {     // €0.0012 per impression
        switch {
        case errors.Is(err, leasedbudget.ErrPacingThrottled):
            servePacingFallback(impression)     // ahead of rate R; spread over time
        case errors.Is(err, leasedbudget.ErrBudgetUnavailable):
            serveNoBudgetFallback(impression)   // STRICT empty: do NOT serve (underspend OK, overspend never)
        }
        continue
    }
    serveAd(impression) // ZERO RPC on this path: pure in-memory decrement + token check
}
// At 30% remaining (lowWatermark), Spend() fires triggerRefill() off-path: a single-flight BudgetDraw
// for `next` with report_spent_cumulative = this lease's running total (cumulative, idempotent). The
// next lease lands before cur drains -> spend never stalls. Steering tunables (rate/ttl) ride on the
// grant, so the node already paces at the steered rate. Consensus touched ~1x/refill, not per impression.
```

Cluster US runs the identical code against the same `(ns,budget)`; its US broker holds a *disjoint* block-lease from L0. The two clusters never merge counts — L0 partitioned 500M into disjoint block-leases, so EU and US can each only spend what they were atomically granted. `Σ spent ≤ €500` holds across both regions with no LWW.

### (c) Emergency recall

```go
// 16:00: the advertiser pauses LI-42 immediately (creative pulled). Controller recalls everything.
res, _ := client.Budget().WithIdempotencyKey("recall-LI-42-pull-1").
    Recall(ctx, ns, budget) // empty holders => recall ALL
// On L0: opLeaseRecall bumps epoch -> 3. Every block-lease at epoch<3 moves leasedOut -> pendingReclaim.
// available is NOT credited (Fix 1). res.MovedToPendingReclaim reports the in-flight total.
// BudgetWatch pushes {epoch:3, recalled:true} to every subscribed broker/node sub-TTL.

// What happens at each holder:
//  - EU/US brokers: their next BudgetDraw to L0 is rejected (stale epoch); they stop sub-leasing and
//    push the recall down to their nodes via BudgetWatch.
//  - Adserver nodes: on the watch event (or next Spend touching the lease) they see epoch != cur.epoch,
//    drop cur/next, and STOP spending. A node mid-impression spends at most what was already in its
//    in-memory lease before the drop -> bounded, never re-granted quantity.
//  - Settle: each broker BudgetReturns its block-lease with cumulative spent. A late/stale-epoch return
//    STILL books spent (settles pendingReclaim). For block-leases not returned, L0's opLeaseSettle frees
//    pendingReclaim only after grantor-monotonic > granted + ttl + 2*skew + maxPauseBudget -> provably
//    after every holder has stopped. STRICT never re-grants pendingReclaim, so no double-spend window.
// Net: spend halts within ~one watch-RTT (sub-second) instead of one TTL; money safety holds throughout.
```

---

## 13. Observability, Metrics & Config

### 13.1 Metrics (mirroring `internal/replication/global/metrics.go` style)

`internal/leasedbudget/metrics.go` defines a `Metrics` struct + `NewMetrics(reg prometheus.Registerer)`. Per-budget quantity **gauges are pull-based** (a `prometheus.Collector` walks local state at scrape — the spine makes increments coordination-free, so pushing per-increment is wrong); apply-path **counters are incremented inline** (cheap, once per lease-worth).

Gauges (labels `{budget, mode, level}`, bounded cardinality — never `lease_id`/`holder`): `cap`, `available`, `leased_out`, `pending_reclaim`, `spent_reported`, `tokens`, `rate`, `utilization_pct`, `outstanding_leases`, `epoch`, `target_paced_spend`, `pacing_error_units`, `refill_stall_seconds`, `local_lease_exhausted`. Counters (`{budget, level, reason}`): `grants_total`, `grant_units_total`, `refills_total`, `refill_rejects_total`, `lease_returns_total`, `returned_unused_units_total`, `recalls_total`, `recall_pending_units_total`, `stale_lease_rejects_total`, `overdraft_events_total`, `overdraft_units_total`. Histogram: `refill_rtt_seconds` (cross-cluster grant latency).

Alerts: `budget_utilization_pct{mode="strict"} > 100` → **pages** (`StrictBudgetInvariantViolated` — a correctness bug, the LWW failure the spine sidesteps); `refill_stall_seconds > leaseTTL` → `BudgetRefillStalled` (partition → underspend); `refill_rtt_seconds` p99 high at `level="broker"` → `CrossClusterGrantSlow`; `overdraft_units_total{mode="relaxed"}` high → `RelaxedBudgetOverdraftHigh`.

### 13.2 Inspect surface

gRPC `BudgetStat`/`BudgetLeases` (bounded-stale default; `rollup=true` returns the L0 best-effort global view with `rollup_as_of_unix_ms`). Read-only HTTP via the `/admin/*` gateway (`design/14`): `GET /admin/budget/{ns}/{base64_budget}`, `GET /admin/budgets`. RECALL/FREEZE are **gRPC-only** (operator role), never HTTP.

### 13.3 Config — `BudgetNamespace` CRD (operator-reconciled, `design/12` conventions)

```yaml
apiVersion: db.Wavespan.io/v1alpha1
kind: BudgetNamespace
metadata: { name: pacing }
spec:
  clusterRef: prod
  replicationPolicyRef: active-active-global   # cross-cluster grant requires global peers (NOT LWW mirror)
  mode: strict
  defaults: { leaseTtlMs: 3000, refillWatermark: 0.30, autotune: true, rateUnitsPerSec: 0,
              selfGuardMs: 500, maxPauseBudgetMs: 2000, dedupRetryWindowMs: 30000 }
  hierarchy: { levels: [global, broker, node], brokerRaftGroup: pacing-broker }
  steering: { maxLeaseTtlMs: 10000, epochOnConfigChange: true }
status:
  conditions:
    - { type: Ready, status: "True" }
    - { type: GlobalAuthorityReachable, status: "True" } # broker->global grant path health (cross-cluster)
```

Admission validation rejects: `mode: strict` with a `replicationPolicyRef` allowing `keep-siblings`/LWW counter merge (STRICT must never merge counts); `leaseTtlMs ∉ (0, maxLeaseTtlMs]`; `refillWatermark ∉ (0,1)`; `cap/rate < 0`; `hierarchy.levels` ≠ the 3-level escrow; `lease_size_units > cap_units`; `dedupRetryWindowMs < maxClockSkewMs + maxPauseBudgetMs`.

### 13.4 Auto-tuning

`r̂` = EWMA of the holder's local spend rate. `lease_size = clamp(r̂ · ttl, minLease, parent.available · maxGrantFraction)` with `maxGrantFraction = 0.25` (no child starves siblings). STRICT biases leases **smaller** (favor frequent refills over stranded budget); RELAXED **larger**. Refill QPS ≈ `3 / lease_ttl_seconds` per holder, independent of impression rate — the measurable write-ceiling break.

### 13.5 Correctness harness (`design/25`)

New `tests/harness/workloads/budget/` + `checker/budget.go`:
- **`budget-strict-cap`** (continuous safety, hard-fail, PR-gate): `sum(acked spend) ≤ cap` *at every instant*.
- **`budget-relaxed-convergence`** (post-heal, like `bank`): all acked spend accounted, overshoot reconciled.
- **`budget-no-overspend-under-partition`**: a partitioned cluster never exceeds its lease-at-partition.
- **`budget-idempotent-spend`**: replay the same `lease_id`/draw nonce across partition + origin-crash → exactly-once decrement.

**Nemeses combined (the verification mandate):** `budget-strict-cap` must run with `cluster-partition` + `clock-skew-bounded` + **`pause`/`resume` past TTL** + **lossy cross-cluster grant-retry** + **cap-cut steering** *together* — the old PR-gate subset (partition-halves + kill-origin) would pass H1/H2 vacuously. **Negative control:** disable the Fix-1 epoch backstop (or the §6.5 guard) and confirm the checker goes RED, proving it catches the double-spend.

---

## 14. Implementation Staging & Open Questions

### 14.1 Staging

**Stage 1 — minimal atomic conditional-deduct primitive.** Add `opLeaseGrant` as a `compare-and-decrement` on a single budget field (check `available ≥ amount && epoch == current`, decrement, emit lease) committed in one Raft entry — reusing `applyHIncrInt`'s shape and the existing dedup. No hierarchy, no pacing, no recall: just exact, idempotent, leader-routed escrow draws within one cluster. This alone replaces the hot-counter `HIncrBy` for single-cluster pacing #2 and is independently shippable/testable.

**Stage 2 — full datatype, single cluster.** Promote to the full `typeBudget` with `scopeBudState`/`scopeBudLease`/`scopeBudExp`/`scopeBudTomb`, token-bucket pacing, lease-expiry sweep, RECALL/FREEZE with `pendingReclaim`, settled tombstones, cumulative-per-lease reporting, the node lease cache, and `BudgetWatch`.

**Stage 3 — 3-level hierarchy + cross-cluster grant.** Wire the L1 broker level and the cross-region `BudgetDraw`/`BudgetReport`/`BudgetReturn` RPC path (request/grant, NOT the LWW global applier), the §6.5 guard-band reclaim, and the cap-cut hard-recall.

**Stage 4 — auto-tune, rollup stat, full harness soak + dashboards.**

### 14.2 Open questions

1. **Cross-region RPC transport.** Does `BudgetService` (synchronous request/response) ride the existing global-repl endpoint (`design/06` `Wavespan-repl.<region>`, cheap-mTLS) or a dedicated grant endpoint? It does not fit the streaming `PushGlobal`/`FetchRange` model and likely needs its own connect-go binding. Where the remote call terminates and how the remote L0 leader is discovered across clusters needs the placement-driver story (`design/30` meta Raft group).
2. **L0 single-region availability.** The root being one Raft group in the home region means a full home-region outage stalls *all* refills globally (clusters serve outstanding leases, then stall). Acceptable, or does the root need a standby/region-failover protocol? `design/30` §19 only sketches "leader-per-region" as FUTURE.
3. **Per-budget single-shard ceiling.** One budget = one shard leader caps L0 grant throughput at one Raft group. The hierarchy mitigates (L0 grants to ~N brokers, not per-impression), but an extremely hot top-level line-item is bounded by that single leader. Do we need a sharded master pool (sum-of-sub-pools + rebalancer)? The escrow model composes, so sub-pool sharding is possible but adds a rebalancer.
4. **`maxPauseBudget` / `maxClockSkew` tuning.** The §6.5 guard band = `2·skew + maxPauseBudget`; larger values mean slower reclaim (more underspend) but safer against long VM pauses/leap-smear. What pause/skew bounds do production clusters actually exhibit? Is HLC (`design/22`) available to holders to tighten skew below the 500ms default?
5. **RELAXED `D` sizing vs fleet size.** `D_node = f·cap/n_leaf` needs the live fleet size to bound `Σ D` within fraction `f` of cap. How is `n_leaf` discovered (broker-counted active holders)? And the gone-holder write-off: an L2 node that overdrafts then permanently dies never reports — need a bounded-staleness write-off policy for permanently-absent holders.
6. **Holder identity/auth.** `holder_id` is currently free-form; two nodes colliding on `nodeID` (config typo, autoscaler hostname reuse) are indistinguishable. Minimum: validate Report/Return `holder == lease.holder`. Stronger: an authority-signed `HolderToken` bound to a principal, issued at first Draw. Security model unspecified.
7. **Multi-budget atomicity granularity.** §11 offers local `Reserve/Commit/Rollback` for the daily+total pair. Is local two-phase enough, or do we need a server-side `BudgetDrawMulti` (all-or-nothing across N budgets on possibly-different shards), which reintroduces cross-shard coordination?

---

## 15. Mapping vires onto LeasedBudget

vires line-item budgets map onto the bytes-key + namespace convention used for collections (`design/34`: "Redis logical DB → waveSpan namespace"):

| vires concept | LeasedBudget mapping | Mode |
|---|---|---|
| pacing namespace (was Redis hash `pacing`) | namespace `pacing` (a `BudgetNamespace` CRD) | — |
| total budget of line-item `<li>` | budget `li/<li>/total`, cap = total cap | **STRICT** (money) |
| daily budget of `<li>` on `<date>` | budget `li/<li>/daily/<date>`, cap = daily cap, `rate` from the delivery schedule | **STRICT** (money) |
| frequency cap per user/creative | namespace `frequency`, budget `cap/<cwID>/<creative>`, cap = N impressions | **RELAXED** (approximate) |

**Pacing / budget (STRICT).** This is the migration target. Today it would be a CP `HIncrBy` on a single hot field `pacing/<li>:dailySpent:<date>` — which cannot sustain per-impression increments across regions. It moves to a STRICT LeasedBudget: the global authority owns the daily/total master cap; per-cluster broker Raft groups are the middle escrow level; stateless adservers are leaf lease holders spending in-memory. Daily budgets get a natural rollover by `<date>`; the controller writes a fresh `BudgetConfig` per day (`cap = dailyCap`, `rate` from the schedule), bumping epoch. Overspend = real money lost, so STRICT's never-exceed (with the hard cap-cut of §8 and the §6.5 guard) is mandatory; underspend stalls under partition are the accepted failure mode. This slots into `design/34` Phase 3 (Exact + atomic, G5) as the production answer for the cross-cluster, high-rate case.

**Frequency capping (RELAXED).** Per-user/creative impression caps tolerate approximation: transient overshoot of a few impressions during a partition is harmless. These map to a RELAXED LeasedBudget (or stay on approximate `HIncrBy` until overcount proves too loose). Overshoot is bounded by `Σ D` (event-driven reporting shrinks the detection lag) and converges via the proportional-throttle debt path (§9.2), never the LWW count-loss the old global-mirror approach would have caused. The frequency-cap counters are exactly the "RELAXED-tolerant" class (`design/34` #1) and are kept on the existing approximate path; only pacing #2 (exact money) requires the STRICT LeasedBudget — coexistence, not replacement.

---

## 16. Independent safety re-verification (2026-06-25)

A second, **independent** adversarial pass (fresh skeptics, no access to the first pass's reasoning, grounded
against the actual `internal/collections` code) was run on the STRICT "never overspend, even transiently"
guarantee and the five fixes §1 claims. Unlike the first pass (whose fixes were folded in by the same
synthesis step that proposed them), this pass concluded:

**BOTTOM LINE: the guarantee DOES NOT hold as written.** The §4.2 telescoping proof is valid only for bucket
*arithmetic*; it never establishes the *temporal disjointness* and *idempotent-settlement* preconditions it
silently relies on. Critically, §6.5/§6.7 assume a **monotonic grantor clock**, but the grounding sweep is
**wall-clock** (`manager.go:322`, `collections.go:159`, and `migrate.go:160` copies `GrantedMs` verbatim on
split) — so "provably non-overlapping" guard band is false.

### 16.1 Confirmed holes

| # | Surface | Sev | One-line repro | Fix kind |
|---|---|---|---|---|
| H-A | In-flight grant time `F` charged to nobody (holder anchors deadline at receipt `G+F`, grantor at stamp `G`) | Critical | Slow Raft commit + WAN tail makes `F > selfGuard+2·skew+maxPause`; grantor re-grants while holder still spends → same quantity live twice | Doc |
| H-B | Spend latches `nowMono()` once before the decrement | Critical | GC/VM pause after the latch, before the decrement: stale clock passes self-fence + deadline; spend served after reclaim | Doc+Impl |
| H-C | Reclaim trigger assumes monotonic grantor clock; code is wall-clock | Critical | NTP step / leap-smear forward fires reclaim early, erasing the margin | Doc+Impl |
| H-1 | `pendingReclaim` drain not single-shot; no `delLeaseExp`; `opLeaseExpire` reuses TTL staleness check, ignores tombstone | Critical | Recall→late Return settles, then expiry sweep re-fires on the live `scopeBudExp` entry → double-credits `available` → permanent phantom budget | Doc+Impl |
| H-2 | `applyLeaseRecall` never advances `lr.Epoch`, no `if lr.Recalled` guard | Critical | Two recalls both match the same row → double-credit on settle | Doc+Impl |
| H-2b | `opLeaseSettle` writes no tombstone, keeps the row; grant check 0b is epoch/recall-blind | Critical | Recall→settle→re-grant; a partitioned holder's original Draw hits the stale row → quantity live twice | Doc+Impl |
| H-N1 | Nested TTLs don't telescope; each hop re-stamps a fresh `ttl` | Critical | Broker restamps a sub-lease past its own block deadline; leaf spends across L0 reclaim+regrant | Doc+Impl |
| H-N2 | Single global `maxClockSkew=500ms` (an intra-mesh HLC reject threshold) assumed to bound inter-region offset | Critical | Cross-region offset transiently >500ms erodes the guard margin | Doc+Impl |
| H-S1 | Split/leader-change adds a 3rd clock + multi-second leaderless freeze; guard budgets only `2·skew` | Critical | Freeze window > maxPauseBudget; new leader wall clock +500ms; holder self-fences & re-draws while old tail still spends; `BudgetWatch` dropped on cutover | Doc+Impl |
| H-R1 | Dead-holder write-off *credits* unreported real spend back | Major (RELAXED) | Holder overdrafts to D, dies pre-report; expiry re-credits consumed quantity → true overshoot ≈ k·(D+lag), not the published bound | Doc+Impl |
| H-R2 | Proportional throttle clamps a deficit holder to 0 → aggregate < R | Major (RELAXED) | Hot holder stalls then bursts → oscillation relocated global→per-holder; "throughput stays ==R" is false | Doc |

### 16.2 Assumption set the guarantee depends on (and the doc's current status)

STATED + enforced: `selfGuard ≥ maxClockSkewMs`; tombstone retention ≥ replay window (but only when a tombstone
exists — `opLeaseSettle` writes none). **IMPLICIT / unenforced / false:** grant in-flight `F` bounded; no pause
after the single latched `nowMono()`; monotonic grantor clock; single-shot pendingReclaim drain; settling
removes the `scopeBudExp` entry; strictly-nesting TTLs; single-shard-per-budget + continuous grantor + one
stable clock per lease lifetime (violated by Split/Merge/re-election); single global skew bounds inter-region
offset; on-expiry unreported spend not re-credited (RELAXED bound honesty). The STRICT contract wording itself
is unreconciled: §1/§4.1 say `sum(spent) ≤ cap` while §6.8 measures `spent ≤ cap_at_grant_epoch` — these differ
for a cap-CUT.

### 16.3 Prioritized doc edits to make the guarantee airtight

1. **Replicated logical deadline, not a live grantor clock.** At grant, replicate `reclaimNotBeforeMs = grantedMs + ttl + 3·skew + maxPauseBudget`; any future leader's wall `now ≥ reclaimNotBeforeMs` may reclaim. Retract the §6.7 "exact copy of the wall-clock TTL sweeper" claim. (H-C, H-S1 clock leg)
2. **Holder freshness gate (bound `F`).** Reject a grant if `localRecvWall − grantedMs > selfGuard`; anchor `deadline_local` on shared `grantedMs`. State `F ≤ selfGuard` as enforced. (H-A)
3. **Single-shot, terminal-symmetric settlement.** `opLeaseReturn`/`opLeaseSettle`/`opLeaseExpire` each: check tombstone first (no-op if present); write tombstone + delete row + `delLeaseExp` in the *same* entry; clamp drain to `min(pendingReclaim, lr.Amount)`. Harden grant 0b to refuse `lr.Recalled || lr.Epoch < cfg.epoch`. Add `if lr.Recalled { continue }` + advance `lr.Epoch` in `applyLeaseRecall`. (H-1, H-2, H-2b)
4. **Enforce strictly-nesting TTLs:** `child_ttl ≤ parent_residual − (2·skew + child_guard)`; broker self-recalls sub-leases before relinquishing a block. (H-N1)
5. **Forbid Split/Merge of a `typeBudget` while any current-epoch lease is outstanding** (drain-then-split) or pin each budget to a non-splitting shard; re-subscribe `BudgetWatch` on cutover with holder self-fence on watch-loss > selfGuard. (H-S1)
6. **Re-read the monotonic clock immediately before the Spend decrement** (under lock); abort if stale or past deadline. (H-B)
7. **Per-edge cross-region skew bound** in the §6.5 grantor term, with an observed-skew metric. (H-N2)
8. **Reconcile the STRICT contract wording** (`sum(spent) ≤ cap` vs monotone-floor `≤ max_t cap`); document a cap-CUT cannot retroactively bound already-granted in-flight leases below their grant-epoch cap; gate the "even transiently" claim on the enforced precondition set.
9. **RELAXED honesty:** dead-holder write-off must *debit* (`spent := amount`), not credit; republish the bound as `Σ_live(D+lag) + intermediate + Σ_dead(reported_overdraft+lag)`; restate throttle as `R·(unclamped/active)`. (H-R1, H-R2)
10. **Harness nemeses** (`design/25`): grant in-flight delay > guard band; pause injected *mid-Spend*; Split/leader-change with outstanding leases; nested-TTL restamp across a clock-domain boundary; recall→settle→re-grant→stale-holder-resume.

**Consequence for the roadmap:** Stage 1 (this is already planned, see `docs/superpowers/plans/2026-06-25-leased-budget-stage1.md`) is unaffected — it has no time, no reclaim, no recall, no hierarchy, one shard, explicit return. Stage 2 (expiry/pacing-clock) and especially Stage 3 (recall) MUST NOT be built until edits 1–8 land here and are re-verified; Stage 4 (cross-region/hierarchy) additionally needs 4, 5, 7.