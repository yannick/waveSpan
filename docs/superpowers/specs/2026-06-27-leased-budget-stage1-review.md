# LeasedBudget — Plan Refinement & Critical Analysis

## Context

`design/35_leased_budget_datatype.md` (904 lines) proposes **LeasedBudget**, a new
distributed escrow-counter datatype for waveSpan to replace naive distributed increments
in ad-serving (pacing/money budgets, frequency caps). `docs/superpowers/plans/2026-06-25-leased-budget-stage1.md`
(1302 lines) is an 11-task TDD plan for **Stage 1 only** (single-cluster STRICT escrow core:
define / grant / report / return, no time, no recall, no hierarchy).

The user wants this refined before implementation, plus a critical analysis (missing specs),
a summary of the replication/consensus/consistency model, and the high-level API. This document
is the deliverable. Three `Explore` agents verified the plan's ~40 code-grounding claims against
the actual `internal/collections`, `internal/replication`, `internal/storage`, and `proto/` code.

**Headline:** the Stage-1 plan is unusually well-grounded and its conservation math is sound.
The design's *own* §16 already declares the STRICT guarantee broken for Stages 2–4 (clock-model
holes). The remaining work is (a) closing a handful of concrete Stage-1 gaps the plan missed, and
(b) deciding the on-disk key layout *now* so Stages 2–4 don't force a snapshot migration.

---

## Part A — Code-grounding verification (what's real)

| Area | Verdict | Notes |
|---|---|---|
| `opKind` ends at `opBatch=15`; `collType` ends at `typeZSet=3` | ✅ TRUE | next free = opKind 16, collType 4 |
| `typeForOp`; `decodeCommandInto` rejects `typeForOp==0` | ✅ TRUE | except `opExpire`/`opRemove` (type-agnostic) — budget ops MUST be in `typeForOp` |
| `wrongType`/`notNumber` sentinels, `errShortCommand`, exported `ErrWrongType`/`ErrBusy`/… | ✅ TRUE | in `command.go`, no `errors.go` |
| `appendChunk`/`takeChunk` (uint32 len-prefix) | ✅ TRUE | exact signatures |
| `command{Op,NS,Coll,Idem,Items}` / `item{Key,Val,Score,ExpiryMs}` | ⚠️ item has **Score+ExpiryMs** too | harmless for budget (uses Key/Val only) |
| scope bytes `scopeCard..scopeType = 0x00..0x03` under `collScope` | ⚠️ INCOMPLETE (now resolved) | `scopeTTLPtr=0x04` ALSO lives under `collScope` (`ttl.go:17`) — taken bytes are `0x00`–`0x04`; `0x05+` confirmed free, **no collision**, but the plan omits `scopeTTLPtr` from its list |
| `applyOne` dedups on `c.Idem` *before* dispatch, no op-kind discrimination | ✅ TRUE | budget ops correctly leave `Idem` empty |
| `updateCtx{s,ops,exists,zscore,cardDelta,htype,vals,inBatchDedup}` | ✅ TRUE | construction literal matches |
| `applyHIncrInt`→`(int64,[]byte,error)`; `fieldVal`/`setFieldVal` overlay; `CFReplData` | ✅ TRUE | budget apply funcs return `(ProposeResult,error)` instead — fine |
| `Lookup` via `snap.Get(CFReplData,key)`; **no** `getDataSnap` | ✅ TRUE | read snapshot directly |
| dedup ring 4096, `dedupGet`/`dedupRecord` in `CFReplData` | ✅ TRUE | |
| `ProposeResult{Value uint64; Data []byte}` | ✅ TRUE | `raftshard.go:7-10` |
| `storage.StoreOp{CF,Key,Value,Delete,…}` | ⚠️ also `ExpiresAtUnixMs` | use CF/Key/Value/Delete |
| `manager.go` `sweepOnce` (stamps `time.Now().UnixMilli()` pre-propose), `coalescable()` | ✅ TRUE | lines ~358/247 (plan's ~312/221 drifted) |
| `ttl.go` `scanDue`/`clearTTL`, due-ordered index + reverse ptr | ✅ TRUE | template for Stage-2 expiry sweep |
| `service.go` `collErr(err)` mapper + Connect handler pattern | ✅ TRUE | extend the switch, don't add a func |
| test harness `freeAddr`/`newMgr`/`waitReady`/`StartShard`/`NewMemStore`/`SingleShardDirectory` | ✅ TRUE | real |
| `shardPrefix(2)` | ❌ PLACEHOLDER | use `b.prefix` / `prefixEnd`; plan already flags |
| `applySingleForTest` test seam | ❌ MISSING | must be added (plan flags) |
| Global applier = `PolicyHLCLastWriteWins`, outcomes Winner/Tombstone/Siblings/Reject | ✅ TRUE | `applier.go:51,102-110` |
| Collections **not** wired into global layer | ✅ TRUE | zero imports of `internal/collections` in `replication/global` |
| `WithForwarder` + `dir.ShardFor` route each `(ns,coll)` to one leader | ✅ TRUE | |
| `migrate.go` split copies record bytes **verbatim** (type-blind) | ✅ TRUE | basis of design hole H-S1 |
| No synchronous cross-cluster per-key RPC | ✅ TRUE | global svc is streaming + `InspectKey` (gRPC-adapter only) |
| leader-stamps-time-before-propose (SAdd TTL, opExpire compares stamped) | ✅ TRUE | apply never reads wall-clock |
| `common.proto` `ResponseMeta`+`Version` | ✅ TRUE | |
| `maxClockSkewMs=500` is an **intra-mesh HLC reject threshold**, not a cross-region offset bound | ✅ TRUE | confirms design hole H-N2 |

**Conclusion:** every load-bearing symbol the plan relies on exists; the only non-real names are
the two the plan itself labels as placeholders. Line numbers drift by 10–40 (the plan disclaims them).

---

## Part B — Critical analysis: missing specs in the Stage-1 plan

Ordered by severity. The first two are the ones that matter most.

> **STATUS (2026-06-27): ✅ ALL of B1–B10 folded into the revised plan**
> (`docs/superpowers/plans/2026-06-25-leased-budget-stage1.md`), and all Part A grounding refs corrected
> there (scope list +`scopeTTLPtr 0x04`; line refs HIncrBy 287-298 / sweepOnce ~358 / coalescable ~247 /
> handlers ~172-186; `shardPrefix`→inline prefix; `applySingleForTest` add; `errors.go`→`command.go`;
> StoreOp/item field facts). See each item's resolution note below.

### B1. On-disk key layout diverges from the design — forces a future migration (HIGH)
The Stage-1 plan picks `scopeBudPool=0x05`, `scopeBudLease=0x06` and folds config+state into one
`poolRec`. The design §6.1 final layout is `scopeBudCfg=0x05`, `scopeBudState=0x06`,
`scopeBudLease=0x07`, `scopeBudExp=0x08`, `scopeBudTomb=0x09`. When Stage 2/3 split config from
state and add the expiry/tombstone scopes, **the byte assignments collide and the record shape
changes**, requiring snapshot migration of a *money* datatype mid-roadmap.
- **✅ RESOLVED (chosen the other way — amend the design, not the plan):** rather than split the record,
  keep ONE combined `poolRec` at `0x05` and make `decodePool` **append-tolerant** (reads its fixed prefix,
  ignores trailing bytes) so Stages 2–3 *append* `lastRefillMs`/`tokens`/`pendingReclaim` with no snapshot
  migration. Plan keeps `0x05`=pool / `0x06`=lease; `0x07`=exp / `0x08`=tomb reserved. **design §6.1 amended**
  to this combined layout (the old `scopeBudCfg`/`scopeBudState` split is marked superseded). This is less
  churn than splitting and is migration-free.
- **✅ Prerequisite RESOLVED:** `scopeTTLPtr=0x04` (`ttl.go:17`) also lives under `collScope`, so taken bytes
  are `0x00`–`0x04` and `0x05+` is genuinely free — **no collision**. The plan's scope-byte list now includes
  `scopeTTLPtr=0x04`.

### B2. The conservation **invariant probe is omitted** (HIGH — cheap, highest-value safety net)
Design §6.8 specifies a `budCheckQuery` Lookup asserting
`available+leasedOut+spent == cap` and `spent ≤ cap` from one consistent snapshot. The Stage-1 plan
adds only `budStatQuery`. For a money datatype this read-only probe is the single best guardrail and
the basis of the `StrictBudgetInvariantViolated` page (§13.1).
- **Fix:** add `budCheckQuery` in Task 6 alongside `budStatQuery`; assert it in the fuzz test (Task 11).

### B3. RELAXED mode silently accepted but unimplemented (MEDIUM)
`BudgetDefine(mode uint8)` and the proto `BudgetMode` accept `RELAXED`, but Stage-1 grant logic only
implements STRICT. A caller defining RELAXED gets STRICT behavior silently.
- **Fix:** `applyBudInit` rejects `mode != modeStrict` with a sentinel (`budBadMode`) → `ErrUnsupportedMode`.

### B4. No input validation / overflow guard in apply (MEDIUM)
`applyBudInit` doesn't guard `cap ≥ 0`; `applyBudGrant` doesn't guard `amount ≥ 0`. Admission
validation at the CRD layer (§13.3) doesn't protect the in-process API or direct proposers. Apply is
deterministic, so guards are safe and replica-consistent.
- **Fix:** reject negative cap/amount; clamp grant to `available` (already done) and assert no int64
  overflow on `available/leasedOut` accumulation.

### B5. Grant idempotency window is a money foot-gun (MEDIUM — documented, not closed)
The plan correctly notes: after `BudgetReturn` deletes the lease row, a duplicate
`BudgetGrant(sameLeaseID)` grants *fresh* quantity (no tombstone in Stage 1). For money this is a
silent double-spend if a client retries a draw across a return.
- **Fix options:** (a) accept + document loudly that leaseIDs are single-use-forever (caller contract);
  (b) write a minimal settled-tombstone on Return even in Stage 1 (pulls a slice of design §6.3
  forward). Recommend (a) for Stage 1 with a prominent API doc + a test asserting the hazard, and
  schedule (b) for Stage 3. Make the decision explicit rather than leaving it as a footnote.

### B6. `holder` is stored but never validated on Report/Return (LOW for Stage 1)
`leaseRec.Holder` is recorded at grant but `applyBudReport`/`applyBudReturn` look up by leaseID only.
LeaseID-as-capability is acceptable single-cluster, but the contract should be stated (design open Q6).
- **Fix:** one sentence in the spec: "Stage 1 treats `lease_id` as the bearer capability; `holder`
  is advisory. Holder binding/auth is Stage 3+."

### B7. `coalescable()` membership for budget ops is unspecified (LOW)
The Stage-1 plan never touches `coalescable()`. Correctness holds either way (the `u.vals` overlay
composes in-batch ops; un-coalesced ops are separate entries), but throughput and the in-batch dedup
path differ. Design §6.2 says grant/report are coalescable, control-plane ops are not.
- **Fix:** explicitly decide — recommend leaving Stage-1 ops **out** of `coalescable()` (simplest,
  still correct) and note it; revisit when pacing lands.

### B8. No snapshot/restore test for the new datatype (LOW — but it's money)
`base_sm` snapshots the whole shard prefix, so budget keys ride along for free, but the plan adds no
test proving a grant survives snapshot→restore.
- **Fix:** add a snapshot round-trip test (grant, snapshot, new SM, restore, assert pool+lease+INV).

### B9. Error taxonomy: "budget not found" overloaded onto `ErrLeaseUnknown` (LOW)
Grant against an undefined budget returns `budNoLease`→`ErrLeaseUnknown`→`FailedPrecondition`. The
name misleads (it's not a lease problem).
- **Fix:** add `budNoBudget`/`ErrBudgetNotFound` distinct from `ErrLeaseUnknown` for clarity.

### B10. `applySingleForTest` seam (MECHANICAL — plan flags)
Doesn't exist. Add a small unexported seam in `statemachine.go` (construct overlay → `applyOne` →
flush via the real batch path) so `mustApply`/`Lookup` see committed state.

---

## Part C — Replication, consensus & consistency of LeasedBudget

**The data structure.** A budget is a fixed quantity `N` (the cap) **partitioned into disjoint
leases that are never merged**. `cap == available + leasedOut + spent` (Stage 1; Stage 3 adds a
`pendingReclaim` term, making it 5-way). Granting atomically moves quantity between disjoint buckets
(`available -= g; leasedOut += g`) in one operation; a holder spends its own lease locally.

**Consensus tier (within a cluster) — CP, same class as `HIncrBy`.**
- LeasedBudget is **not** its own Raft state machine. It's a new `collType` handled by the *existing*
  `shardSM` on the dragonboat Raft tier, exactly like Sets/Hashes/ZSets.
- Each budget `(ns, budget)` lives on **exactly one shard**, owned by one Raft leader; the placement
  `Directory` + `WithForwarder` route any client to that leader (verified).
- Every mutation (grant/report/return) is **one Raft log entry**: a deterministic read-modify-write
  inside `applyOne`, replicated to the shard's voters, committed by quorum. ⇒ mutations are
  **linearizable**. Reads (`BudgetStat`) are bounded-stale by default, linearizable on request.
- Idempotency: `lease_id` is the grant key; a retry hits the durable lease row (Stage 1) and returns
  the original amount without re-debiting. (The shared 4096-entry dedup ring is the fast path; budget
  ops deliberately leave `command.Idem` empty and enforce idempotency in the apply layer.)

**Why escrow instead of a replicated counter.** waveSpan's cross-cluster layer
(`internal/replication/global`) is **active-active async HLC last-write-wins** (verified:
`PolicyHLCLastWriteWins`; outcomes Winner/Tombstone/Siblings/Reject; "no cross-cluster consensus in
the hot path"). LWW silently discards concurrent increments — unsafe for counters/money — and
collections are **not wired into the global layer at all** (verified: zero imports). Escrow sidesteps
this: because leases are **disjoint and never merged, there is nothing for LWW to clobber**. A lost or
partitioned message can only **strand** quantity (→ underspend), never duplicate it (→ overspend).
That asymmetry is the whole safety argument. Consensus is touched **once per lease-worth of spend**,
not per impression ⇒ it breaks the single-leader hot-counter write ceiling.

**Cross-cluster movement (Stage 4) — explicit request/grant, NOT the LWW mirror.** Budget moves via a
**synchronous remote grant** against the home-region root Raft group (routed by `WithForwarder` to its
leader, committed as one entry). The home root is the single writer of the master cap; other-region
brokers are *clients*, not replicas. **Caveat (verified):** no such synchronous cross-cluster transport
exists today — the global service is streaming (`PushGlobal`/`FetchRange`) plus the per-key
`InspectKey` (served only by the gRPC adapter). Stage 4 must build a new grant transport (design open Q1).

**3-level hierarchy (Stages 3–4).** L0 root authority (one Raft group, home region) → L1 per-cluster
broker (Raft group) → L2 in-memory node lease (no consensus, a token bucket). L0 and L1 run the same
state machine; L2 holds one in-memory lease and spends with zero coordination. Conservation telescopes
across levels *only while disjointness is preserved*.

**Consistency modes.**
- **STRICT (CP):** `spent ≤ cap` always; gives up availability under partition (stall = underspend).
  For money.
- **RELAXED (AP):** bounded transient overshoot `D` that converges via cumulative-`max` reports +
  proportional-throttle reconciliation. For frequency caps.

**⚠️ The standing caveat (design §16).** The STRICT "never overspend *even transiently*" guarantee
**does not hold as written for Stages 2–4.** The §4.2 telescoping proof covers bucket *arithmetic*
but not the *temporal disjointness* it silently assumes: the time-based reclaim/recall machinery
assumes a **monotonic grantor clock the engine does not provide** — the sweep is wall-clock (verified:
`manager.go`, `migrate.go` copies lease bytes verbatim across splits, `maxClockSkew=500ms` is an
intra-mesh HLC threshold, not a cross-region bound). §16 lists 11 holes and 10 prioritized doc edits.
**Stage 1 — no time, no reclaim, no recall, no hierarchy, one shard, explicit return — is unaffected
and its conservation math is sound.** Stages 2–3 must not be built until §16 edits 1–8 land and are
re-verified; Stage 4 additionally needs edits 4, 5, 7.

---

## Part D — High-level API

**Two client surfaces** (design §10–§11):

1. **Holder / node surface** — `client.LeasedBudget()`, the only node-facing API:
   - `Acquire(ctx, key) → *Budget` (opens a local lease cell, subscribes to `BudgetWatch`)
   - `Budget.Spend(n)` — **zero-coordination** in-memory decrement + pacing-token check (no RPC/Raft)
   - `SpendBlocking`, `Remaining`, `Return`, and `Reserve/Commit/Rollback` (all-or-nothing across the
     daily+total budget pair)
2. **Controller / admin surface** — `client.Budget()`:
   - `Define`, `Steer` (cap/rate; cap-cut ⇒ mandatory recall+block), `Recall`
     (invalidate-now + reclaim-after-settle), `Freeze`/`Thaw`, `Stat` (`rollup=true` ⇒ global view),
     `Leases`.

**Server RPCs (`BudgetService`):** controller (`BudgetDefine/Steer/Recall/Freeze/Thaw/Stat/Leases/Watch`)
+ holder-internal (`BudgetDraw` = create-if-absent by `lease_id`; `BudgetReport` = cumulative-per-lease
`max`-fold; `BudgetReturn` = release unspent, **always books spent**). Mutations linearizable through
the owning shard leader; `Stat`/`Leases` bounded-stale by default. Quantities are **int64 micro-units**.
Exhaustion is signalled one way: `ResourceExhausted` + `retry_after_ms`.

**Stage-1 subset only:** `BudgetDefine / BudgetGrant / BudgetReport / BudgetReturn / BudgetStat`
(no Steer/Recall/Freeze/Watch; `Grant` here is the Stage-1 name for the internal `Draw`). In-process
typed API on `Collections`: `BudgetDefine / BudgetGrant / BudgetReport / BudgetReturn / BudgetStat`.

---

## Part E — Recommended path forward

1. **Create the worktree** (isolated, off `main`) via `superpowers:using-git-worktrees`, and copy the
   two source docs into it so refinement happens in isolation:
   - `design/35_leased_budget_datatype.md`
   - `docs/superpowers/plans/2026-06-25-leased-budget-stage1.md`
   (Both currently exist only as uncommitted files in the main worktree; the worktree gets committed copies.)
2. **Confirm `scopeTTLPtr`'s byte** and reconcile the Stage-1 key layout with design §6.1 (gap B1) —
   this is a one-line code check that de-risks the whole roadmap.
3. **Fold gaps B1–B10 into a revised Stage-1 plan** in the worktree (the existing 11 tasks stay; add
   the invariant probe, mode/overflow guards, layout alignment, snapshot test, and the idempotency
   decision). Re-run the `writing-plans` reviewer loop.
4. **Surface the design-level blockers** for Stages 2–4 (the §16 edits) as a checklist gating those
   stages — do not start Stage 2 implementation until they're closed.
5. **Then** execute the refined Stage-1 plan via `superpowers:subagent-driven-development` (TDD,
   per-task spec + quality review).

## Verification

- Stage-1 success = `go test -race ./internal/collections/... ./internal/grpcsrv/...` green, the
  conservation fuzz (Task 11) asserting `available+leasedOut+spent==cap` and `spent≤cap` after every
  op across thousands of randomized interleavings, the new `budCheckQuery` probe, and a snapshot
  round-trip test — all on a real single-shard Raft cluster via the existing `newMgr`/`StartShard`
  harness.
- This planning deliverable is verified by the three completed grounding agents (Part A): every
  load-bearing symbol exists; the only unreal names are the two the plan already flags as placeholders.
