# LeasedBudget — Stage 2 (Pacing + Expiry/Reclaim + Node Lease Cache) Design

**Status:** Proposed (2026-06-28). Builds on Stage 1 (single-cluster STRICT escrow core, shipped on
branch `leased-budget-refine`). Spec for `design/35_leased_budget_datatype.md` Stage 2, with the
**§16 clock-model fixes baked in** (the design's own re-verification declared the naïve expiry/reclaim
unsafe — this stage closes edits 1–3, 6 of §16.3 for the single-cluster case).

**Owners:** storage-engine team. **Consumers:** vires ad-serving (pacing/budget).

---

## 1. Scope & boundaries

**In scope (single-cluster, STRICT only):**
1. **Token-bucket pacing** on grants (server) — activate the `rate`/`burst` already stored in `poolRec`.
2. **Timed lease expiry + auto-reclaim** (server), hardened against the §16 clock holes.
3. **Node-side lease cache** in **both** SDKs (`sdk/go` and `wavespan-sdk`), implemented **independently**
   in each (no shared module): `Acquire` / `Spend` (zero-coordination) / `Return`, single-flight refill.

**Explicitly deferred (do NOT pull in):**
- Steering / epoch bumps / emergency **recall** + `pendingReclaim` → **Stage 3**.
- **RELAXED** mode, 3-level hierarchy, cross-cluster grant RPC → **Stage 4**.

Because there is **no recall** in Stage 2, conservation stays the simple **3-term** invariant
`cap == available + leasedOut + spent` — the guard band proves a force-expired holder has already stopped, so
there is no *concurrently-spending* in-flight quantity needing a `pendingReclaim` home. Crucially, though, a
force-expired holder is **un-attested** (it may have served impressions it never reported), so forced expiry
**debits the remainder to `spent`** rather than crediting `available` (§3.5); only a holder-attested graceful
Return credits the true remainder (§3.6). The probe (`budCheckQuery`) verifies the *equality*; the §7 soak
separately verifies real **acked** spend ≤ cap (the equality alone cannot — see §7).

**Success criteria:** an adserver `Acquire`s a budget and `Spend`s millions of times with **zero Raft per
spend**; consensus is touched ~once per refill; a crashed/partitioned/paused holder can **never** cause
STRICT overspend (worst case = underspend); the conservation probe (`budCheckQuery`) holds at every instant
under a combined clock-skew + pause-past-TTL + crash nemesis soak.

---

## 2. The clock-safety model (the heart of Stage 2)

The naïve "grantor reclaims on a bare timer" is unsafe: a holder spending an in-memory lease while the
grantor reclaims the same quantity = the same money live twice (§16 holes H-A/H-B/H-C/H-S1). The fix is an
**asymmetric, replicated-logical-deadline** model:

**Holder stops EARLY, on a single SUSPEND-AWARE monotonic clock (no cross-domain skew).**
At grant receipt the holder stamps its *own* monotonic clock:
`deadline_local = monotonicReceipt + ttl_ms − self_guard_ms`, with `self_guard ≥ maxClockSkewMs` (500ms
default). It hard-stops spending at `deadline_local`. Only one clock is read for the stop decision, so
cross-domain skew is irrelevant to the holder.

**LOAD-BEARING (C1): `nowMono()` MUST be suspend-inclusive — `CLOCK_BOOTTIME`, not `CLOCK_MONOTONIC`.**
Go's monotonic clock (what `time.Now()`'s monotonic reading and `time.Since` use) is `CLOCK_MONOTONIC`,
which **freezes during VM suspend / host sleep**; only `CLOCK_BOOTTIME` counts suspended time. If the holder
used the frozen clock, a VM-migrate longer than the guard band would leave both `now − lastSeenMon > maxPause`
and `now ≥ deadline_local` un-fired on resume → it resumes spending a lease the grantor already reclaimed
(re-opens H-B/H-S1, same money live twice). So `nowMono()` reads `CLOCK_BOOTTIME` via syscall
(`unix.ClockGettime(unix.CLOCK_BOOTTIME, …)` on Linux; the platform suspend-aware equivalent elsewhere) — NOT
`time.Now()`. This is a load-bearing assumption of the proof below; the §7 nemesis injects a real suspend that
freezes `CLOCK_MONOTONIC` to prove the fence fires.

**Grantor reclaims LATE, on replicated logical time (survives leader change).** (§16 edit #1)
At grant, the leader stamps `grantedMs` (pre-propose, deterministic) and the apply computes & **replicates**
into the lease row:
```
reclaimNotBeforeMs = grantedMs + ttl_ms + 3*maxClockSkewMs + maxPauseBudgetMs
```
The expiry sweep may settle a lease only when the leader-stamped `sweepNowMs ≥ reclaimNotBeforeMs`. Because
`reclaimNotBeforeMs` is a value in the replicated log (not a live clock), a *new* leader after re-election —
with a different wall clock — uses the same threshold. (Retracts §16's "exact copy of the wall-clock TTL
sweeper" claim.)

**Holder freshness gate (bound in-flight time F).** (§16 edit #2)
The node rejects a grant whose transit was too long: if `localRecvWall − grantedMs > self_guard_ms`, it
drops the grant and re-draws. This bounds `F ≤ self_guard`, so anchoring `deadline_local` on the holder's
receipt is sound.

**Self-fence beyond the pause budget.** (§16 edits #6/#B)
The holder re-reads its monotonic clock **under the cell lock, immediately before the decrement**. If the
observed gap since the last spend exceeds `maxPauseBudgetMs` (VM migrate / long GC), or `now ≥ deadline_local`,
it treats the lease as presumed-dead, drops `cur`/`next`, and re-draws — *before* serving.

**Why this is safe.** Three clocks are involved (grant-leader, reclaim-leader, holder), each pairwise offset
bounded by `skew`. The holder stops by, in grant-leader time, `grantedMs + F + ttl − self_guard` where the
freshness gate bounds `F ≤ self_guard` (and a receiver clock *behind* the leader only makes the measured
transit smaller — the gate is one-sided, so it never falsely rejects). The grantor cannot reclaim before, in
grant-leader time, `grantedMs + ttl + 3*skew + maxPause` minus up to `skew` of reclaim-leader offset. The gap
between "holder has stopped" and "earliest possible reclaim" is therefore
`≥ (ttl + 3*skew + maxPause − skew) − (ttl + F − self_guard) = 2*skew + maxPause + self_guard − F ≥ 2*skew + maxPause > 0`
(using `F ≤ self_guard`). The windows are **provably non-overlapping** for skew+pause within budget;
self-fencing (on the suspend-aware clock) covers pauses beyond budget. The `3*skew` term (vs `2*skew`) is
exactly what absorbs the third clock a leader-change introduces (§16 H-S1): because the reclaim threshold is
the **replicated** `reclaimNotBeforeMs` — not a live clock and not recomputed from the split-copied
`GrantedMs` — a new leader with a different wall clock uses the identical threshold, so leader change / split
(which copies lease bytes verbatim, `migrate.go`) cannot shorten the guard. Lease disjointness is preserved ⇒
the Stage-1 STRICT conservation proof carries over unchanged.

---

## 3. Server-side changes

### 3.1 `poolRec` extension (append-only — migration-free)
Append two fields to the combined pool record (the Stage-1 `decodePool` already tolerates trailing bytes):
`lastRefillMs int64`, `tokens int64` (micro-unit token bucket, integer-exact). `decodePool` returns them as
0 for Stage-1 records (no pacing) — correct default.

### 3.2 Token-bucket pacing in `applyBudGrant`
Grant is gated by **both** capacity and accrued pace tokens, using the **leader-stamped `grantedMs`**
(carried in the command, stamped pre-propose like `SAddTTL`):
```
if lastRefillMs == 0 { lastRefillMs = grantedMs }        // I3: lazy-init — first paced grant accrues 0,
                                                         //     never elapsed≈grantedMs (epoch) overflow
elapsedMs   = clamp(grantedMs - lastRefillMs, 0, maxElapsedMs)  // clamp >=0 AND <= a ceiling so
                                                         //     rate*elapsedMs cannot overflow int64
accrued     = rate * elapsedMs / 1000                    // integer floor, replica-deterministic; with the
                                                         //     clamp, the product is bounded well under 2^63
capRemain   = cap - spent
ceil        = min(burst, capRemain)
tokens      = min(ceil, tokens + accrued)                // also clamped >= 0 defensively
lastRefillMs= max(lastRefillMs, grantedMs)               // monotone forward only
grant       = min(amount, tokens, available)             // STRICT: paced AND capacity-bounded
tokens     -= grant
```
`maxElapsedMs` is chosen so `rate*maxElapsedMs` stays < 2^62 for any admissible `rate` (e.g. cap accrual to a
full `burst` needs only `burst/rate` seconds; clamp to a few multiples of that). `rate == 0` ⇒ pacing disabled
(Stage-1 behavior preserved: `tokens` ignored, grant = `min(amount, available)`). No wall-clock in apply. The
grant is still one Raft entry; idempotent retry returns the existing lease.

**Two buckets, distinct roles (I5 — avoid double-pacing undershoot).** The **node** bucket (§4.1) is the
**authoritative smoother** that shapes per-impression delivery at rate `R`. The **server** bucket here is a
**coarse anti-hoard cap** that stops any one cluster/node from draining the pool faster than it can deliver —
it must NOT also bind at exactly `R` or aggregate delivery would systematically undershoot `R` (a stated
success criterion). Sizing: `server_burst ≥ node_chunk` and `server_rate ≥ Σ(node rates)` with headroom, so
the server bucket only bites under genuine abuse, never during normal paced delivery.

### 3.3 `leaseRec` extension (append-only) + timed grant inputs
Append to `leaseRec`: `GrantedMs int64`, `ReclaimNotBeforeMs int64`, `ExpiresMs int64`. **Append-tolerance
detail:** Stage-1 `leaseRec` ends with a length-prefixed `holder` chunk, so the new fixed fields MUST be
encoded **after** the holder chunk (encoding them before would shift the holder offset and break Stage-1
decode). `decodeLease` reads them from the trailing `rest` after `takeChunk(holder)`, **defaulting to 0 when
`rest` is short** (a Stage-1 record) — the same append-only contract as `decodePool`.

`ExpiresMs` (= `grantedMs + ttl`) is **informational only** (M2): it is surfaced to operators/holders as a
human-readable absolute expiry, and **MUST NOT** feed any stop or reclaim decision. The holder stops on its own
suspend-aware `deadline_local`; the sweep reclaims on `ReclaimNotBeforeMs`. (If this footgun isn't worth the
field, drop it — nothing depends on it.)

The grant command carries `ttl_ms`, `self_guard_ms`, `max_pause_budget_ms`. `applyBudGrant` computes
`ReclaimNotBeforeMs` per §2 and writes the expiry-index entry keyed on it. Stage-1 grants (no ttl) are
non-expiring (ttl=0 ⇒ no expiry-index entry, no reclaim — unchanged).

### 3.4 New sub-scopes (reserved in Stage 1)
```
scopeBudExp  byte = 0x07 // be(reclaimNotBeforeMs) | <leaseID> -> empty   (reclaim-ordered sweep index)
scopeBudTomb byte = 0x08 // <leaseID> -> settled tombstone {finalSpent, reason}  (single-shot settlement)
```
Mirror `ttl.go`'s due-ordered index + the existing `scanDue` pattern.

### 3.5 `opBudExpire` + a second pass in `sweepOnce`
A `budExpiryDueQuery{NowMs, Limit}` Lookup scans `scopeBudExp` for `be(reclaimNotBeforeMs) ≤ sweepNowMs`
(exact copy of TTL `scanDue`). The leader (already gated in `sweepOnce`) stamps `sweepNowMs` and proposes
`opBudExpire(leaseID, sweepNowMs)` per due lease. `applyBudExpire`:
1. **Tombstone check first** — if a tombstone exists, no-op (idempotent, §16 edit #3 terminal-symmetric).
2. Re-read the lease; if absent (already returned) → write/keep tombstone, delete the expiry-index entry, done.
3. Re-check `sweepNowMs ≥ lr.ReclaimNotBeforeMs` (a renewed/re-drawn lease has a later value → skip).
4. **Settle by DEBIT, not credit (C2 / §16 edit #9 — the money-safety crux).** A force-expired holder is
   un-attested: it may have *served impressions it never reported* (the node serves-then-reports, and a crash
   or self-fence drops the lease without a final report — §4). Its last cumulative report is therefore a
   **lower bound** on real spend, not the truth. Crediting the unreported remainder back to `available` would
   re-grant already-consumed money → real Σspend > cap (an overspend the conservation probe cannot see,
   because it sums the pool's own under-counted `spent`). So forced expiry treats the **entire** outstanding
   remainder as consumed:
   ```
   rem = lr.Amount − lr.Spent
   spent     += rem        // book it as spent (pessimistic: assume it was all served)
   leasedOut −= rem
   available += 0          // NOTHING returns to available on a forced expiry
   ```
   The genuinely-unspent portion is now *stranded* (counted consumed though it wasn't) ⇒ bounded **underspend**
   — the safe direction. INV-LOCAL still holds exactly (`rem` moved leasedOut→spent). This is the opposite of a
   graceful Return (§3.6), where the holder's authoritative final report *does* let the true remainder return to
   `available`. Liveness cost: forced expiry strands up to one chunk per dead holder until a fresh `Define`/day
   rollover; acceptable for STRICT money (underspend, never overspend).
5. In the **same entry**: delete the lease row, delete the expiry-index entry, write the tombstone.

### 3.6 Settlement symmetry for `opBudReturn`
`applyBudReturn` adopts the same terminal-symmetric shape: tombstone-check first (no-op if settled), then
settle + delete row + delete expiry-index entry + write tombstone, all in one entry. **A graceful Return is
holder-ATTESTED** — the holder folds its authoritative final cumulative spent and explicitly relinquishes the
lease — so it is safe to **credit** the true remainder back: `rem = lr.Amount − finalSpent;
available += rem; leasedOut −= rem`. This is the *only* settlement path that returns quantity to `available`;
forced expiry (§3.5, un-attested) debits instead. A late return after an expiry is a tombstone no-op; an
expiry after a return is a tombstone no-op. (Closes the Stage-1 B5 single-use window for the *timed* path — a
returned/expired lease can no longer be re-granted fresh because the tombstone is retained ≥ the replay
window; the grant idempotency check gains a tombstone branch.)

### 3.7 Grant idempotency gains a tombstone branch
`applyBudGrant` step 0: if a tombstone exists for `leaseID` → return "already settled" (never re-grant).
This is the Stage-2 closure of the Stage-1 B5 hazard for any lease that has a ttl.

### 3.8 Conservation probe unchanged
`budCheckQuery` still asserts `available + leasedOut + spent == cap` and `spent ≤ cap`. Pacing and expiry
preserve all three. The fuzz/harness assert it after every op including expiry settlements.

---

## 4. Node-side lease cache (both SDKs, independent)

Lives in each SDK as a `LeasedBudgetClient` (node surface) distinct from the Stage-1 `BudgetClient`
(controller surface). API:
```
func (c *Client) LeasedBudget() *LeasedBudgetClient
func (lb *LeasedBudgetClient) Acquire(ctx, key BudgetKey, opts ...) (*Budget, error)
func (b *Budget) Spend(n int64) error               // zero-coordination fast path
func (b *Budget) Remaining() int64
func (b *Budget) Return(ctx) error                  // graceful shutdown: return unspent
```

### 4.1 `Spend` fast path (no RPC, no Raft)
`now := nowMono()` reads the **suspend-aware** clock (`CLOCK_BOOTTIME`, §2 C1) — re-read **under the lock,
immediately before the decrement** (§16 #6/#B; not latched earlier). Under the cell lock:
(a) **self-fence** — if `lastSeenMon != 0 && now − lastSeenMon > maxPauseNs`, the lease may have been
reclaimed during a suspend/pause: fire a **best-effort async final report** of the lease's cumulative spent
(so the non-crash case recovers its real spend instead of being force-debited), then drop `cur`/`next`; set
`lastSeenMon = now`. (b) **pacing gate** — local token bucket: if `tokens < n` → `ErrPacingThrottled`.
(c) drop `cur` if `now ≥ cur.deadlineMon`. (d) if `cur.remaining ≥ n`: decrement, `tokens -= n`, and if below
the low watermark trigger an off-path single-flight refill; return nil. (e) STRICT empty → trigger refill,
return `ErrBudgetUnavailable` (caller serves a no-budget fallback; underspend OK, overspend never).

Note the report-before-drop is **best-effort only** — it is NOT relied on for safety (a hard crash skips it).
Safety rests entirely on forced expiry's debit (§3.5); the report-before-drop merely narrows the stranded-
underspend window in the common (survived-the-pause) case.

### 4.2 Refill (single-flight, double-buffer)
At the low watermark, one in-flight `Draw` (stable `leaseID` per logical refill; retries reuse it →
exactly-once with the tombstone) requests the next chunk and reports this lease's cumulative spent. The grant
result echoes `granted_ms`, `ttl_ms`, `self_guard_ms`, **and `max_pause_budget_ms`** (M4 — the cell needs all
four: the first three to run the freshness gate + stamp the deadline, and `max_pause_budget_ms` to set
`maxPauseNs` for the self-fence). On success: apply the freshness gate (drop if `recvWall − grantedMs >
selfGuard`), set `cell.maxPauseNs = max_pause_budget_ms`, install
`next.deadlineMon = nowMono() + ttl − selfGuard` (suspend-aware clock); promote `next→cur` when `cur` drains. Crash waste ≤
`cur.remaining + next.remaining ≤ 2·chunk`; reclaim latency ≤ `ttl + guard`. Refill RPC rate ≈
`1/refillInterval` per active budget per node — **independent of impression QPS** (the write-ceiling break).

### 4.3 Routing difference (per the two-SDK analysis)
`sdk/go` routes the refill `Draw` direct-to-leader via its `shardRouter`; `wavespan-sdk` uses the shared conn
+ server-side forwarding. Both correct; `sdk/go` has lower refill latency. The cache logic is otherwise
identical and maintained independently in each.

---

## 5. Proto additions (backward-compatible)
Extend `BudgetGrantRequest` with optional `ttl_ms`, `self_guard_ms`, `max_pause_budget_ms` (0 ⇒ Stage-1
non-expiring grant). Extend `BudgetGrantResult` with `ttl_ms`, `self_guard_ms`, `max_pause_budget_ms`,
`granted_ms` echoed back so the holder can run the freshness gate and stamp its monotonic deadline. No new
RPCs (the node cache uses the existing `BudgetGrant`/`BudgetReport`/`BudgetReturn`). Regenerate stubs in the
server repo **and** both SDK gens (Go-only; pin `protoc-gen-go@v1.36.11`; zero drift in unrelated stubs).

---

## 6. Config / parameters
On `BudgetDefine`/grant: `default_lease_ttl_ms` (seconds-scale), `self_guard_ms` (≥ `maxClockSkewMs`=500),
`max_pause_budget_ms` (e.g. 2000), `dedup_retry_window_ms`.

**Tombstone retention (I4) — sized by the TRANSPORT replay window, not the clock.** A settled lease's
tombstone must outlive the longest possible *retried* `Draw` for that `leaseID`, or a retry arriving after the
row is gone re-grants fresh money (reopens B5/H1). Replay latency is an RPC property (client retry budget +
queueing), NOT a clock property — so retention `≥ max(RPC_retry_budget, skew + maxPause)`, i.e. the
design-§6.1 ~30s order, **not** the ~2.5s `skew+maxPause`. Make it `dedup_retry_window_ms`, default 30s,
admission-validated `≥ maxClockSkew + maxPause`.

**Tombstone GC (M3).** Tombstones are swept by a `ttl.go`-style due-index entry at
`reclaimNotBeforeMs + dedup_retry_window_ms` (reuse the existing TTL sweeper machinery), so they don't
accumulate unboundedly.

Lease sizing: `chunk = clamp(EWMA(node spend rate) · ttl, minChunk, maxChunk)` (auto-tune; STRICT biases
smaller to bound stranded underspend).

---

## 7. Testing & correctness harness
- **Server unit/fuzz:** extend the conservation fuzz to interleave timed grants + expiry sweeps + returns;
  assert `budCheck.OK` after every op. Pacing math determinism test (leader-stamped time). Tombstone
  idempotency (expire↔return in both orders → single settlement).
- **Node cache:** deterministic-clock tests for the self-fence (inject a pause past budget mid-Spend → lease
  dropped, no spend served), the freshness gate (stale grant rejected), double-buffer promotion, single-flight.
- **Combined nemesis soak (the §16 mandate):** run a `budget-strict-cap` workload with
  `clock-skew-bounded` + `pause/resume past TTL` + holder-crash together. **The checker MUST assert
  `Σ (externally-ACKED impressions × price) ≤ cap`** — ground truth tracked by the harness, NOT the pool's
  internal `spent` field. This distinction is load-bearing: forced expiry's debit (§3.5) can *over*-count
  internal `spent` (safe), and an overspend bug *under*-counts it (the budCheckQuery equality stays green
  while real money exceeds cap, C2). Only an external acked-spend ledger catches C2.
- **Suspend nemesis (C1):** inject a real pause that **freezes `CLOCK_MONOTONIC` while `CLOCK_BOOTTIME`
  advances** past the guard band (simulate VM suspend), and assert the holder self-fences (drops its lease,
  serves nothing) on resume. A holder built on `time.Now()` monotonic would FAIL this — that's the point.
- **Tombstone idempotency:** expire-then-return and return-then-expire on the same lease both yield exactly
  one settlement; a retried `Draw` after settlement returns "already settled", never a fresh grant.
- **Negative controls (must go RED):** (a) disable the §2 guard band / freshness gate → acked-spend checker
  red; (b) change forced expiry from debit back to credit (§3.5) → acked-spend checker red (proves the soak
  actually catches C2); (c) use `CLOCK_MONOTONIC` instead of `CLOCK_BOOTTIME` under the suspend nemesis → red.

---

## 8. Staging within Stage 2
- **2a — server pacing:** `poolRec` fields + token-bucket gate in `applyBudGrant` (leader-stamped). Tests.
- **2b — server expiry/reclaim:** `leaseRec` fields, `scopeBudExp`/`scopeBudTomb`, `opBudExpire`,
  `sweepOnce` second pass, settlement symmetry + tombstone branch in grant/return. Tests incl. tombstone
  idempotency.
- **2c — node cache in `sdk/go`:** `LeasedBudgetClient`/`Budget` with the §2 holder model; deterministic-clock
  tests.
- **2d — node cache in `wavespan-sdk`:** independent implementation, same model.
- **2e — combined nemesis soak + negative control** (correctness harness).

Each sub-stage is independently testable. 2a/2b land first (server is money-authoritative); 2c/2d depend on
2b's grant timing fields; 2e gates the stage.

## 9. Open questions
1. `maxClockSkewMs=500` is the intra-mesh HLC threshold; is it a safe single bound for the guard here, or do
   we want a configurable per-budget skew? (Stage 4 cross-region needs per-edge; Stage 2 single-cluster can
   use the mesh value.)
2. Auto-tune EWMA location — node-local vs broker-reported (broker is Stage 3); Stage 2 uses node-local.
3. (Resolved — was the C2 bug.) Forced expiry **debits** the full outstanding remainder to `spent`
   (§3.5), stranding the genuinely-unspent portion as bounded underspend — the safe direction. The only
   way real budget returns to `available` is a holder-attested graceful Return (§3.6). Open sub-question:
   how aggressively to recover stranded budget (e.g. a periodic reconciliation that re-credits a budget back
   to its true `Σ acked` at day rollover) — deferred; Stage 2 accepts the stranding.
