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
`cap == available + leasedOut + spent` — natural expiry settles *directly* (the guard band proves the
holder has already stopped, so there is no in-flight quantity needing a `pendingReclaim` home).

**Success criteria:** an adserver `Acquire`s a budget and `Spend`s millions of times with **zero Raft per
spend**; consensus is touched ~once per refill; a crashed/partitioned/paused holder can **never** cause
STRICT overspend (worst case = underspend); the conservation probe (`budCheckQuery`) holds at every instant
under a combined clock-skew + pause-past-TTL + crash nemesis soak.

---

## 2. The clock-safety model (the heart of Stage 2)

The naïve "grantor reclaims on a bare timer" is unsafe: a holder spending an in-memory lease while the
grantor reclaims the same quantity = the same money live twice (§16 holes H-A/H-B/H-C/H-S1). The fix is an
**asymmetric, replicated-logical-deadline** model:

**Holder stops EARLY, on a single monotonic clock (no cross-domain skew).**
At grant receipt the holder stamps its *own* monotonic clock:
`deadline_local = monotonicReceipt + ttl_ms − self_guard_ms`, with `self_guard ≥ maxClockSkewMs` (500ms
default). It hard-stops spending at `deadline_local`. Only one clock is read for the stop decision, so
cross-domain skew is irrelevant to the holder.

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

**Why this is safe.** Holder stop window ends at `receipt + ttl − self_guard`; grantor reclaim window starts
at `grantedMs + ttl + 3*skew + maxPause`. With `F ≤ self_guard` and `receipt ≈ grantedMs + F`, the gap
between "holder stopped" and "grantor reclaims" is `≥ 3*skew + maxPause − F − self_guard ≥ skew + maxPause > 0`.
The windows are **provably non-overlapping** for skew+pause within budget; self-fencing covers pauses beyond
budget. Lease disjointness is preserved ⇒ the Stage-1 STRICT conservation proof carries over unchanged.

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
elapsedMs   = max(0, grantedMs - lastRefillMs)           // clamp >=0; regressed stamp accrues 0
accrued     = floor(rate * elapsedMs / 1000)             // integer-exact, replica-deterministic
capRemain   = cap - spent
ceil        = min(burst, capRemain)
tokens      = min(ceil, tokens + accrued)
lastRefillMs= max(lastRefillMs, grantedMs)               // monotone forward only
grant       = min(amount, tokens, available)             // STRICT: paced AND capacity-bounded
tokens     -= grant
```
`rate == 0` ⇒ pacing disabled (Stage-1 behavior preserved: `tokens` ignored, grant = `min(amount, available)`).
No wall-clock in apply. The grant is still one Raft entry; idempotent retry returns the existing lease.

### 3.3 `leaseRec` extension (append-only) + timed grant inputs
Append to `leaseRec`: `GrantedMs int64`, `ReclaimNotBeforeMs int64`, `ExpiresMs int64` (the holder-facing
absolute expiry hint = `grantedMs + ttl`). The grant command carries `ttl_ms`, `self_guard_ms`,
`max_pause_budget_ms`. `applyBudGrant` computes `ReclaimNotBeforeMs` per §2 and writes the expiry index
entry. Stage-1 grants (no ttl) are non-expiring (ttl=0 ⇒ no expiry-index entry, no reclaim — unchanged).

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
4. **Settle directly** (3-term, no `pendingReclaim`): `rem = lr.Amount − lr.Spent`;
   `available += rem; leasedOut −= rem` (spent already booked by prior reports). Unreported spend is dropped
   (under-count — safe).
5. In the **same entry**: delete the lease row, delete the expiry-index entry, write the tombstone.

### 3.6 Settlement symmetry for `opBudReturn`
`applyBudReturn` adopts the same terminal-symmetric shape: tombstone-check first (no-op if settled), then
settle + delete row + delete expiry-index entry + write tombstone, all in one entry. A late return after an
expiry is a tombstone no-op; an expiry after a return is a tombstone no-op. (Closes the Stage-1 B5 single-use
window for the *timed* path — a returned/expired lease can no longer be re-granted fresh because the tombstone
is retained ≥ the replay window; the grant idempotency check gains a tombstone branch.)

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
Under the cell lock: (a) **self-fence** — if `now − lastSeenMon > maxPauseNs`, drop `cur`/`next`; set
`lastSeenMon = now`. (b) **pacing gate** — local token bucket: if `tokens < n` → `ErrPacingThrottled`.
(c) drop `cur` if `now ≥ cur.deadlineMon`. (d) if `cur.remaining ≥ n`: decrement, `tokens -= n`, and if below
the low watermark trigger an off-path single-flight refill; return nil. (e) STRICT empty → trigger refill,
return `ErrBudgetUnavailable` (caller serves a no-budget fallback; underspend OK, overspend never).

### 4.2 Refill (single-flight, double-buffer)
At the low watermark, one in-flight `Draw` (stable `leaseID` per logical refill; retries reuse it →
exactly-once with the tombstone) requests the next chunk and reports this lease's cumulative spent. On
success: apply the freshness gate (drop if `recvWall − grantedMs > selfGuard`), install
`next.deadlineMon = nowMono() + ttl − selfGuard`; promote `next→cur` when `cur` drains. Crash waste ≤
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
`max_pause_budget_ms` (e.g. 2000), `dedup_retry_window_ms` (tombstone retention ≥ `maxClockSkew + maxPause`).
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
  `clock-skew-bounded` + `pause/resume past TTL` + holder-crash together; assert `Σ acked spend ≤ cap` at
  every instant. **Negative control:** disable the §2 guard band (or the freshness gate) and confirm the
  checker goes RED — proving it catches the double-spend.

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
3. Dead-holder unreported-spend write-off is **under-count** here (safe); the RELAXED debit policy is Stage 4.
