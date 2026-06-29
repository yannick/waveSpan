# LeasedBudget — Stage 2 (Pacing + Expiry/Reclaim + Node Cache) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or
> superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add token-bucket pacing, §16-hardened timed lease expiry/auto-reclaim, and a node-side lease cache
(in BOTH SDKs) to the single-cluster STRICT `LeasedBudget` datatype shipped in Stage 1 — so an adserver
`Acquire`s a budget and `Spend`s with zero Raft per spend, and a crashed/paused/partitioned holder can never
cause STRICT overspend.

**Architecture:** Extends the Stage-1 `typeBudget` datatype in `internal/collections`. Pacing and expiry are
server-side, deterministic (leader-stamped time, never `time.Now()` in apply). Expiry uses a **replicated
logical deadline** (`reclaimNotBeforeMs`) and **debits** the remainder on forced expiry (un-attested holder),
while graceful Return **credits** (attested). The node cache is an in-memory token bucket on a **suspend-aware
(`CLOCK_BOOTTIME`)** monotonic clock with a self-fence; it touches Raft ~once per refill. Conservation stays
**3-term** (`cap == available + leasedOut + spent`) — no recall/`pendingReclaim` (Stage 3).

**Tech Stack:** Go 1.26, dragonboat Raft, `wavesdb` LSM via `storage.LocalStore`, Connect/grpc-go (buf
codegen), `golang.org/x/sys/unix` for `CLOCK_BOOTTIME`.

**Spec:** `docs/superpowers/specs/2026-06-28-leased-budget-stage2-design.md` (read it — section refs `§N` below
point at it). This plan implements that spec's sub-stages **2a–2e**. Stage 3 (steering/recall/epoch) and
Stage 4 (RELAXED/hierarchy/cross-cluster) are OUT OF SCOPE — do not pull them in.

**Verified Stage-1 anchors** (grep to confirm; line numbers are hints): `poolRec`/`decodePool` (append-tolerant)
+ scope bytes `scopeBudPool 0x05`/`scopeBudLease 0x06` + reserved `0x07`/`0x08` in `budget.go`; `applyBudGrant`/
`applyBudReport`/`applyBudReturn` + `budCheckQuery`/`budCheck` in `budget.go`; the budget dispatch block +
`applySingleForTest` in `statemachine.go`; the TTL sweeper `sweepOnce` (leader-gated, stamps
`time.Now().UnixMilli()` pre-propose) + `coalescable()` in `manager.go`; `scanDue`/`clearTTL`/the due-index
layout in `ttl.go`; `SAddTTL` stamps `expiry := time.Now().UnixMilli()+ttlMs` pre-propose in `collections.go`;
the Stage-1 budget client `BudgetClient` in `sdk/go/budget.go` and `wavespan-sdk/budget.go`.

---

## Scope check

Five independently-testable sub-stages, server-first (the server is money-authoritative; the node caches
depend on the grant carrying timing fields). Each sub-stage ends green. Do NOT start 2c/2d before 2b lands
(the node cache needs the grant-result timing echo from 2b).

## Conservation model (every task must preserve it)

```
INV-LOCAL (3-term, STRICT):   cap == available + leasedOut + spent
  Grant(g):   g=min(req, tokens, available); available-=g; leasedOut+=g; lease{amount:g, spent:0}
  Report(d):  d=max(0, reportedCumulative - lease.spent) clamped to amount; leasedOut-=d; spent+=d
  Return:     ATTESTED -> rem=amount-finalSpent; available+=rem; leasedOut-=rem    (credit)
  Expire:     UN-ATTESTED -> rem=amount-spent; spent+=rem; leasedOut-=rem; available+=0  (DEBIT, §3.5)
```
The probe (`budCheckQuery`) verifies the equality; the §7 soak separately verifies external **acked** spend ≤
cap (the equality alone cannot catch a credit-instead-of-debit overspend — that is the C2 bug).

## File structure

| File | Responsibility | Action |
|---|---|---|
| `internal/collections/budget.go` | poolRec/leaseRec field appends; pacing math; `applyBudExpire`; tombstone codec + scopes; settlement symmetry; `budExpiryDueQuery` | Modify |
| `internal/collections/command.go` | `opBudExpire` opKind; grant-command timing fields; any new sentinels/errors | Modify |
| `internal/collections/collections.go` | `BudgetGrant` stamps `grantedMs` + carries ttl/selfGuard/maxPause; `BudgetDefine` seeds tokens/rate/burst | Modify |
| `internal/collections/manager.go` | second pass in `sweepOnce` proposing `opBudExpire` (+ tombstone GC) | Modify |
| `internal/collections/statemachine.go` | dispatch `opBudExpire`; `budExpiryDueQuery` Lookup | Modify |
| `internal/collections/budget_test.go` | pacing, expiry/debit, tombstone idempotency, conservation fuzz (timed) | Modify |
| `proto/wavespan/v1/budget.proto` | extend `BudgetGrantRequest`/`Result` with ttl/self_guard/max_pause/granted_ms | Modify |
| `internal/collections/service.go`, `internal/grpcsrv/budget.go` | thread the new grant fields | Modify |
| `sdk/go/leasedbudget.go` (new) + `sdk/go/leasedbudget_clock_linux.go` (+ `_other.go`) | node cache: `LeasedBudgetClient`/`Budget`, `nowMono()` boottime | Create |
| `wavespan-sdk/leasedbudget.go` (new) + clock files | node cache (independent, mirrors sdk/go) | Create |
| `tests/harness/workloads/budget/*`, `checker/budget.go` | §7 soak + suspend nemesis + negative controls | Create |

---

# Sub-stage 2a — Server token-bucket pacing

## Task 2a.1: `poolRec` pacing fields + init seeding

**Files:** Modify `internal/collections/budget.go`, `internal/collections/budget_test.go`.

- [ ] **Step 1: Failing test** — append `LastRefillMs int64` and `Tokens int64` to `poolRec`; assert the
  extended record round-trips AND that a Stage-1 (57-byte) record still decodes with the two new fields = 0.

```go
func TestPoolRecPacingFieldsRoundTripAndBackCompat(t *testing.T) {
	p := poolRec{Cap: 1000, Available: 400, LeasedOut: 600, Epoch: 1, Mode: modeStrict, Rate: 50, Burst: 100, LastRefillMs: 123456, Tokens: 77}
	got, err := decodePool(encodePool(p))
	if err != nil || got != p {
		t.Fatalf("round-trip = %+v err=%v, want %+v", got, err, p)
	}
	// a Stage-1 record (57 bytes, no pacing tail) decodes with LastRefillMs=0, Tokens=0
	old := encodePoolStage1(poolRec{Cap: 1000, Available: 1000, Epoch: 1, Mode: modeStrict, Burst: 1000}) // 57-byte writer
	g2, err := decodePool(old)
	if err != nil || g2.LastRefillMs != 0 || g2.Tokens != 0 {
		t.Fatalf("stage-1 record back-compat: %+v err=%v", g2, err)
	}
}
```
(Add a tiny `encodePoolStage1` test-only helper that writes exactly the original 57 bytes, to prove the
append-tolerance contract.)

- [ ] **Step 2: Run — FAIL** (`go test ./internal/collections/ -run TestPoolRecPacingFields`): unknown fields.

- [ ] **Step 3: Implement.** Append the two fields to `poolRec`; extend `encodePool` to write 8+8 more bytes
  (total 73); keep `decodePool`'s `len(b) < 57` floor and read the tail **only if present**:
```go
// in decodePool, after the fixed 57-byte prefix:
if len(b) >= 73 {
	p.LastRefillMs = getI64(b[57:])
	p.Tokens = getI64(b[65:])
}
```
Seed at init: in `applyBudInit`, after validation, set `Tokens: burst` when `rate > 0` else `0`, and
`LastRefillMs: 0` (lazy-init, 2a.2). (§3.1 M-3.)

- [ ] **Step 4: Run — PASS.**
- [ ] **Step 5: Commit** `feat(collections): poolRec pacing fields (lastRefillMs, tokens), append-tolerant`.

## Task 2a.2: leader-stamped `grantedMs` on the grant command

**Files:** Modify `command.go`, `collections.go`, `budget.go`, `budget_test.go`.

The grant must carry a leader-stamped `grantedMs` (for pacing, always) plus optional `ttl_ms`/`self_guard_ms`/
`max_pause_budget_ms` (for 2b expiry). **Grant `Items[0].Val` layout changes** to keep `holder` as the trailing
rest while adding fixed fields before it:
```
Val = amount(8 BE) | grantedMs(8) | ttl(8) | selfGuard(8) | maxPause(8) | holder(rest)
```
This is a breaking change to the Stage-1 grant encoding — update the `grantCmd` test builder and
`Collections.BudgetGrant` together. `grantedMs` is always stamped (pacing needs it even when ttl=0).

- [ ] **Step 1: Failing test** — extend the `grantCmd` builder to take `grantedMs, ttl int64` (selfGuard/maxPause
  default 0 for pacing-only tests) and write the new layout; add `TestApplyBudGrantPaced`:
```go
func TestApplyBudGrantPaced(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	// cap 1000, rate 100 u/s, burst 100 -> seeded tokens=100
	_, _ = u.applyBudInit(initCmdPaced(ns, coll, 1000, 100, 100))
	// first paced grant at grantedMs=1000: lazy-init lastRefillMs=1000, accrued=0, tokens=100 (seeded burst)
	r := mustGrant(t, u, ns, coll, "A", 80, /*grantedMs*/1000, /*ttl*/0)
	if decodeGrant(r.Data) != 80 { t.Fatalf("paced grant=%d want 80 (from seeded burst)", decodeGrant(r.Data)) }
	// tokens now 20; a 50-draw saturates to 20 even though available is huge
	r2 := mustGrant(t, u, ns, coll, "B", 50, 1000, 0)
	if decodeGrant(r2.Data) != 20 { t.Fatalf("paced grant=%d want 20 (token-bound)", decodeGrant(r2.Data)) }
	// 1s later: +100 tokens accrued, capped at burst=100
	r3 := mustGrant(t, u, ns, coll, "C", 100, 2000, 0)
	if decodeGrant(r3.Data) != 100 { t.Fatalf("after 1s grant=%d want 100", decodeGrant(r3.Data)) }
}
```
Add `TestApplyBudGrantRateZeroUnchanged` (rate=0 ⇒ Stage-1 behavior: grant=min(amount,available), tokens
ignored) and `TestPacingNoOverflow` (huge rate + lazy-init: first grant on `lastRefillMs=0` accrues 0, never
`rate*~1.7e12`).

- [ ] **Step 2: Run — FAIL.**
- [ ] **Step 3: Implement.** (a) `Collections.BudgetGrant` gains `ttlMs, selfGuardMs, maxPauseMs int64` params
  and stamps `grantedMs := time.Now().UnixMilli()` pre-propose (mirror `SAddTTL`/`collections.go`), writing the
  new Val layout. (b) `applyBudGrant` parses the fixed fields, runs the §3.2 pacing math when `rate > 0`:
```go
// after the existing amount<0 / idempotency / pool-found guards, before computing grant:
if p.Rate > 0 {
	if p.LastRefillMs == 0 { p.LastRefillMs = grantedMs } // lazy-init (M-3): first grant accrues 0
	maxElapsed := pacingMaxElapsedMs(p.Rate, p.Burst)      // per-budget, rate-independent product bound (M-2)
	elapsed := clampI64(grantedMs-p.LastRefillMs, 0, maxElapsed)
	accrued := p.Rate * elapsed / 1000                     // integer floor, deterministic
	ceil := min64(p.Burst, p.Cap-p.Spent)
	p.Tokens = min64(ceil, p.Tokens+accrued)
	if p.Tokens < 0 { p.Tokens = 0 }
	if grantedMs > p.LastRefillMs { p.LastRefillMs = grantedMs }
}
grant := amount
if p.Rate > 0 { grant = min64(grant, p.Tokens) }
if grant > p.Available { grant = p.Available }
if grant <= 0 { return ProposeResult{Data: budNoCapacity}, nil }
... // existing available-=grant; leasedOut+=grant; setPool; setLease
if p.Rate > 0 { p.Tokens -= grant } // (fold into the setPool write)
```
`pacingMaxElapsedMs(rate, burst) = k * (burst/rate) * 1000` with `k=4`; admission (Task 2a.3) bounds `burst`.

- [ ] **Step 4: Run — PASS** (all pacing tests + existing Stage-1 grant tests, after updating `grantCmd`).
- [ ] **Step 5: Commit** `feat(collections): token-bucket pacing on grant (leader-stamped grantedMs)`.

## Task 2a.3: admission guards + `BudgetDefine` rate/burst

**Files:** Modify `collections.go`, `budget.go`, `budget_test.go`.

- [ ] Extend `BudgetDefine` to accept `rate, burst int64` (Stage-1 callers pass 0/cap). Reject in `applyBudInit`
  (sentinel `budBadMode` or a new `budBadParam`): `burst > pacingBurstCeil` (so `pacingMaxElapsedMs` cannot
  overflow), `rate < 0`, `burst < 0`. Test `TestBudgetDefineRejectsOverflowBurst`. Commit.

---

# Sub-stage 2b — Server timed expiry + auto-reclaim (the §16-hardened core)

## Task 2b.1: `leaseRec` timing fields (append-tolerant after holder)

**Files:** Modify `budget.go`, `budget_test.go`.

- [ ] **Step 1: Failing test** — append `GrantedMs, ReclaimNotBeforeMs, ExpiresMs int64` to `leaseRec`,
  encoded **after** the trailing holder chunk; assert round-trip AND that a Stage-1 lease (no tail) decodes
  with the three = 0.
- [ ] **Step 3: Implement.** In `encodeLease`, after `appendChunk(holder)`, append the three int64s. In
  `decodeLease`, after `takeChunk(holder)`, read them from `rest` **iff `len(rest) >= 24`** (else 0). (§3.3.)
- [ ] Commit `feat(collections): leaseRec timing fields (append-tolerant)`.

## Task 2b.2: grant writes `ReclaimNotBeforeMs` + expiry index (`scopeBudExp 0x07`)

**Files:** Modify `budget.go`, `budget_test.go`.

- [ ] When the grant carries `ttl > 0`, `applyBudGrant` computes
  `reclaimNotBefore = grantedMs + ttl + 3*maxClockSkewMs + maxPause` (§2), stores `GrantedMs/ReclaimNotBeforeMs/
  ExpiresMs(=grantedMs+ttl)` on the lease, and writes a `scopeBudExp` index entry keyed
  `be(reclaimNotBeforeMs)|leaseID -> empty` (mirror `ttl.go` `addTTL`). `ttl == 0` ⇒ no index entry
  (non-expiring; Stage-1 behavior). Add `budExpKey(ns,coll,reclaimMs,leaseID)` + helpers. Test
  `TestGrantWritesExpiryIndex` (scan `scopeBudExp`, assert the entry + the stored `ReclaimNotBeforeMs`).
- [ ] Commit `feat(collections): timed grant writes reclaim deadline + expiry index`.

## Task 2b.3: tombstone scope (`scopeBudTomb 0x08`) + grant tombstone branch + settlement symmetry

**Files:** Modify `budget.go`, `budget_test.go`.

- [ ] Add `tombRec{FinalSpent int64; Reason byte}` (`reasonReturn`/`reasonExpire`) + `encodeTomb`/`decodeTomb`
  + `budTombKey`. Add `getTomb`/`setTomb` overlay helpers.
- [ ] `applyBudGrant` **step 0**: if a tombstone exists for `leaseID` → return `budSettled` sentinel
  ("already settled", never re-grant). (§3.7 — closes B5 for timed leases.)
- [ ] Refactor `applyBudReturn` to the terminal-symmetric shape (§3.6): tombstone-check first (no-op if
  present); fold final report (credit `available += amount-finalSpent`); then **same entry**: delete lease
  row, delete the `scopeBudExp` entry (use the lease's stored `ReclaimNotBeforeMs`), write the tombstone.
- [ ] Tests: `TestReturnWritesTombstoneAndCredits`, `TestGrantAfterReturnSettledNoRegrant` (timed lease: grant
  w/ttl → return → re-grant same id → `budSettled`, NOT fresh). Commit.

## Task 2b.4: `opBudExpire` (DEBIT settlement) + `budExpiryDueQuery` + dispatch

**Files:** Modify `command.go` (`opBudExpire opKind = 20`; `typeForOp`→typeBudget), `budget.go`, `statemachine.go`,
`budget_test.go`.

- [ ] **Step 1: Failing tests** — the money-critical ones:
```go
func TestApplyBudExpireDebits(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	// grant 100 w/ ttl; report 50; then expire
	mustGrant(t, u, ns, coll, "L", 100, 1000, 3000)       // grantedMs=1000 ttl=3000
	_, _ = u.applyBudReport(reportCmd(ns, coll, "L", 50))
	// expire at a sweepNow past reclaimNotBefore
	r, _ := u.applyBudExpire(expireCmd(ns, coll, "L", /*sweepNow*/9_999_999))
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	// DEBIT: the unreported 50 is booked as spent, NOT returned to available
	if p.Spent != 100 || p.Available != 900 || p.LeasedOut != 0 {
		t.Fatalf("expire debit: spent=%d avail=%d leased=%d want 100/900/0", p.Spent, p.Available, p.LeasedOut)
	}
	// tombstone written; expiry-index entry gone; lease row gone
	if _, found, _ := u.getLease(ns, coll, []byte("L")); found { t.Fatal("lease row not deleted") }
	if _, found, _ := u.getTomb(ns, coll, []byte("L")); !found { t.Fatal("tombstone not written") }
	_ = r
}

func TestApplyBudExpireStaleSkips(t *testing.T) { /* sweepNow < ReclaimNotBeforeMs -> no-op, lease intact */ }
func TestExpireThenReturnIdempotent(t *testing.T) { /* expire, then return same id -> tombstone no-op, single settlement */ }
func TestReturnThenExpireIdempotent(t *testing.T) { /* return, then expire -> tombstone no-op */ }
```
- [ ] **Step 3: Implement `applyBudExpire`** (§3.5): tombstone-check first (no-op); re-read lease (absent ⇒
  ensure tombstone + delete index, done); re-check `sweepNowMs >= lr.ReclaimNotBeforeMs` (else skip — renewed);
  then **debit**: `rem = lr.Amount - lr.Spent; p.Spent += rem; p.LeasedOut -= rem;` (`available += 0`); in the
  same entry delete lease row, delete `scopeBudExp` entry, write tombstone. Dispatch `opBudExpire` in
  `applyOne`'s budget block (it carries no `c.Idem`). Add `budExpiryDueQuery{NowMs,Limit}` Lookup scanning
  `scopeBudExp` for `be(reclaimNotBeforeMs) <= NowMs` (mirror `scanDue`).
- [ ] **Step 4: Run — PASS** (incl. `budCheckQuery.OK` after expiry). Commit
  `feat(collections): opBudExpire (debit settlement) + due query + dispatch`.

## Task 2b.5: `sweepOnce` second pass proposes `opBudExpire` (+ tombstone GC)

**Files:** Modify `manager.go`, `budget_test.go`.

- [ ] In the leader-gated `sweepOnce` (after the TTL pass), run `budExpiryDueQuery{NowMs: time.Now().UnixMilli(),
  Limit: N}` and for each due lease propose `opBudExpire(leaseID, sweepNowMs)` with `sweepNowMs` stamped
  pre-propose (same discipline as the TTL sweep). Add a tombstone-GC pass: a `ttl.go`-style due index at
  `reclaimNotBeforeMs + dedup_retry_window_ms` that deletes stale tombstones (M-3/I-2).
- [ ] **Integration test** (real shard, mirror `hincr_test.go`): define paced+ttl budget, grant via the typed
  API, wait > (ttl + guard), assert via `BudgetStat`/`budCheckQuery` that the lease auto-expired with DEBIT
  semantics and conservation holds. Commit `feat(collections): lease-expiry sweep + tombstone GC in sweepOnce`.

---

# Sub-stage 2c — Node-side lease cache (sdk/go)

## Task 2c.1: proto — extend `BudgetGrant` with timing; regenerate (server + sdk/go gens)

**Files:** Modify `proto/wavespan/v1/budget.proto`; regenerate.

- [ ] Add to `BudgetGrantRequest`: `int64 ttl_ms`, `int64 self_guard_ms`, `int64 max_pause_budget_ms`. Add to
  `BudgetGrantResult`: `int64 granted_ms`, `int64 ttl_ms`, `int64 self_guard_ms`, `int64 max_pause_budget_ms`
  (echoed so the holder runs the freshness gate + stamps its deadline + sets self-fence; §5 M-4).
- [ ] Regenerate the SERVER stubs and the `sdk/go` vendored stubs. **Codegen gotcha** (from Stage-1): server
  full `buf generate` fails on the missing TS plugin — use the filtered Go-only template; `sdk/go` uses
  `make sdk-proto` (Go-only). Pin `protoc-gen-go@v1.36.11` (prepend `~/go/bin`); `git diff` must show ONLY
  budget stub changes — ZERO drift in unrelated `.pb.go`. Thread the new request fields through `service.go`
  + `grpcsrv/budget.go` into `Collections.BudgetGrant`, and populate the result echo.
- [ ] `go build ./...` + handler test asserting the echo round-trips. Commit
  `feat(proto): timed BudgetGrant fields (ttl/self_guard/max_pause + granted_ms echo)`.

## Task 2c.2: `nowMono()` — suspend-aware `CLOCK_BOOTTIME` (C1)

**Files:** Create `sdk/go/leasedbudget_clock_linux.go`, `sdk/go/leasedbudget_clock_other.go`,
`sdk/go/leasedbudget_clock_test.go`.

- [ ] **Step 1: Failing test** — `TestNowMonoAdvancesAndIsMonotonic` (two reads, second ≥ first; nonzero).
- [ ] **Step 3: Implement.** Linux (`//go:build linux`): `unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts)` →
  `int64` ns. Other (`//go:build !linux`): fall back to `time.Now()` monotonic with a **doc comment + runtime
  warning** that suspend-fencing is not guaranteed off-Linux (the §7 suspend nemesis is Linux-only). Expose
  `nowMono() int64` (ns). (§2 C1 — this is load-bearing; do NOT use `time.Now()` on Linux.)
- [ ] Commit `feat(sdk): suspend-aware nowMono() via CLOCK_BOOTTIME (linux)`.

## Task 2c.3: `LeasedBudgetClient` + `budgetCell` + `Spend` fast path

**Files:** Create `sdk/go/leasedbudget.go`; Modify `sdk/go/client.go`, `sdk/go/leasedbudget_test.go`.

The cell uses an **injectable clock** (`now func() int64`, defaulting to `nowMono`) so tests drive the
self-fence/deadline deterministically.

- [ ] **Step 1: Failing tests** (deterministic clock):
```go
// fast path: in-budget spend decrements locally, no RPC
func TestSpendDecrementsLocally(t *testing.T) { /* install cur{remaining:100,deadline:big}; Spend(10) x10 -> ok; 11th -> ErrBudgetUnavailable+refill */ }
// self-fence: a pause past maxPause drops cur/next and refuses to serve
func TestSpendSelfFencesOnPause(t *testing.T) {
	clk := &fakeClock{t: 1000}
	cell := newTestCell(clk, /*maxPauseNs*/500, /*cur remaining*/100, /*deadline*/10_000)
	if err := cell.Spend(1); err != nil { t.Fatal(err) }     // lastSeen=1000
	clk.t = 1000 + 600                                        // jump > maxPause(500): simulated suspend
	if err := cell.Spend(1); err != ErrBudgetUnavailable { t.Fatalf("self-fence: got %v want ErrBudgetUnavailable", err) }
	if cell.cur != nil { t.Fatal("cur not dropped on self-fence") }
}
// deadline: now >= cur.deadline drops cur
func TestSpendStopsAtDeadline(t *testing.T) { /* ... */ }
// pacing gate: tokens<n -> ErrPacingThrottled
func TestSpendPacingThrottled(t *testing.T) { /* ... */ }
```
- [ ] **Step 3: Implement** per §4.1 EXACTLY (order matters): re-read `now` under the lock immediately before
  the decrement; (a) self-fence (`lastSeenMon != 0 && now-lastSeenMon > maxPauseNs` → drop cur/next, **no
  report** — I-1); (b) pacing gate; (c) drop `cur` if `now >= cur.deadlineMon`; (d) decrement + low-watermark
  refill trigger; (e) STRICT-empty → trigger refill + `ErrBudgetUnavailable`. `Spend` holds the cell lock but
  NEVER across an RPC (refill is off-path, single-flight). Add `Acquire`/`Remaining`.
- [ ] **Step 4: Run — PASS.** Commit `feat(sdk): LeasedBudgetClient + zero-coordination Spend fast path`.

## Task 2c.4: refill (single-flight, double-buffer, freshness gate)

**Files:** Modify `sdk/go/leasedbudget.go`, `sdk/go/leasedbudget_test.go`.

- [ ] **Step 1: Failing tests** — `TestRefillInstallsNextAndPromotes` (low-watermark `Spend` triggers one
  `Draw`; `next` installs; promotes to `cur` when `cur` drains); `TestRefillFreshnessGateRejectsStaleGrant`
  (mock `Draw` returns `granted_ms` older than `now-selfGuard` → grant dropped, re-draw); `TestRefillSingleFlight`
  (concurrent low-watermark `Spend`s issue exactly one in-flight `Draw`).
- [ ] **Step 3: Implement** per §4.2: single-flight via `atomic.Bool`; stable per-refill `leaseID`
  (`hash(node,budget,nonce)`, reused across retries → exactly-once with the 2b tombstone; rotate nonce after a
  committed refill, mint fresh after an `AlreadySettled`); on success run the freshness gate
  (`recvWall - granted_ms > self_guard` → drop), set `cell.maxPauseNs = max_pause_budget_ms`, install
  `next.deadlineMon = nowMono() + ttl - selfGuard`; report this lease's cumulative spent on the `Draw`.
  Crash waste ≤ `2·chunk`.
- [ ] **Step 4: Run — PASS.** Commit `feat(sdk): single-flight refill, double-buffer, freshness gate`.

## Task 2c.5: graceful `Return`

**Files:** Modify `sdk/go/leasedbudget.go`, `sdk/go/leasedbudget_test.go`.

- [ ] `Budget.Return(ctx)` folds the final cumulative spent and calls `BudgetReturn` (server credits the true
  remainder, §3.6). Test `TestReturnReleasesUnspent` (integration: Acquire, Spend some, Return → server
  `BudgetStat` shows the unspent credited back, conservation holds). Commit
  `feat(sdk): graceful Budget.Return (attested credit)`.

---

# Sub-stage 2d — Node-side lease cache (wavespan-sdk, independent)

## Task 2d.1–2d.5: mirror 2c.2–2c.5 in the external repo

**Files:** Create `wavespan-sdk/leasedbudget.go` + clock files + tests; Modify `wavespan-sdk/client.go`.
Separate git repo — commit there.

- [ ] First ensure the external SDK's vendored proto has the 2c.1 timing fields: copy the updated
  `budget.proto` into `wavespan-sdk/proto/wavespan/v1/`, `buf generate` (Go-only; pin protoc-gen-go; zero
  drift), thread nothing server-side (client only).
- [ ] Re-implement `nowMono()` (boottime), `LeasedBudgetClient`/`budgetCell`/`Spend`/refill/`Return`
  **independently** (do NOT import sdk/go), mirroring §4 and the 2c tests. Routing difference: this SDK has no
  shard router — the refill `Draw` uses the shared conn (server-side forwarding). Port the full 2c test suite.
- [ ] `go build/vet/test ./...` green. Commit in `wavespan-sdk`:
  `feat: LeasedBudget node cache (Acquire/Spend/refill/Return, boottime self-fence)`.

---

# Sub-stage 2e — Correctness harness (the §16 mandate)

## Task 2e.1: timed conservation fuzz (server)

**Files:** Modify `internal/collections/budget_test.go`.

- [ ] Extend `TestBudgetConservationFuzz` to interleave timed grants (random ttl, leader-stamped grantedMs
  from a deterministic counter) + reports + returns + **expiry sweeps** (advance the stamped clock, run
  `budExpiryDueQuery`, apply `opBudExpire`). After EVERY op assert `budCheck.OK`. Track an external
  `acked[leaseID]` ledger and assert `Σ acked ≤ cap` always (the equality alone can't catch a debit→credit
  regression). Commit.

## Task 2e.2: combined nemesis soak + suspend nemesis + negative controls

**Files:** Create `tests/harness/workloads/budget/strict_cap.go`, `tests/harness/checker/budget.go` (mirror an
existing workload/checker).

- [ ] **`budget-strict-cap` workload:** N node caches `Spend`ing against one shared budget under
  `cluster-partition` + `clock-skew-bounded` + `pause/resume past TTL` + holder-crash **together**. The checker
  asserts `Σ (externally-acked impressions × price) ≤ cap` at every instant — ground truth, NOT the pool's
  internal `spent` (C2).
- [ ] **Suspend nemesis (C1):** inject a pause that freezes `CLOCK_MONOTONIC` while `CLOCK_BOOTTIME` advances
  past the guard; assert the holder self-fences (serves nothing) on resume.
- [ ] **Negative controls (MUST go RED — assert the checker fails):** (a) disable the freshness gate / guard
  band; (b) flip forced expiry from debit→credit; (c) use `CLOCK_MONOTONIC` under the suspend nemesis. Each is
  a separate test guarded behind a build tag or a checker knob so CI runs the positive path and the negatives
  are runnable on demand. `log()` what each negative control proves.
- [ ] Commit `test(harness): budget-strict-cap soak + suspend nemesis + negative controls`.

## Task 2e.3: Stage-2 verification gate

**Files:** none (verification only).

- [ ] `go test -race ./internal/collections/... ./internal/grpcsrv/...` green, no races.
- [ ] `cd sdk/go && GOWORK=off go test -race ./...` and `cd wavespan-sdk && go test -race ./...` green.
- [ ] `go vet` + repo lint (`golangci-lint run`) clean across all three modules.
- [ ] Update `design/35` §14 staging: mark Stage 2 IMPLEMENTED; note the §16 edits 1/2/3/6 closed for the
  single-cluster case, the debit-on-forced-expiry rule, and the boottime requirement.
- [ ] Final review (subagent) across the whole Stage-2 diff, then `superpowers:finishing-a-development-branch`.
- [ ] Commit `test(collections): Stage-2 verification gate + design status`.

---

## Notes for the implementing engineer
- **TDD, frequent commits, match conventions** (grep real symbols; line numbers drift). Verified-fact list is
  in the Stage-1 plan's "Notes" — it still holds (no `errors.go`; `applySingleForTest` exists now; StoreOp
  fields; budget scopes 0x05–0x08).
- **No wall-clock in apply, EVER.** All time is leader-stamped (`grantedMs`, `sweepNowMs`) into the command
  pre-propose. The ONLY place a real clock is read on the server is `sweepOnce`/`BudgetGrant` *before* propose.
- **The money-safety crux is one line:** forced expiry DEBITS (`spent += rem`), graceful Return CREDITS
  (`available += rem`). Never credit on a path that isn't holder-attested. The fuzz + soak assert external
  acked spend, not the internal equality.
- **Boottime is load-bearing.** On Linux the node cache MUST read `CLOCK_BOOTTIME`. `time.Now()` monotonic
  freezes on suspend and silently breaks the self-fence.
- **Sub-stage gates:** 2a, 2b independently green before 2c/2d (which need 2b's grant-result echo). 2e last.
- **Reference skills:** @superpowers:test-driven-development, @superpowers:subagent-driven-development.
