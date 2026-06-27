# LeasedBudget — Stage 1 (Single-Cluster Escrow Core) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a STRICT single-cluster escrow datatype (`LeasedBudget`) to waveSpan's `internal/collections` consensus tier, with atomic, idempotent grant/report/return over one Raft entry, exposed via proto + gRPC, with the conservation invariant `cap == available + leasedOut + spent` provable in tests.

**Architecture:** `LeasedBudget` is a new `collType` (`typeBudget = 4`) handled by the *existing* `shardSM` in `internal/collections`, exactly as Sets/Hashes/SortedSets are. It reuses the existing command encoding, `updateCtx` overlays, dedup ring (`dedup.go`), type-checking (`ensureType`), snapshot/restore (`base_sm.go`), and leader routing (`WithForwarder`). A budget is a pool record + a lease table, both keyed under the shard's `subData` space. Lease grant is a single deterministic Raft entry that atomically moves quantity between disjoint buckets — the same read-modify-write discipline as `applyHIncrInt` (`hincr.go:38`).

**Tech Stack:** Go 1.26, dragonboat Raft (`internal/collections`), `wavesdb` LSM via `storage.LocalStore`, Connect-RPC + grpc-go (buf codegen), `storage.NewMemStore()` test harness.

**Spec:** `design/35_leased_budget_datatype.md` (full design). This plan implements **Stage 1 of 4**:
- **Stage 1 (this plan):** single-cluster STRICT escrow core — define / grant / report / return, idempotent, conservation-checked. No pacing rate, no lease expiry, no hierarchy, no recall, no RELAXED.
- **Stage 2:** token-bucket pacing rate (leader-stamped `grantedMs`), lease expiry sweep + reclaim (reusing `ttl.go`/`sweepOnce`), node-side lease cache SDK (`Acquire/Spend/refill`), waste bound.
- **Stage 3:** dynamic steering (`opBudSteer` + epoch), emergency recall (`pendingReclaim` bucket + invalidate-now/reclaim-after-settle + §6.5 guard band), hard cap-cut, `BudgetWatch`.
- **Stage 4:** RELAXED mode (bounded overdraft + proportional-throttle reconciliation), 3-level hierarchy + cross-cluster grant RPC.

Each stage is independently working, testable software. **Do not** pull Stage 2+ concerns into Stage 1.

---

## Scope check

The full datatype spans several independent subsystems (server state machine, node SDK cache, cross-cluster transport, steering controller). They are decomposed into the four stages above; this plan covers only the Stage-1 server core, which produces a usable, testable escrow pool on a single shard. Stages 2–4 each get their own plan after Stage 1 lands and is verified.

## Conservation model (the invariant every task must preserve)

For a STRICT pool, `leasedOut` tracks the **unspent** quantity currently held in outstanding leases, and `spent` is the cumulative consumed total:

```
INV-LOCAL:   cap == available + leasedOut + spent
where        leasedOut == Σ (lease.amount − lease.spent)   over outstanding leases
             spent     == Σ lease.spent (incl. returned leases)
```

Lifecycle effect on the buckets (each is one Raft entry):
- **Init:** `available = cap`, `leasedOut = 0`, `spent = 0`.
- **Grant(g):** `g = min(req, available)`; `available -= g`, `leasedOut += g`, new `lease{amount:g, spent:0}`.
- **Report(δ):** `δ = max(0, reportedCumulative − lease.spent)`; `lease.spent += δ`, `leasedOut -= δ`, `spent += δ`.
- **Return:** fold final report first; `rem = lease.amount − lease.spent`; `available += rem`, `leasedOut -= rem`; delete lease row.

All quantities are **`int64` micro-units** (BE-encoded), never decimal strings — budgets are compared/saturated arithmetically on every grant, and money exactness is preserved by integer micro-units.

## File structure

| File | Responsibility | Action |
|---|---|---|
| `internal/collections/command.go` | add `opBudInit/opBudGrant/opBudReport/opBudReturn` to `opKind`; `typeBudget` to `collType`; `typeForOp` cases | Modify |
| `internal/collections/budget.go` | **new** — budget key layout, pool/lease record encode/decode, `applyBudInit/applyBudGrant/applyBudReport/applyBudReturn`, sentinels | Create |
| `internal/collections/statemachine.go` | dispatch the four ops in `applyOne`; add budget Lookup query (`budStatQuery`, `budCheckQuery`) | Modify |
| `internal/collections/collections.go` | typed API: `BudgetDefine/BudgetGrant/BudgetReport/BudgetReturn/BudgetStat` | Modify |
| `internal/collections/command.go` | `ErrNoCapacity`, `ErrBudgetExists`, `ErrBudgetNotFound`, `ErrLeaseUnknown`, `ErrUnsupportedMode` (errors live here, NOT in a separate errors.go — it doesn't exist) | Modify |
| `internal/collections/service.go` | Connect handlers delegating to the typed API | Modify |
| `internal/grpcsrv/collections.go` | gRPC adapters wrapping the Connect service | Modify |
| `proto/wavespan/v1/budget.proto` | **new** — `BudgetService` (Stage-1 subset) + messages | Create |
| `internal/collections/budget_test.go` | **new** — TDD tests (encode/decode, apply logic, conservation, idempotency, integration) | Create |

> **SDK path note:** the standalone Go SDK is the `wavespan-sdk` repo (root-level `collections.go`, `internal/gen/wavespan/v1`). The SDK client is **Stage 2** (it pairs with the node-lease cache). Stage 1 is exercised via the in-process `Collections` API in `budget_test.go`, exactly like `hincr_test.go`.

---

## Task 1: opKinds, collType, and typeForOp

**Files:**
- Modify: `internal/collections/command.go` (enum at lines 44-60, `collType` at 70-74, `typeForOp` at 77-87)
- Test: `internal/collections/budget_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/collections/budget_test.go`:

```go
package collections

import "testing"

func TestBudgetOpKindsAndType(t *testing.T) {
	// New opKinds are distinct and contiguous after opBatch(15).
	ops := []opKind{opBudInit, opBudGrant, opBudReport, opBudReturn}
	seen := map[opKind]bool{}
	for _, o := range ops {
		if o <= opBatch {
			t.Fatalf("op %d must be > opBatch(%d)", o, opBatch)
		}
		if seen[o] {
			t.Fatalf("duplicate opKind %d", o)
		}
		seen[o] = true
	}
	// typeForOp maps all four budget ops to typeBudget.
	for _, o := range ops {
		if got := typeForOp(o); got != typeBudget {
			t.Fatalf("typeForOp(%d) = %d, want typeBudget(%d)", o, got, typeBudget)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run TestBudgetOpKindsAndType`
Expected: FAIL — compile error `undefined: opBudInit` / `typeBudget`.

- [ ] **Step 3: Add the enum values and typeForOp cases**

In `command.go`, extend the `opKind` block (after `opBatch opKind = 15`):

```go
	opBudInit   opKind = 16 // leased-budget: create pool (cap, mode); epoch=1
	opBudGrant  opKind = 17 // leased-budget: atomic lease grant (create-if-absent by Idem)
	opBudReport opKind = 18 // leased-budget: cumulative-per-lease spent fold (max)
	opBudReturn opKind = 19 // leased-budget: release unspent; book spent
```

Extend the `collType` block (after `typeZSet collType = 3`):

```go
	typeBudget collType = 4
```

In `typeForOp`, add cases mapping the four ops to `typeBudget` (mirror the existing hash cases; `opBudInit` is the type-creating op, like `opHSet`):

```go
	case opBudInit, opBudGrant, opBudReport, opBudReturn:
		return typeBudget
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/collections/ -run TestBudgetOpKindsAndType`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/command.go internal/collections/budget_test.go
git commit -m "feat(collections): add LeasedBudget opKinds and typeBudget"
```

---

## Task 2: Budget key layout and record codecs

**Files:**
- Create: `internal/collections/budget.go`
- Test: `internal/collections/budget_test.go`

Budget keys live under the shard `subData` space alongside collections, using new sub-scope bytes. The existing sub-scopes under `collScope` are `scopeCard 0x00 / scopeElem 0x01 / scopeZPtr 0x02 / scopeType 0x03` (`statemachine.go:33-40`) **and `scopeTTLPtr 0x04` (`ttl.go:17`)** — so `0x05` is the first free byte (verified: taken bytes are `0x00`–`0x04`, no collision). `typeKey` already stores the type header, so a budget pool reuses `scopeType` for WRONGTYPE checks and adds its own pool/lease scopes:

```
collScope(ns,coll) | scopeBudPool (0x05)              -> poolRec  {cap, available, leasedOut, spent, epoch, mode, rate, burst}
collScope(ns,coll) | scopeBudLease(0x06) | <leaseID>  -> leaseRec {holder, amount, spent, epoch}
# reserved for later stages (do NOT use in Stage 1): 0x07 = lease-expiry index (Stage 2), 0x08 = settled tombstone (Stage 3)
```

> **Key-layout forward-compatibility (resolves review gap B1).** Stage 1 stores ONE combined `poolRec`
> at `0x05` (holding both the config and the accounting fields). To stay **migration-free**, `decodePool`
> reads only its fixed prefix and **tolerates trailing bytes**, so Stages 2–3 can *append* fields
> (`lastRefillMs`, `tokens`, `pendingReclaim`) to the same record with no snapshot rewrite. The contract:
> **fields are append-only — never reorder or resize an existing field.** This supersedes the design's
> original separate `scopeBudCfg`/`scopeBudState` split (design §6.1 is amended to this combined record);
> the lease table stays at `0x06`, and `0x07`/`0x08` are reserved as above so no record ever moves.

- [ ] **Step 1: Write the failing test**

Add to `budget_test.go`:

```go
func TestPoolRecRoundTrip(t *testing.T) {
	p := poolRec{Cap: 500_000_000, Available: 400_000_000, LeasedOut: 90_000_000, Spent: 10_000_000, Epoch: 3, Mode: modeStrict, Rate: 0, Burst: 500_000_000}
	got, err := decodePool(encodePool(p))
	if err != nil {
		t.Fatalf("decodePool: %v", err)
	}
	if got != p {
		t.Fatalf("round-trip = %+v, want %+v", got, p)
	}
}

func TestLeaseRecRoundTrip(t *testing.T) {
	l := leaseRec{Holder: []byte("node-7"), Amount: 600_000, Spent: 250_000, Epoch: 3}
	got, err := decodeLease(encodeLease(l))
	if err != nil {
		t.Fatalf("decodeLease: %v", err)
	}
	if string(got.Holder) != string(l.Holder) || got.Amount != l.Amount || got.Spent != l.Spent || got.Epoch != l.Epoch {
		t.Fatalf("round-trip = %+v, want %+v", got, l)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run 'TestPoolRecRoundTrip|TestLeaseRecRoundTrip'`
Expected: FAIL — undefined `poolRec` / `encodePool` / `modeStrict`.

- [ ] **Step 3: Implement `budget.go` codecs and key layout**

```go
package collections

import (
	"encoding/binary"
	"errors"
)

// Budget sub-scope bytes. Existing collScope sub-scopes are 0x00..0x04 (scopeCard/Elem/ZPtr/Type in
// statemachine.go:33-40, scopeTTLPtr in ttl.go:17), so 0x05 is the first free byte.
const (
	scopeBudPool  byte = 0x05 // the pool record (combined cfg+state; append-extensible)
	scopeBudLease byte = 0x06 // <leaseID> -> leaseRec
	// reserved (NOT used in Stage 1): 0x07 lease-expiry index (Stage 2), 0x08 settled tombstone (Stage 3)
)

// Budget modes (Stage 1 ships STRICT only; modeRelaxed reserved for Stage 4 and rejected at init here).
const (
	modeStrict uint8 = 1
	modeRelaxed uint8 = 2
)

// Sentinels returned in ProposeResult.Data (mirror wrongType/notNumber in command.go:36-39).
var (
	budNoCapacity = []byte("BUDNOCAP")     // grant could not allocate (>0) in STRICT
	budExists     = []byte("BUDEXISTS")    // init on an existing pool
	budNoLease    = []byte("BUDNOLEASE")   // report/return on unknown lease
	budNoBudget   = []byte("BUDNOBUDGET")  // grant against a pool that does not exist (B9: distinct from budNoLease)
	budBadMode    = []byte("BUDBADMODE")   // init with a non-STRICT mode, or invalid cap (B3/B4)
)

var (
	errShortPool  = errors.New("collections: short budget pool record")
	errShortLease = errors.New("collections: short budget lease record")
)

// poolRec is the budget pool. Quantities are int64 micro-units.
type poolRec struct {
	Cap       int64
	Available int64
	LeasedOut int64 // Σ (lease.amount - lease.spent) over outstanding leases (UNSPENT held)
	Spent     int64 // Σ lease.spent (cumulative consumed)
	Epoch     uint64
	Mode      uint8
	Rate      int64 // micro-units/sec; Stage 1 ignores (0 = no pacing)
	Burst     int64 // ceiling; Stage 1: == Cap
}

func (s *shardSM) budPoolKey(ns, coll []byte) []byte  { return append(s.collScope(ns, coll), scopeBudPool) }
func (s *shardSM) budLeasePrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudLease)
}
func (s *shardSM) budLeaseKey(ns, coll, leaseID []byte) []byte {
	return append(s.budLeasePrefix(ns, coll), leaseID...)
}

func putI64(b []byte, v int64) { binary.BigEndian.PutUint64(b, uint64(v)) }
func getI64(b []byte) int64    { return int64(binary.BigEndian.Uint64(b)) }

func encodePool(p poolRec) []byte {
	b := make([]byte, 8*4+8+1+8*2) // cap,avail,leased,spent | epoch | mode | rate,burst
	putI64(b[0:], p.Cap)
	putI64(b[8:], p.Available)
	putI64(b[16:], p.LeasedOut)
	putI64(b[24:], p.Spent)
	binary.BigEndian.PutUint64(b[32:], p.Epoch)
	b[40] = p.Mode
	putI64(b[41:], p.Rate)
	putI64(b[49:], p.Burst)
	return b
}

// decodePool is APPEND-TOLERANT (B1): it reads only the fixed 57-byte prefix and ignores any trailing
// bytes, so Stages 2-3 may append fields (lastRefillMs, tokens, pendingReclaim) to the same record with
// NO snapshot migration. Contract: append-only — never reorder or resize an existing field.
func decodePool(b []byte) (poolRec, error) {
	if len(b) < 57 { // a shorter buffer is corruption; a longer one is a newer-stage record (tolerated)
		return poolRec{}, errShortPool
	}
	return poolRec{
		Cap: getI64(b[0:]), Available: getI64(b[8:]), LeasedOut: getI64(b[16:]), Spent: getI64(b[24:]),
		Epoch: binary.BigEndian.Uint64(b[32:]), Mode: b[40], Rate: getI64(b[41:]), Burst: getI64(b[49:]),
	}, nil
}

// leaseRec is one outstanding lease.
type leaseRec struct {
	Holder []byte
	Amount int64
	Spent  int64
	Epoch  uint64
}

func encodeLease(l leaseRec) []byte {
	b := make([]byte, 0, 8*2+8+4+len(l.Holder))
	var num [8]byte
	putI64(num[:], l.Amount); b = append(b, num[:]...)
	putI64(num[:], l.Spent); b = append(b, num[:]...)
	binary.BigEndian.PutUint64(num[:], l.Epoch); b = append(b, num[:]...)
	b = appendChunk(b, l.Holder) // uint32 len-prefixed (command.go:255)
	return b
}

func decodeLease(b []byte) (leaseRec, error) {
	if len(b) < 24 {
		return leaseRec{}, errShortLease
	}
	l := leaseRec{Amount: getI64(b[0:]), Spent: getI64(b[8:]), Epoch: binary.BigEndian.Uint64(b[16:])}
	holder, _, err := takeChunk(b[24:]) // command.go:262
	if err != nil {
		return leaseRec{}, err
	}
	l.Holder = append([]byte{}, holder...)
	return l, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/collections/ -run 'TestPoolRecRoundTrip|TestLeaseRecRoundTrip'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/budget.go internal/collections/budget_test.go
git commit -m "feat(collections): LeasedBudget key layout + pool/lease codecs"
```

---

## Task 3: `updateCtx` budget helpers (overlay-aware read/write)

**Files:**
- Modify: `internal/collections/budget.go`
- Test: `internal/collections/budget_test.go`

These mirror `fieldVal`/`setFieldVal` (`hincr.go:19-34`): writes go to `u.ops` *and* an in-batch overlay so two budget ops in one coalesced batch compose. Budget pool/lease use the generic `vals` overlay (keyed by the full storage key) plus `u.s.getData`.

- [ ] **Step 1: Write the failing test** — add `TestUpdateCtxPoolOverlay` that constructs a `shardSM` over `storage.NewMemStore()` (see Task 7 harness), opens an `updateCtx`, calls `setPool` then `getPool` and asserts the overlay returns the just-written pool before any flush. (Defer running until helpers exist.)

```go
func TestUpdateCtxPoolOverlay(t *testing.T) {
	u := newTestUpdateCtx(t) // helper added in Task 7; wraps shardSM + empty overlays
	ns, coll := []byte("pacing"), []byte("li/42/total")
	p := poolRec{Cap: 100, Available: 100, Epoch: 1, Mode: modeStrict, Burst: 100}
	u.setPool(ns, coll, p)
	got, found, err := u.getPool(ns, coll)
	if err != nil || !found || got.Available != 100 {
		t.Fatalf("getPool after setPool = %+v found=%v err=%v", got, found, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run TestUpdateCtxPoolOverlay`
Expected: FAIL — undefined `setPool`/`getPool`/`newTestUpdateCtx`.

- [ ] **Step 3: Implement the helpers in `budget.go`**

```go
import "github.com/yannick/wavespan/internal/storage" // match the import path used in hincr.go

// getPool returns the pool record honoring the in-batch overlay.
func (u *updateCtx) getPool(ns, coll []byte) (poolRec, bool, error) {
	k := u.s.budPoolKey(ns, coll)
	if v, ok := u.vals[string(k)]; ok {
		if v == nil {
			return poolRec{}, false, nil
		}
		p, err := decodePool(v)
		return p, err == nil, err
	}
	v, found, err := u.s.getData(k)
	if err != nil || !found {
		return poolRec{}, false, err
	}
	p, err := decodePool(v)
	return p, err == nil, err
}

func (u *updateCtx) setPool(ns, coll []byte, p poolRec) {
	k := u.s.budPoolKey(ns, coll)
	enc := encodePool(p)
	u.vals[string(k)] = enc
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Value: enc})
}

func (u *updateCtx) getLease(ns, coll, id []byte) (leaseRec, bool, error) {
	k := u.s.budLeaseKey(ns, coll, id)
	if v, ok := u.vals[string(k)]; ok {
		if v == nil {
			return leaseRec{}, false, nil
		}
		l, err := decodeLease(v)
		return l, err == nil, err
	}
	v, found, err := u.s.getData(k)
	if err != nil || !found {
		return leaseRec{}, false, err
	}
	l, err := decodeLease(v)
	return l, err == nil, err
}

func (u *updateCtx) setLease(ns, coll, id []byte, l leaseRec) {
	k := u.s.budLeaseKey(ns, coll, id)
	enc := encodeLease(l)
	u.vals[string(k)] = enc
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Value: enc})
}

func (u *updateCtx) delLease(ns, coll, id []byte) {
	k := u.s.budLeaseKey(ns, coll, id)
	u.vals[string(k)] = nil
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: k, Delete: true})
}
```

> **Verified facts** (no need to re-confirm): `storage.StoreOp` has fields `{CF, Key, Value, Delete, ExpiresAtUnixMs}` (`store.go:34-43`); budget helpers set only `CF/Key/Value` (and `Delete` for `delLease`), leaving `ExpiresAtUnixMs` zero. `setFieldVal` (`hincr.go`) writes with `CF: storage.CFReplData` — use the same. The delete pattern matches `clearTTL` (`ttl.go:74-75`).

- [ ] **Step 4: Run test to verify it passes** (after Task 7 adds `newTestUpdateCtx`; if implementing in order, temporarily inline a minimal `updateCtx` constructor, then remove).

Run: `go test ./internal/collections/ -run TestUpdateCtxPoolOverlay`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/budget.go internal/collections/budget_test.go
git commit -m "feat(collections): overlay-aware budget pool/lease helpers"
```

---

## Task 4: Apply logic — init / grant / report / return (pure functions)

**Files:**
- Modify: `internal/collections/budget.go`
- Test: `internal/collections/budget_test.go`

These are the heart of the datatype. Each runs inside `applyOne` under the deterministic overlay and must preserve `INV-LOCAL`. They take the decoded `command` and return **`(ProposeResult, error)` directly** (NOT the `(int64, []byte, error)` shape of `applyHIncrInt`; `ProposeResult{Value uint64; Data []byte}` is defined in `raftshard.go:7-10`, so all quantity fields assigned to `Value` must be `uint64(...)`-cast). The result `Data` carries sentinels or encoded values; benign failures return a sentinel in `Data`, never an `error` (errors are reserved for storage faults / corrupt entries).

**Idempotency design (revised after plan review — critical).** Budget ops must **NOT** set `command.Idem`.
`applyOne` dedups on `c.Idem` *before* dispatch via the shared 4096-entry ring (`statemachine.go:266-276`,
`dedup.go:27,43,52`), keyed on the raw key with no op-kind discrimination. If grant/report/return all reused
`leaseID` as `Idem`, a report would hit the grant's cached entry and never execute. So `leaseID` travels in
`Items[0].Key`, `Idem` stays empty, and idempotency is enforced **at the apply layer**:
- **grant** — idempotent for the lease's lifetime via the durable lease-row check (`getLease(leaseID)`); a retry returns the existing amount.
- **report** — naturally idempotent via the cumulative-`max` fold (a duplicate/stale cumulative is a no-op).
- **return** — naturally idempotent: once the lease row is deleted, a retry returns `budNoLease`.

> **Stage-1 idempotency CONTRACT (review gap B5 — explicit, not a footnote):** grant idempotency holds only
> while the lease row exists. After `BudgetReturn` deletes it, a late duplicate `BudgetGrant(sameLeaseID)`
> grants **fresh** quantity (no tombstone yet; the ring may have evicted) — a silent double-spend for money.
> Therefore **`lease_id` is single-use-forever: callers MUST NOT reuse a `lease_id` after `BudgetReturn`.**
> This is a documented Stage-1 limitation, captured by `TestGrantAfterReturnReusesIdRegrantsFreshQuantity`
> (Task 11) so it is asserted, not silent. Stage 3 closes the window with settled-lease tombstones (spec §6.3).

Command field mapping (reuse `command`/`item` from `command.go:110-127`, no struct changes; `Idem` empty for all).
Note the `item` struct also carries `Score float64` and `ExpiryMs int64` (used by zset/TTL) — budget ops leave
them zero and use only `Key`/`Val`:
- `opBudInit`: `Items[0].Key = mode(1B) | cap(8B) | rate(8B) | burst(8B)`.
- `opBudGrant`: `Items[0].Key = leaseID`; `Items[0].Val = amount(8B BE) | holder`.
- `opBudReport`: `Items[0].Key = leaseID`; `Items[0].Val = cumulativeSpent(8B BE)`.
- `opBudReturn`: `Items[0].Key = leaseID`; `Items[0].Val = finalCumulativeSpent(8B BE)`.

- [ ] **Step 1: Write the failing tests**

```go
func TestApplyBudInitAndGrant(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	// init cap=1000 STRICT
	if _, err := u.applyBudInit(initCmd(ns, coll, 1000)); err != nil {
		t.Fatalf("init: %v", err)
	}
	// grant 600 to node-A
	r, err := u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if g := decodeGrant(r.Data); g != 600 {
		t.Fatalf("grant amount = %d, want 600", g)
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 400 || p.LeasedOut != 600 {
		t.Fatalf("after grant: avail=%d leased=%d, want 400/600", p.Available, p.LeasedOut)
	}
}

func TestApplyBudGrantSaturatesAtAvailable(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 500))
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 900)) // ask > available
	if g := decodeGrant(r.Data); g != 500 {                // PARTIAL grant to available
		t.Fatalf("grant = %d, want 500 (saturated)", g)
	}
	// next grant gets nothing -> BUDNOCAP sentinel
	r2, _ := u.applyBudGrant(grantCmd(ns, coll, "B", 100))
	if string(r2.Data) != string(budNoCapacity) {
		t.Fatalf("second grant Data = %q, want BUDNOCAP", r2.Data)
	}
}

func TestApplyBudReportThenReturnConserves(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	_, _ = u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	// report cumulative spent 250
	_, _ = u.applyBudReport(reportCmd(ns, coll, "A", 250))
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p)
	if p.Spent != 250 || p.LeasedOut != 350 {
		t.Fatalf("after report: spent=%d leased=%d want 250/350", p.Spent, p.LeasedOut)
	}
	// duplicate/stale report (cumulative 200 < 250) is a no-op (max fold)
	_, _ = u.applyBudReport(reportCmd(ns, coll, "A", 200))
	p, _, _ = u.getPool(ns, coll)
	if p.Spent != 250 {
		t.Fatalf("stale report changed spent to %d, want 250", p.Spent)
	}
	// return: unspent 350 goes back to available
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "A", 250))
	p, _, _ = u.getPool(ns, coll)
	assertInv(t, p)
	if p.Available != 750 || p.LeasedOut != 0 || p.Spent != 250 {
		t.Fatalf("after return: avail=%d leased=%d spent=%d want 750/0/250", p.Available, p.LeasedOut, p.Spent)
	}
}

func TestApplyBudGrantIdempotent(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	r1, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // leaseID "A" (Idem)
	r2, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // retry same leaseID
	if decodeGrant(r1.Data) != 600 || decodeGrant(r2.Data) != 600 {
		t.Fatalf("idempotent grant mismatch: %d vs %d", decodeGrant(r1.Data), decodeGrant(r2.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	if p.LeasedOut != 600 || p.Available != 400 { // NOT 1200/-200
		t.Fatalf("retry double-granted: leased=%d avail=%d", p.LeasedOut, p.Available)
	}
}

// B3: a non-STRICT mode is rejected at init (Stage 1 is STRICT-only).
func TestApplyBudInitRejectsNonStrict(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	c := initCmd(ns, coll, 1000)
	c.Items[0].Key[0] = modeRelaxed // override mode byte
	r, _ := u.applyBudInit(c)
	if string(r.Data) != string(budBadMode) {
		t.Fatalf("init RELAXED Data = %q, want BUDBADMODE", r.Data)
	}
	if _, found, _ := u.getPool(ns, coll); found {
		t.Fatalf("rejected init must not create a pool")
	}
}

// B4: a negative cap is rejected; a negative draw never debits.
func TestApplyBudRejectsNegativeInputs(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("pacing"), []byte("li/42")
	if r, _ := u.applyBudInit(initCmd(ns, coll, -5)); string(r.Data) != string(budBadMode) {
		t.Fatalf("negative cap Data = %q, want BUDBADMODE", r.Data)
	}
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", -1))
	if string(r.Data) != string(budNoCapacity) {
		t.Fatalf("negative draw Data = %q, want BUDNOCAP", r.Data)
	}
	p, _, _ := u.getPool(ns, coll)
	if p.Available != 1000 { // unchanged
		t.Fatalf("negative draw debited available to %d", p.Available)
	}
}

// B9: a grant against an undefined budget returns budNoBudget (not budNoLease).
func TestApplyBudGrantNoBudget(t *testing.T) {
	u := newTestUpdateCtx(t)
	r, _ := u.applyBudGrant(grantCmd([]byte("pacing"), []byte("missing"), "A", 1))
	if string(r.Data) != string(budNoBudget) {
		t.Fatalf("grant on missing budget Data = %q, want BUDNOBUDGET", r.Data)
	}
}
```

Add test builders + `assertInv` + `decodeGrant` near the top of the test file:

```go
func initCmd(ns, coll []byte, cap int64) command {
	k := make([]byte, 1+8*3)
	k[0] = modeStrict
	putI64(k[1:], cap)  // cap
	putI64(k[9:], 0)    // rate
	putI64(k[17:], cap) // burst = cap (Stage 1)
	return command{Op: opBudInit, NS: ns, Coll: coll, Items: []item{{Key: k}}}
}

// grant: leaseID in Items[0].Key; Val = amount(8B) || holder. NOTE: the test uses leaseID == holder string
// purely for brevity; in production they differ (leaseID is per-refill). Idem stays EMPTY (see idempotency design).
func grantCmd(ns, coll []byte, holder string, amt int64) command {
	v := make([]byte, 8+len(holder)); putI64(v, amt); copy(v[8:], holder)
	return command{Op: opBudGrant, NS: ns, Coll: coll, Items: []item{{Key: []byte(holder), Val: v}}}
}
func reportCmd(ns, coll []byte, leaseID string, cum int64) command {
	v := make([]byte, 8); putI64(v, cum)
	return command{Op: opBudReport, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}
func returnCmd(ns, coll []byte, leaseID string, cum int64) command {
	v := make([]byte, 8); putI64(v, cum)
	return command{Op: opBudReturn, NS: ns, Coll: coll, Items: []item{{Key: []byte(leaseID), Val: v}}}
}
func decodeGrant(b []byte) int64 { if len(b) < 8 { return -1 }; return getI64(b) }
func assertInv(t *testing.T, p poolRec) {
	t.Helper()
	if p.Available+p.LeasedOut+p.Spent != p.Cap {
		t.Fatalf("INV-LOCAL violated: %d+%d+%d != cap %d", p.Available, p.LeasedOut, p.Spent, p.Cap)
	}
	if p.Available < 0 || p.LeasedOut < 0 || p.Spent > p.Cap {
		t.Fatalf("bucket out of range: %+v", p)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/collections/ -run TestApplyBud`
Expected: FAIL — undefined `applyBudInit` etc.

- [ ] **Step 3: Implement the four apply functions in `budget.go`**

```go
// applyBudInit creates a pool. Returns budExists if one already exists, or budBadMode for a non-STRICT
// mode / invalid cap (B3, B4) — both are sentinels in Data, not errors.
func (u *updateCtx) applyBudInit(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) < 25 {
		return ProposeResult{}, errShortCommand // existing sentinel in command.go
	}
	k := c.Items[0].Key
	mode := k[0]
	cap := getI64(k[1:])
	rate := getI64(k[9:])
	burst := getI64(k[17:])
	// B3: Stage 1 only supports STRICT. B4: reject negative cap/rate/burst (admission-validates at the SM
	// too, since the in-process API and direct proposers bypass the CRD layer; apply is deterministic).
	if mode != modeStrict || cap < 0 || rate < 0 || burst < 0 {
		return ProposeResult{Data: budBadMode}, nil
	}
	if _, found, err := u.getPool(c.NS, c.Coll); err != nil {
		return ProposeResult{}, err
	} else if found {
		return ProposeResult{Data: budExists}, nil
	}
	p := poolRec{Cap: cap, Available: cap, LeasedOut: 0, Spent: 0, Epoch: 1, Mode: mode, Rate: rate, Burst: burst}
	u.setPool(c.NS, c.Coll, p)
	return ProposeResult{Value: 1}, nil
}

// applyBudGrant atomically allocates min(requested, available) and emits a lease.
// Idempotent for the lease's lifetime: a retry with the same leaseID (Items[0].Key) returns the existing
// lease, never re-debits. leaseID + Idem are NOT routed through the dedup ring (see idempotency design).
// B6: `holder` is recorded on the lease but NOT validated on later Report/Return — in Stage 1 the lease_id
// is the bearer capability. Holder binding/auth is Stage 3+ (design open Q6).
func (u *updateCtx) applyBudGrant(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 || len(c.Items[0].Val) < 8 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	amount := getI64(c.Items[0].Val[0:8])
	holder := c.Items[0].Val[8:]
	if amount < 0 { // B4: reject a negative draw (deterministic guard; never trust the caller)
		return ProposeResult{Data: budNoCapacity}, nil
	}
	// idempotency: existing lease row for this leaseID -> return its amount.
	if l, found, err := u.getLease(c.NS, c.Coll, leaseID); err != nil {
		return ProposeResult{}, err
	} else if found {
		return ProposeResult{Value: uint64(l.Amount), Data: encodeGrant(l.Amount)}, nil
	}
	p, found, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoBudget}, nil // B9: budget pool does not exist (distinct from budNoLease)
	}
	grant := amount
	if grant > p.Available {
		grant = p.Available
	}
	if grant <= 0 { // STRICT: nothing to give
		return ProposeResult{Data: budNoCapacity}, nil
	}
	p.Available -= grant
	p.LeasedOut += grant
	u.setPool(c.NS, c.Coll, p)
	u.setLease(c.NS, c.Coll, leaseID, leaseRec{Holder: append([]byte{}, holder...), Amount: grant, Spent: 0, Epoch: p.Epoch})
	return ProposeResult{Value: uint64(grant), Data: encodeGrant(grant)}, nil
}

// applyBudReport folds a cumulative-per-lease spent total (max), moving the delta leasedOut->spent.
func (u *updateCtx) applyBudReport(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 || len(c.Items[0].Val) < 8 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	l, found, err := u.getLease(c.NS, c.Coll, leaseID)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoLease}, nil
	}
	reported := getI64(c.Items[0].Val)
	if reported <= l.Spent { // stale/duplicate -> no-op (max fold)
		return ProposeResult{Value: uint64(l.Amount - l.Spent)}, nil
	}
	if reported > l.Amount { // clamp: a lease cannot spend more than granted (STRICT)
		reported = l.Amount
	}
	delta := reported - l.Spent
	l.Spent = reported
	p, _, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	p.LeasedOut -= delta
	p.Spent += delta
	u.setPool(c.NS, c.Coll, p)
	u.setLease(c.NS, c.Coll, leaseID, l)
	return ProposeResult{Value: uint64(l.Amount - l.Spent)}, nil
}

// applyBudReturn folds the final spent, returns the unspent remainder to available, deletes the lease.
func (u *updateCtx) applyBudReturn(c command) (ProposeResult, error) {
	if len(c.Items) == 0 || len(c.Items[0].Key) == 0 {
		return ProposeResult{}, errShortCommand
	}
	leaseID := c.Items[0].Key
	l, found, err := u.getLease(c.NS, c.Coll, leaseID)
	if err != nil {
		return ProposeResult{}, err
	}
	if !found {
		return ProposeResult{Data: budNoLease}, nil // already returned / unknown
	}
	// fold final report if present and larger
	if len(c.Items[0].Val) >= 8 {
		if reported := getI64(c.Items[0].Val); reported > l.Spent {
			if reported > l.Amount {
				reported = l.Amount
			}
			delta := reported - l.Spent
			l.Spent = reported
			p, _, _ := u.getPool(c.NS, c.Coll)
			p.LeasedOut -= delta
			p.Spent += delta
			u.setPool(c.NS, c.Coll, p)
		}
	}
	rem := l.Amount - l.Spent
	p, _, err := u.getPool(c.NS, c.Coll)
	if err != nil {
		return ProposeResult{}, err
	}
	p.Available += rem
	p.LeasedOut -= rem
	u.setPool(c.NS, c.Coll, p)
	u.delLease(c.NS, c.Coll, leaseID)
	return ProposeResult{Value: uint64(rem)}, nil
}

func encodeGrant(amount int64) []byte { b := make([]byte, 8); putI64(b, amount); return b }
```

> `errShortCommand` already exists (`command.go` uses it). If the exact name differs, grep for the short-command error in `command.go` and reuse it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/collections/ -run TestApplyBud`
Expected: PASS (all four).

- [ ] **Step 5: Commit**

```bash
git add internal/collections/budget.go internal/collections/budget_test.go
git commit -m "feat(collections): LeasedBudget apply logic (init/grant/report/return) with conservation"
```

---

## Task 5: Wire the ops into `applyOne` dispatch + type check

**Files:**
- Modify: `internal/collections/statemachine.go` (`applyOne` dispatch, lines 256-320; mirror the `opHIncrBy` branch at 287-320)
- Test: `internal/collections/budget_test.go`

- [ ] **Step 1: Write the failing test** — drive a command end-to-end through `Update`/`applyOne` (not the apply* functions directly) and assert a WRONGTYPE when a budget op hits a hash collection.

```go
func TestApplyOneBudgetDispatchAndWrongType(t *testing.T) {
	sm := newTestSM(t) // Task 7 helper: shardSM over MemStore with applied-index plumbing
	ns := []byte("pacing")
	// init then grant via the SM apply path
	mustApply(t, sm, initCmd(ns, []byte("li/1"), 1000))
	r := mustApply(t, sm, grantCmd(ns, []byte("li/1"), "A", 600))
	if decodeGrant(r.Data) != 600 {
		t.Fatalf("grant via applyOne = %d, want 600", decodeGrant(r.Data))
	}
	// a hash collection rejects budget ops with WRONGTYPE
	mustApply(t, sm, command{Op: opHSet, NS: ns, Coll: []byte("h"), Items: []item{{Key: []byte("f"), Val: []byte("1")}}})
	rw := mustApply(t, sm, grantCmd(ns, []byte("h"), "A", 1))
	if string(rw.Data) != string(wrongType) {
		t.Fatalf("budget op on hash Data = %q, want WRONGTYPE", rw.Data)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run TestApplyOneBudgetDispatch`
Expected: FAIL — budget ops fall through / no dispatch.

- [ ] **Step 3: Add the dispatch branch in `applyOne`**

In `applyOne`, after the existing `opHIncrBy/opHIncrByFloat` special-case block and before the generic `applyCommand`, add a budget block. Follow the exact shape of the HIncr branch: do the dedup check (for grant/report/return that carry `Idem`), the `ensureType(ns, coll, typeBudget)` check, then call the apply function:

```go
	case opBudInit, opBudGrant, opBudReport, opBudReturn:
		// type guard (opBudInit creates the type; others require it)
		if c.Op == opBudInit {
			if ok, err := u.ensureType(c.NS, c.Coll, typeBudget); err != nil {
				return ProposeResult{}, err
			} else if !ok {
				return ProposeResult{Data: wrongType}, nil
			}
		} else {
			if tp, err := u.typeOf(c.NS, c.Coll); err != nil {
				return ProposeResult{}, err
			} else if tp != 0 && tp != typeBudget {
				return ProposeResult{Data: wrongType}, nil
			}
		}
		switch c.Op {
		case opBudInit:
			return u.applyBudInit(c)
		case opBudGrant:
			return u.applyBudGrant(c)
		case opBudReport:
			return u.applyBudReport(c)
		case opBudReturn:
			return u.applyBudReturn(c)
		}
```

> The budget apply functions return `(ProposeResult, error)` directly, so the dispatch is a plain `return u.applyBudGrant(c)` — do **not** adapt to the `(int64,[]byte,error)` HIncr shape. Budget ops carry **no** `c.Idem`, so they are NOT routed through the generic dedup ring in `applyOne` (the dedup-on-`c.Idem` block is at `statemachine.go:~273-282`, before dispatch) — idempotency is enforced inside the apply functions (durable lease-row check for grant; `max`-fold for report; lease-absence for return). Insert this `case` block in the `applyOne` `switch` alongside the existing op cases; place it before the generic `applyCommand` fallthrough.
>
> Note: `decodeCommandInto` (`command.go:~228-234`) rejects ops where `typeForOp(op)==0` *except* `opExpire`/`opRemove`; since Task 1 puts the four budget ops in `typeForOp` (`command.go:~97-108`), they decode fine — Task 1 is a hard prerequisite for any end-to-end apply here.
>
> Behavior note: a grant against an **uninitialized** budget returns `budNoBudget` (→ `ErrBudgetNotFound`, B9), NOT WRONGTYPE. WRONGTYPE is only for a budget op hitting a set/hash/zset collection (the `typeOf` guard below). Don't "fix" the test to expect WRONGTYPE for grant-before-define.
>
> **Coalescing (review gap B7):** do **not** add the budget ops to `manager.go` `coalescable()` (`~line 247`) in Stage 1. Each budget op then commits as its own Raft entry, which is correct — the `u.vals` overlay still composes ops that *do* land in one batch, and STRICT can never over-grant because Raft serializes concurrent grants. Revisit in Stage 2 (design §6.2 makes grant/report coalescable, keeps control-plane ops atomic).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/collections/ -run TestApplyOneBudgetDispatch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/statemachine.go internal/collections/budget_test.go
git commit -m "feat(collections): dispatch LeasedBudget ops in applyOne with type guard"
```

---

## Task 6: Read path — `budStatQuery` + `budCheckQuery` Lookups

**Files:**
- Modify: `internal/collections/statemachine.go` (Lookup query dispatch, lines ~477-601)
- Modify: `internal/collections/budget.go`
- Test: `internal/collections/budget_test.go`

`BudgetStat` reads the pool via the existing Lookup mechanism (like `cardQuery`/`hGetQuery`). Reads do not affect safety — grants are the only safety-critical path. This task ALSO adds `budCheckQuery` (review gap B2, design §6.8): the conservation-invariant probe — the single cheapest, highest-value guardrail for a money datatype and the basis of the `StrictBudgetInvariantViolated` alert.

- [ ] **Step 1: Write the failing test**

```go
func TestBudStatQuery(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))
	res, err := sm.Lookup(budStatQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	st := res.(budStat)
	if st.Cap != 1000 || st.Available != 400 || st.LeasedOut != 600 {
		t.Fatalf("budStat = %+v, want cap1000 avail400 leased600", st)
	}
}

// B2: the conservation-invariant probe holds after a normal grant/report cycle.
func TestBudCheckQuery(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))
	mustApply(t, sm, reportCmd(ns, coll, "A", 250))
	res, err := sm.Lookup(budCheckQuery{NS: ns, Coll: coll})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	chk := res.(budCheck)
	if !chk.OK {
		t.Fatalf("invariant probe failed: %+v", chk)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run TestBudStatQuery`
Expected: FAIL — undefined `budStatQuery`.

- [ ] **Step 3: Implement the query**

In `budget.go`:

```go
type budStatQuery struct{ NS, Coll []byte }

type budStat struct {
	Exists                              bool
	Cap, Available, LeasedOut, Spent    int64
	Epoch                               uint64
	Mode                                uint8
}

// lookupBudStat reads the pool from a snapshot (called by shardSM.Lookup).
// Reads directly from the snapshot exactly like snapCard / hGetQuery (statemachine.go:479,576-577):
// snap.Get(storage.CFReplData, key) -> (value, found, err). There is NO getDataSnap helper.
func (s *shardSM) lookupBudStat(snap storage.Snapshot, q budStatQuery) (budStat, error) {
	v, found, err := snap.Get(storage.CFReplData, s.budPoolKey(q.NS, q.Coll))
	if err != nil || !found {
		return budStat{Exists: false}, err
	}
	p, err := decodePool(v)
	if err != nil {
		return budStat{}, err
	}
	return budStat{Exists: true, Cap: p.Cap, Available: p.Available, LeasedOut: p.LeasedOut, Spent: p.Spent, Epoch: p.Epoch, Mode: p.Mode}, nil
}

// --- B2: conservation-invariant probe (design §6.8) ---
type budCheckQuery struct{ NS, Coll []byte }

// budCheck reports whether INV-LOCAL holds for the pool, with the buckets for diagnostics.
type budCheck struct {
	Exists                            bool
	OK                                bool // available+leasedOut+spent == cap AND spent <= cap AND no bucket < 0
	Cap, Available, LeasedOut, Spent  int64
}

// lookupBudCheck reads ONE consistent snapshot and asserts the conservation invariant. Read-only; the
// caller (BudgetStat-style API + the metrics StrictBudgetInvariantViolated alert) decides how to react.
func (s *shardSM) lookupBudCheck(snap storage.Snapshot, q budCheckQuery) (budCheck, error) {
	v, found, err := snap.Get(storage.CFReplData, s.budPoolKey(q.NS, q.Coll))
	if err != nil || !found {
		return budCheck{Exists: false}, err
	}
	p, err := decodePool(v)
	if err != nil {
		return budCheck{}, err
	}
	ok := p.Available+p.LeasedOut+p.Spent == p.Cap &&
		p.Spent <= p.Cap && p.Available >= 0 && p.LeasedOut >= 0 && p.Spent >= 0
	return budCheck{Exists: true, OK: ok, Cap: p.Cap, Available: p.Available, LeasedOut: p.LeasedOut, Spent: p.Spent}, nil
}
```

In `statemachine.go` `Lookup`, add the cases (read directly from the passed `snap` via `snap.Get(storage.CFReplData, key)`, exactly like `cardQuery`/`hGetQuery` — there is NO `getDataSnap` helper):

```go
	case budStatQuery:
		return s.lookupBudStat(snap, q)
	case budCheckQuery:
		return s.lookupBudCheck(snap, q)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/collections/ -run TestBudStatQuery`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/statemachine.go internal/collections/budget.go internal/collections/budget_test.go
git commit -m "feat(collections): BudgetStat + BudgetCheck (invariant probe) Lookup queries"
```

---

## Task 7: Test harness helpers (SM + updateCtx + integration)

**Files:**
- Modify: `internal/collections/budget_test.go`

Add the helpers the earlier tests reference (`newTestUpdateCtx`, `newTestSM`, `mustApply`). Model them on `hincr_test.go` / `collections_test.go` (`freeAddr`, `newMgr`, `waitReady`, `storage.NewMemStore()`).

- [ ] **Step 1: Implement helpers** (no new behavior; enables the prior tests to compile/run without temporary inline constructors)

```go
// newTestSM builds a shardSM over an in-memory store with the same prefix/applied plumbing tests use.
// NOTE: there is NO `shardPrefix()` helper. baseSM.prefix is the 8-byte big-endian encoding of shardID
// (base_sm.go:26-29). Build it the same way base_sm.go does — either reuse the real baseSM constructor if
// the package exports one, or derive the prefix inline as below.
func newTestSM(t *testing.T) *shardSM {
	t.Helper()
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	const shardID uint64 = 2
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, shardID) // 8-byte BE shardID, exactly as base_sm.go builds it
	return &shardSM{baseSM: baseSM{store: mem, shardID: shardID, prefix: prefix}}
}

// newTestUpdateCtx opens an updateCtx with ALL overlay maps initialized (match the literal in
// statemachine.go:181-185 exactly — exists, zscore, cardDelta, htype, vals, inBatchDedup). Writing to a
// nil map (e.g. ensureType -> u.htype[cs]=want) panics, so every map must be non-nil even though the
// budget apply funcs only touch `vals`.
func newTestUpdateCtx(t *testing.T) *updateCtx {
	t.Helper()
	return &updateCtx{
		s:            newTestSM(t),
		exists:       map[string]bool{},
		zscore:       map[string]*float64{},
		cardDelta:    map[string]int64{},
		htype:        map[string]collType{},
		vals:         map[string][]byte{},
		inBatchDedup: map[string]ProposeResult{},
	}
}

// mustApply runs one command through the SM apply path and returns the ProposeResult.
func mustApply(t *testing.T, sm *shardSM, c command) ProposeResult {
	t.Helper()
	enc := encodeCommand(c)
	// drive through the same entry point Update/applyOne uses for a single entry
	r, err := applySingleForTest(sm, enc)
	if err != nil {
		t.Fatalf("apply %v: %v", c.Op, err)
	}
	return r
}
```

> **`applySingleForTest` does NOT exist yet — it MUST be added** (review gap B10). There is no single-entry
> test seam in the package today. Add a small **unexported** helper in `statemachine.go` (no production
> callers) that mirrors what `Update` does for one entry: construct the `updateCtx` (with all overlay maps,
> matching the literal in `statemachine.go:~188-192`), call `applyOne(enc, nil, scratch)`, then flush
> `u.ops` to the store via the same batch-commit path `Update` uses, so the next `mustApply`/`Lookup` sees
> committed state. Inspect `hincr_test.go` and the real `Update` body to copy the exact flush call (do not
> invent a `BatchRC` name — use whatever `Update` actually calls). Keep it unexported/test-only.

- [ ] **Step 2: Run the full budget test suite**

Run: `go test ./internal/collections/ -run TestBud -v && go test ./internal/collections/ -run TestApplyBud -v`
Expected: PASS for all budget tests.

- [ ] **Step 3: Run the whole package to check no regressions**

Run: `go test ./internal/collections/...`
Expected: PASS (existing Set/Hash/ZSet tests still green).

- [ ] **Step 4: Commit**

```bash
git add internal/collections/budget_test.go internal/collections/statemachine.go
git commit -m "test(collections): LeasedBudget test harness + green suite"
```

---

## Task 8: Typed API on `Collections`

**Files:**
- Modify: `internal/collections/collections.go` (mirror `HIncrBy` at `collections.go:287-298`)
- Modify: `internal/collections/command.go` (the error `var` block — there is NO `errors.go`)
- Test: `internal/collections/budget_test.go` (integration test through a real Raft shard, like `hincr_test.go`)

- [ ] **Step 1: Write the failing integration test** (real Manager + shard, mirrors `hincr_test.go` setup exactly)

```go
func TestBudgetEndToEnd(t *testing.T) {
	mem := storage.NewMemStore()
	t.Cleanup(func() { _ = mem.Close() })
	addr := freeAddr(t)
	m := newMgr(t, t.TempDir(), addr, mem)
	if err := m.StartShard(2, 1, map[uint64]string{1: addr}, false); err != nil {
		t.Fatalf("StartShard: %v", err)
	}
	defer m.Stop()
	c := New(m, SingleShardDirectory(2))
	waitReady(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ns, coll := []byte("pacing"), []byte("li/42/total")

	if err := c.BudgetDefine(ctx, ns, coll, 1000, modeStrict); err != nil {
		t.Fatalf("define: %v", err)
	}
	g, err := c.BudgetGrant(ctx, ns, coll, []byte("node-A"), 600, []byte("lease-A1"))
	if err != nil || g != 600 {
		t.Fatalf("grant = %d, %v want 600", g, err)
	}
	if err := c.BudgetReport(ctx, ns, coll, []byte("lease-A1"), 250); err != nil {
		t.Fatalf("report: %v", err)
	}
	st, err := c.BudgetStat(ctx, ns, coll, true)
	if err != nil || st.Available != 400 || st.LeasedOut != 350 || st.Spent != 250 {
		t.Fatalf("stat = %+v, %v want avail400 leased350 spent250", st, err)
	}
	if st.Available+st.LeasedOut+st.Spent != st.Cap {
		t.Fatalf("INV violated through Raft: %+v", st)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/collections/ -run TestBudgetEndToEnd`
Expected: FAIL — undefined `BudgetDefine` etc.

- [ ] **Step 3: Implement the typed API** in `collections.go` (mirror `HIncrBy`'s `proposeCmd` usage at `collections.go:287-298`):

```go
// BudgetDefine creates a STRICT escrow pool with the given cap (micro-units). Stage 1 is STRICT-only.
func (c *Collections) BudgetDefine(ctx context.Context, ns, coll []byte, cap int64, mode uint8) error {
	k := make([]byte, 1+8*3)
	k[0] = mode
	putI64(k[1:], cap)
	putI64(k[9:], 0)    // rate (Stage 1: none)
	putI64(k[17:], cap) // burst = cap
	_, data, err := c.proposeCmd(ctx, command{Op: opBudInit, NS: ns, Coll: coll, Items: []item{{Key: k}}})
	if err != nil {
		return err
	}
	switch string(data) {
	case string(budExists):
		return ErrBudgetExists
	case string(budBadMode): // B3/B4: non-STRICT mode or invalid cap
		return ErrUnsupportedMode
	}
	return nil
}

// BudgetGrant atomically leases up to `amount`; leaseID makes it idempotent (for the lease's lifetime).
// leaseID rides in Items[0].Key; Val = amount(8B)||holder; Idem is left EMPTY (see Task 4 idempotency design).
func (c *Collections) BudgetGrant(ctx context.Context, ns, coll, holder []byte, amount int64, leaseID []byte) (int64, error) {
	v := make([]byte, 8+len(holder))
	putI64(v, amount)
	copy(v[8:], holder)
	n, data, err := c.proposeCmd(ctx, command{Op: opBudGrant, NS: ns, Coll: coll, Items: []item{{Key: leaseID, Val: v}}})
	if err != nil {
		return 0, err
	}
	switch string(data) {
	case string(budNoCapacity):
		return 0, ErrNoCapacity
	case string(budNoBudget):
		return 0, ErrBudgetNotFound // B9: budget pool does not exist
	case string(budNoLease):
		return 0, ErrLeaseUnknown
	}
	return int64(n), nil
}

// BudgetReport folds a cumulative-per-lease spent total (idempotent, max). leaseID in Items[0].Key, no Idem.
func (c *Collections) BudgetReport(ctx context.Context, ns, coll, leaseID []byte, spentCumulative int64) error {
	v := make([]byte, 8)
	putI64(v, spentCumulative)
	_, data, err := c.proposeCmd(ctx, command{Op: opBudReport, NS: ns, Coll: coll, Items: []item{{Key: leaseID, Val: v}}})
	if err != nil {
		return err
	}
	if string(data) == string(budNoLease) {
		return ErrLeaseUnknown
	}
	return nil
}

// BudgetReturn releases the unspent remainder (folding a final cumulative spent) and removes the lease.
func (c *Collections) BudgetReturn(ctx context.Context, ns, coll, leaseID []byte, finalSpent int64) error {
	v := make([]byte, 8)
	putI64(v, finalSpent)
	_, data, err := c.proposeCmd(ctx, command{Op: opBudReturn, NS: ns, Coll: coll, Items: []item{{Key: leaseID, Val: v}}})
	if err != nil {
		return err
	}
	if string(data) == string(budNoLease) {
		return ErrLeaseUnknown
	}
	return nil
}

// BudgetStat reads pool accounting (bounded-stale unless linearizable).
func (c *Collections) BudgetStat(ctx context.Context, ns, coll []byte, linearizable bool) (budStat, error) {
	v, err := c.read(ctx, ns, coll, budStatQuery{NS: ns, Coll: coll}, linearizable)
	if err != nil {
		return budStat{}, err
	}
	st, _ := v.(budStat)
	return st, nil
}
```

Add to the existing `var (...)` error block in **`command.go`** (there is NO `errors.go`; `ErrWrongType`/`ErrNotNumber`/`ErrBusy`/`errShortCommand` all live in `command.go:21-39`):

```go
var (
	ErrNoCapacity      = errors.New("collections: budget has no capacity to grant")
	ErrBudgetExists    = errors.New("collections: budget already exists")
	ErrBudgetNotFound  = errors.New("collections: budget not found")              // B9: grant before define
	ErrLeaseUnknown    = errors.New("collections: lease unknown")                 // report/return on a missing lease
	ErrUnsupportedMode = errors.New("collections: budget mode not supported in stage 1") // B3: non-STRICT
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/collections/ -run TestBudgetEndToEnd`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/collections/collections.go internal/collections/command.go internal/collections/budget_test.go
git commit -m "feat(collections): typed LeasedBudget API (Define/Grant/Report/Return/Stat)"
```

---

## Task 9: Proto — `BudgetService` (Stage-1 subset)

**Files:**
- Create: `proto/wavespan/v1/budget.proto`
- Modify: regenerate stubs

Mirror `collections.proto` style and the `common.proto` `ResponseMeta`. Stage-1 RPCs only.

- [ ] **Step 1: Write `budget.proto`**

```proto
syntax = "proto3";
package wavespan.v1;
import "wavespan/v1/common.proto";

// BudgetService is the LeasedBudget escrow API (Stage 1: single-cluster STRICT).
// Mutations are linearizable through the owning shard leader (like CollectionService).
service BudgetService {
  rpc BudgetDefine(BudgetDefineRequest) returns (BudgetStatResult);
  rpc BudgetGrant(BudgetGrantRequest)   returns (BudgetGrantResult);
  rpc BudgetReport(BudgetReportRequest) returns (BudgetStatResult);
  rpc BudgetReturn(BudgetReturnRequest) returns (BudgetStatResult);
  rpc BudgetStat(BudgetStatRequest)     returns (BudgetStatResult);
}

enum BudgetMode { BUDGET_MODE_UNSPECIFIED = 0; BUDGET_MODE_STRICT = 1; BUDGET_MODE_RELAXED = 2; }

message BudgetDefineRequest {
  string namespace = 1; bytes budget = 2;
  int64  cap_units = 3; BudgetMode mode = 4;
  optional string idempotency_key = 5;
}
message BudgetGrantRequest {
  string namespace = 1; bytes budget = 2;
  string holder_id = 3; int64 amount_units = 4;
  bytes  lease_id = 5; // idempotency key for the grant
}
message BudgetGrantResult {
  ResponseMeta meta = 1; int64 granted_units = 2; bool partial = 3; bool no_capacity = 4;
}
message BudgetReportRequest {
  string namespace = 1; bytes budget = 2; bytes lease_id = 3; int64 spent_cumulative = 4;
}
message BudgetReturnRequest {
  string namespace = 1; bytes budget = 2; bytes lease_id = 3; int64 spent_cumulative = 4;
}
message BudgetStatRequest { string namespace = 1; bytes budget = 2; bool linearizable = 3; }
message BudgetStatResult {
  ResponseMeta meta = 1; bool exists = 2;
  int64 cap_units = 3; int64 available_units = 4; int64 leased_out_units = 5; int64 spent_units = 6;
  uint64 epoch = 7; BudgetMode mode = 8;
}
```

- [ ] **Step 2: Regenerate stubs and verify they compile**

Run: `buf generate` (server stubs into `proto/wavespan/v1/`). If a `justfile` target exists, prefer `just proto`.
Expected: new `budget.pb.go` / `budget_grpc.pb.go`; `go build ./...` succeeds.

- [ ] **Step 3: Commit**

```bash
git add proto/wavespan/v1/budget.proto proto/wavespan/v1/budget*.pb.go
git commit -m "feat(proto): BudgetService Stage-1 RPCs"
```

---

## Task 10: Connect service handlers + gRPC adapter

**Files:**
- Modify: `internal/collections/service.go` (mirror the `HIncrBy` handler at `service.go:~172-186`; the `collErr` error-mapping switch is at `service.go:~79-92`)
- Modify: `internal/grpcsrv/collections.go` (mirror the delegation pattern; `NewCollections` adapter at `grpcsrv/collections.go:23`) — or a new `internal/grpcsrv/budget.go` if `BudgetService` is a separate gRPC service registration. Registration mirrors `cmd/wavespan-node/main.go:712` (`RegisterCollectionServiceServer`).
- Test: `internal/grpcsrv` or `internal/collections` handler test (mirror existing service tests)

- [ ] **Step 1: Write the failing handler test** — a Connect-level test that calls `Service.BudgetDefine`/`BudgetGrant` and asserts the response + that `ErrNoCapacity` maps to `ResourceExhausted`, `ErrBudgetExists` to `FailedPrecondition`. Model on existing service handler tests.

- [ ] **Step 2: Run to verify it fails.**

Run: `go test ./internal/collections/ -run TestService.*Budget`
Expected: FAIL — handlers undefined.

- [ ] **Step 3: Implement the Connect handlers** in `service.go`, each building the typed call and mapping errors. Extend the `collErr` switch (`service.go:~79-92`):

```go
	case errors.Is(err, ErrNoCapacity):
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, ErrBudgetExists):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrBudgetNotFound): // B9
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrUnsupportedMode): // B3
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrLeaseUnknown):
		return connect.NewError(connect.CodeFailedPrecondition, err)
```

Add handler methods, e.g.:

```go
func (s *Service) BudgetGrant(ctx context.Context, req *connect.Request[wavespanv1.BudgetGrantRequest]) (*connect.Response[wavespanv1.BudgetGrantResult], error) {
	m := req.Msg
	n, err := s.cols.BudgetGrant(ctx, []byte(m.GetNamespace()), m.GetBudget(), []byte(m.GetHolderId()), m.GetAmountUnits(), m.GetLeaseId())
	if err != nil {
		if errors.Is(err, ErrNoCapacity) {
			return connect.NewResponse(&wavespanv1.BudgetGrantResult{NoCapacity: true}), nil // no-capacity is a normal result, not an error
		}
		return nil, collErr(err) // the existing service.go error mapper (service.go:78); extend its switch, do not add a new func
	}
	return connect.NewResponse(&wavespanv1.BudgetGrantResult{GrantedUnits: n, Partial: n < m.GetAmountUnits()}), nil
}
```

Implement `BudgetDefine/BudgetReport/BudgetReturn/BudgetStat` analogously. Then add the gRPC adapter methods in `grpcsrv` delegating via `connect.NewRequest`, and register `BudgetService` in the gRPC server wiring (find where `CollectionService` is registered in `cmd/wavespan-node` / `internal/grpcsrv` and add the budget server).

- [ ] **Step 4: Run handler + full package tests**

Run: `go test ./internal/collections/... ./internal/grpcsrv/...`
Expected: PASS.

- [ ] **Step 5: Build the node binary to confirm wiring**

Run: `go build ./...`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add internal/collections/service.go internal/grpcsrv/ proto/
git commit -m "feat(grpc): expose BudgetService Stage-1 handlers"
```

---

## Task 11: Stage-1 verification gate

**Files:** none (verification only)

- [ ] **Step 1: Full test + race**

Run: `go test -race ./internal/collections/... ./internal/grpcsrv/...`
Expected: PASS, no races.

- [ ] **Step 2: Property/fuzz the conservation invariant** — add a randomized test that interleaves define/grant/report/return with random (incl. negative and oversized) amounts and leaseIDs across several holders and asserts `assertInv` after every op and that `Spent ≤ Cap` always. Drive negative/oversized inputs too, to exercise the B4 guards.

```go
func TestBudgetConservationFuzz(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 10_000))
	// deterministic PRNG seeded from a constant (Math.random/time not used)
	r := newDetRand(12345)
	leases := map[string]int64{}
	for i := 0; i < 5000; i++ {
		// ... random op (include negative/oversized amounts to hit B4 guards);
		// after each op: p,_,_ := u.getPool(ns,coll); assertInv(t, p)
	}
}
```

- [ ] **Step 2b: B5 — assert the single-use-lease_id hazard is real (documented, not silent)**

```go
// Stage-1 limitation: a lease_id reused after Return grants FRESH quantity (no tombstone yet).
// This test pins the contract so the limitation can never be mistaken for idempotency.
func TestGrantAfterReturnReusesIdRegrantsFreshQuantity(t *testing.T) {
	u := newTestUpdateCtx(t)
	ns, coll := []byte("p"), []byte("c")
	_, _ = u.applyBudInit(initCmd(ns, coll, 1000))
	_, _ = u.applyBudGrant(grantCmd(ns, coll, "A", 600))
	_, _ = u.applyBudReturn(returnCmd(ns, coll, "A", 0)) // unspent 600 returns; lease row deleted
	r, _ := u.applyBudGrant(grantCmd(ns, coll, "A", 600)) // SAME id -> fresh grant (the hazard)
	if decodeGrant(r.Data) != 600 {
		t.Fatalf("expected fresh re-grant of 600 (Stage-1 limitation), got %d", decodeGrant(r.Data))
	}
	p, _, _ := u.getPool(ns, coll)
	assertInv(t, p) // conservation still holds; it's the caller contract (single-use id) that's at stake
}
```

- [ ] **Step 2c: B8 — snapshot round-trip** — prove budget state survives snapshot/restore (it's money; `base_sm` snapshots the whole shard prefix, so budget keys ride along — assert it).

```go
func TestBudgetSurvivesSnapshotRestore(t *testing.T) {
	sm := newTestSM(t)
	ns, coll := []byte("pacing"), []byte("li/1")
	mustApply(t, sm, initCmd(ns, coll, 1000))
	mustApply(t, sm, grantCmd(ns, coll, "A", 600))
	// snapshot -> fresh SM -> restore, exactly as base_sm.go's Save/RecoverFromSnapshot are exercised
	// in the existing collection snapshot test (mirror that harness).
	restored := snapshotRoundTrip(t, sm) // helper: Save to a buffer, build newTestSM, Recover from it
	res, _ := restored.Lookup(budCheckQuery{NS: ns, Coll: coll})
	chk := res.(budCheck)
	if !chk.Exists || !chk.OK || chk.Available != 400 || chk.LeasedOut != 600 {
		t.Fatalf("post-restore budCheck = %+v, want exists/OK avail400 leased600", chk)
	}
}
```

> Wire `snapshotRoundTrip` to `base_sm.go`'s real snapshot API (the existing Set/Hash snapshot test shows the exact `SaveSnapshot`/`RecoverFromSnapshot` calls and stream type — copy them; do not invent names).

- [ ] **Step 3: Verify the build & lint**

Run: `go vet ./internal/collections/...` and the repo's lint target (`just lint` / `golangci-lint run` if configured).
Expected: clean.

- [ ] **Step 4: Update design doc status** — in `design/35_leased_budget_datatype.md` §14, mark Stage 1 as implemented and note the `leasedOut = unspent-in-leases` refinement (3-term conservation in Stage 1; the 5-term form with `pendingReclaim` arrives with recall in Stage 3).

- [ ] **Step 5: Commit**

```bash
git add internal/collections/budget_test.go design/35_leased_budget_datatype.md
git commit -m "test(collections): conservation fuzz + Stage-1 verification gate"
```

---

## Stages 2–4 roadmap (separate plans)

Each becomes its own plan after Stage 1 is verified:

- **Stage 2 — Pacing + expiry + node cache.** Add `rate/burst` token-bucket gating to `applyBudGrant` using a **leader-stamped `grantedMs`** (stamp before propose, exactly as `sweepOnce` stamps TTL time at `manager.go:~358`, `now := time.Now().UnixMilli()`; never read wall-clock in apply). Add a lease-expiry index at `scopeBudExp 0x07` (reuse `ttl.go` layout) and a second pass in `sweepOnce` that proposes `opBudExpire` to reclaim stranded leases. Build the node-side lease cache in the SDK (`LeasedBudgetClient.Acquire → Budget.Spend/refill`, single-flight refill, low-watermark hysteresis, crash waste ≤ 2·chunk).
- **Stage 3 — Steering + recall.** `opBudSteer` (+ epoch bump), the `pendingReclaim` bucket and **invalidate-now / reclaim-after-settle** recall with the §6.5 holder-stop guard band (holder stops at `deadline − selfGuard`; grantor reclaims at `+2·skew + maxPauseBudget`; self-fencing), hard cap-cut enforcement, `BudgetWatch` push stream. This is where the conservation equation becomes the 5-term form.
- **Stage 4 — RELAXED + hierarchy.** RELAXED bounded overdraft + proportional-throttle reconciliation (§9.2), then the 3-level hierarchy and the cross-cluster request/grant RPC (a remote grant against the home-region L0 Raft group via `WithForwarder` — NOT the LWW global mirror).

## vires integration (after Stage 4)

Per `design/34` Phase 3 + `design/35` §15: pacing/budget #2 → STRICT `LeasedBudget` (`namespace=pacing`, `budget=li/<li>/total` and `li/<li>/daily/<date>`); frequency caps #1 → RELAXED (or stay on approximate `HIncrBy` until overcount proves too loose).

---

## Notes for the implementing engineer

- **TDD discipline:** every task is test-first; run the failing test before implementing.
- **Line numbers are approximate** (they drift as the tree changes) — treat every `file.go:NNN` as a hint and grep for the symbol. The symbol *names* below, however, are verified against the current tree.
- **Verified code facts (do NOT re-derive):**
  - There is **no `errors.go`** — all errors live in `command.go` (`ErrWrongType`/`ErrNotNumber`/`ErrBusy`/`errShortCommand` etc.).
  - There is **no `shardPrefix()`** — `baseSM.prefix` is the 8-byte BE of `shardID` (`base_sm.go:26-29`); build it inline (Task 7).
  - There is **no `getDataSnap`** — `Lookup` reads the passed snapshot directly via `snap.Get(storage.CFReplData, key)` (Task 6).
  - There is **no single-entry apply seam** — `applySingleForTest` MUST be added as an unexported helper (Task 7, gap B10).
  - `storage.StoreOp` fields are `{CF, Key, Value, Delete, ExpiresAtUnixMs}` (`store.go:34-43`); budget uses `CF/Key/Value/Delete`.
  - `item` has `{Key, Val, Score, ExpiryMs}` (`command.go:110-127`); budget uses only `Key/Val`.
  - Under `collScope`, bytes `0x00`–`0x04` are taken (`scopeCard/Elem/ZPtr/Type` + `scopeTTLPtr 0x04`); `0x05+` is free.
  - `decodeCommandInto` (`command.go:~228-234`) rejects `typeForOp(op)==0` *except* `opExpire`/`opRemove`; budget ops are in `typeForOp` (Task 1).
  - Key landmarks: `HIncrBy` `collections.go:287-298`; `sweepOnce` `manager.go:~358`; `coalescable` `manager.go:~247`; `collErr` `service.go:~79-92`; service registration `cmd/wavespan-node/main.go:712`.
- **No wall-clock in apply.** Stage 1 has no time dependency at all; keep it that way (pacing/expiry time arrives leader-stamped in Stage 2).
- **Reference skills:** @superpowers:test-driven-development, @superpowers:subagent-driven-development.
```
