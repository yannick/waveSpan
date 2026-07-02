# 34. Replacing Redis in vires with waveSpan

> Status: design proposal · Audience: waveSpan engine team + vires platform team
> Scope: catalogue every Redis use in **vires**, map each onto **waveSpan**
> (via `wavespan-sdk`), and specify exactly which engine features must be added or
> hardened to make a full migration possible.

## 1. Summary & recommendation

vires (the Go ad-serving platform) uses Redis through a single client seam
(`pkg/client/redisio/Client`) for **caching, real-time counters, deduplication,
audience-segment lookup, and a Lua-scripted page-match cache**. There is no Redis
pub/sub, no Streams, no Sentinel/Cluster, and no `WATCH`/`MULTI`/`EXEC` — the only
atomicity primitive in use is Lua scripting (one cache).

waveSpan can absorb the **majority** of this today, and the rest with a small,
well-scoped set of new primitives:

- **KV tier** (eventual, origin+1 durable) cleanly covers the pure cache + TTL use
  cases (creative data, prebid responses, DigiSeg segments).
- **Collections tier** (CP/Raft, linearizable; `HIncrBy`/`HIncrByFloat`, sets,
  sorted sets, `BulkRemove`) covers the hash counters and set membership, and — via
  `BulkRemove` — lets us **redesign the Lua match cache with no scripting**.
- The genuine gaps are: **scalar atomic counters with TTL (G1)**, **atomic
  test-and-set / CAS (G2)**, **TTL precision/visibility (G3)**, the **match-cache
  redesign (G4)**, **Collections-tier hardening (G5)**, a **`KEYS`→`Scan`
  convention (G6)**, and **client/metrics ergonomics (G7)**.

**Recommended consistency model — tiered:**
- *Approximate, eventually-consistent counters* on the per-request hot path
  (frequency capping, creative stats, delivery stats). A small overcount under
  concurrency is acceptable for caps and reporting.
- *Exact, linearizable CP-tier counters* only where overspend costs money
  (pacing / budget).

**Recommended approach:** introduce a backend-agnostic interface at the existing
`redisio.Client` seam, then migrate use case by use case in phases gated on the
features below. No code is changed by this document; the migration phases (§6) are
the execution plan.

---

## 2. vires Redis inventory

**Client library:** `github.com/gomodule/redigo v1.9.2`.

**Seam:** `pkg/client/redisio/Client` exposes `ReadConn`/`WriteConn` and
`ReadConnToDB`/`WriteConnToDB`/`Close`. It wraps `redigo` pools
(`pkg/infra/redigo/{config,pool,dial}.go`) with:
- **master/slave read-splitting** — reads go to a slave when one is configured and
  synced (`waitForSlaves`, `SlavesReady`, INFO-based sync check in `info.go`);
- **per-DB pools** — a `map[int]*redis.Pool` for masters and slaves, DBs `0–15`;
- **Prometheus pool metrics** — `pkg/client/redisio/metrics.go`
  (`vires_redis_conns_total`, waits, dials).

There is no Sentinel/Cluster support; logical separation is by Redis DB number.

### 2.1 Use-case catalogue

| # | Use case | File | Structure / commands | Keys | TTL | Hot path | Consistency need |
|---|----------|------|----------------------|------|-----|----------|------------------|
| 1 | Frequency capping | `pkg/data/cache/frequency.go` | HASH `HINCRBY`/`HGET`/`EXPIRE` | `frequency/<cwID>`, field `Name/Type/CreativeID` | 24h | **yes** | approx OK |
| 2 | Pacing / budget | `pkg/data/cache/pacing.go` | HASH `HINCRBY`/`HSET`/`HGET`/`DEL` | `pacing`, fields `<li>:totalSpent`, `<li>:dailySpent:<date>`, `latestEvent` | none | **yes** | **exact (money)** |
| 3 | Creative stats | `pkg/data/cache/creative_stats.go` | HASH `HINCRBY`/`HSET`/`HGET`/`DEL` | `creativeStats`, field `<creative>/<date>/<event>` | L1 only | **yes** | approx OK |
| 4 | Delivery stats | `pkg/data/cache/delivery_stats.go` | HASH `HINCRBY` (global/placement/line-item) `HSET`/`HGET`/`DEL` | `deliveryStats`, hierarchical field | L1 only | **yes** | approx OK |
| 5 | Creative data | `pkg/data/cache/creative_data.go` | HASH `HSET`/`HGET`/`HDEL`/`DEL` (JSON) | `creativeData`, field `<creative>` | L1 only | yes (read) | persistent KV |
| 6 | Generic counter | `pkg/components/cache/counting.go` | STRING `INCR`/`INCRBY`/`EXPIRE`/`GET`/`KEYS`/`DEL` | `<prefix><key>` | configurable | varies | approx OK; **`KEYS` unsafe** |
| 7 | Set cache | `pkg/components/cache/set.go` | SET `SADD`/`SMEMBERS`/`SREM`/`EXPIRE` | `<prefix><key>` | configurable | varies | membership |
| 8 | Prebid cache | `delivery/prebid/cache.go` | STRING `SET`/`GET`/`EXPIRE` (JSON) | `placementData/<placement>` | ~30m–1h | **yes** | KV + TTL |
| 9 | Win debounce | `delivery/prebid/debounce_cache.go` | STRING `GETSET` + `EXPIRE`, DB 10 | `winSeen:<id>` | 1 min | **yes** | **atomic test-and-set** |
| 10 | DigiSeg segments | `delivery/adserver/strategy/segments/digiseg/segment.go` | STRING `SET … EX 300`/`GET`, DB 15 | `<ip>` | 5 min | **yes** | KV + TTL |
| 11 | Article fetcher state | `service/article-scraper/fetcher/cache.go` | HASH state + STRING `INCR`/`EXPIRE` (seen) + STRING debounce | `/fetcher/data/<id>`, `fetcher/debounce/<id>`, `fetcher/seen/<id>/<date>` | seen 3d, debounce cfg | no | persistent + ephemeral; falls back to GORM DB |
| 12 | Page-match cache | `pkg/logic/match/cache.go` | **Lua (`EVALSHA`)** over HASH + SET, plus `HGETALL` | `+<hash>`/`-<hash>` (HASH), `M<id>`/`N<id>` (SET) | 24h | **yes (read)** | multi-key atomic add/invalidate |
| — | L1 in-memory | most caches above | `ttlcache/v3` in front of Redis | — | per-cache | — | keep as-is (L1 unchanged) |

**Cardinality hints:** frequency `O(users × events)`; pacing `O(lineItems × dates)`;
delivery/creative stats `O(placements×lineItems×events)` / `O(creatives×dates×events)`;
prebid `O(placements)`; debounce `O(wins)`; DigiSeg `O(distinct IPs)`; match
`O(articles × pageMatches)`.

**Tech-debt flagged in the code:** `KEYS` in counting.go (production-unsafe scan)
and `GETSET` in debounce_cache.go (deprecated; should be `SET NX`-style logic).

---

## 3. waveSpan capability mapping

waveSpan tiers (from `wavespan-sdk` + `design/03`, `design/30`):

- **KV tier** — `Put`/`Get`/`MultiGet`/`Delete`/`Scan`; write opts `WithTTL(ms)`,
  `WithIdempotencyKey`, `WithoutOriginPlusOne`; read opts `WithHideExpired`,
  `WithoutDynamicCache`; scan modes (cache-fast / cache-complete / routed-eventual).
  Eventual consistency, origin+1 durability. **TTL is lazy/best-effort** (sweeper +
  `hideExpiredOnRead`).
- **Collections tier** (CP/Raft, design 30; SDK contract complete in
  `collections.go`, **server maturity M30 in-progress**) — Sets (`SAdd`/`SAddTTL`/
  `SRem`/`SIsMember`/`SCard`/`SMembers`), Hashes (`HSet`/`HDel`/`HGet`/`HLen`/
  `HGetAll`/**`HIncrBy`**/**`HIncrByFloat`**), Sorted Sets (`ZAdd`/`ZRem`/`ZScore`/
  `ZCard`/`ZRange`), **`BulkRemove`** (remove members from many/all collections in a
  namespace), idempotency keys on writes, `linearizable` flag on reads.
- **Vector / Graph** tiers exist but are not needed for Redis parity.

**Structural mappings used throughout:**
- Redis **logical DB number → waveSpan namespace** (e.g. DB 10 → `prebid-debounce`,
  DB 15 → `digiseg`). Namespaces also carry per-namespace conflict policy,
  replication factor, and `hideExpiredOnRead`.
- Redis **master/slave read-splitting → waveSpan read mode**: bounded-stale local
  reads (the default) replace "read from slave"; `linearizable=true` replaces
  "read from master when correctness matters".
- **L1 `ttlcache/v3` stays unchanged** in front of whichever backend; only the L2
  Redis calls are replaced.

### 3.1 Per-use-case target

| # | Use case | waveSpan target | Consistency | TTL strategy | Needs feature |
|---|----------|-----------------|-------------|--------------|---------------|
| 1 | Frequency | Collections hash `HIncrBy` **or** KV approx counter | eventual/approx | namespace TTL 24h | G3, (G1 if KV) |
| 2 | Pacing/budget | Collections hash `HIncrBy`/`HIncrByFloat`, linearizable | **CP exact** | none / `latestEvent` via `HSet` | **G5** |
| 3 | Creative stats | Collections hash `HIncrBy` (approx, non-linearizable read) | eventual/approx | L1 only | G5 (throughput) |
| 4 | Delivery stats | Collections hashes `HIncrBy` ×3 levels | eventual/approx | L1 only | G5 (throughput) |
| 5 | Creative data | KV `Put`/`Get` (JSON value), or Collections hash | eventual | L1 only | — |
| 6 | Generic counter | KV scalar counter + TTL; list via `Scan` | eventual/approx | per-key TTL | **G1, G6** |
| 7 | Set cache | Collections set `SAddTTL`/`SMembers`/`SRem` | eventual/CP | `SAddTTL` | G3 |
| 8 | Prebid cache | KV `Put(WithTTL)`/`Get` | eventual | KV TTL ~30–60m | G3 |
| 9 | Win debounce | KV **atomic CAS / `PutIfAbsent`** + TTL | needs atomicity | KV TTL 1m + read-time expiry | **G2, G3** |
| 10 | DigiSeg | KV `Put(WithTTL 5m)`/`Get` | eventual | KV TTL 5m | G3 |
| 11 | Fetcher state | KV/Collections hash for state + KV scalar counter (seen) + KV CAS/TTL (debounce); GORM fallback unchanged | eventual | seen 3d, debounce cfg | G1, G2, G3 |
| 12 | Page-match | Collections hashes (article→matches) + sets (pageMatch→articles) + `BulkRemove` for invalidation | eventual | namespace TTL 24h | **G4** |

---

## 4. Gap analysis — features waveSpan must add or harden

Each gap below names the concrete vires usage that drives it.

### G1. Scalar atomic counters with TTL  *(new KV primitive)*
**Driven by:** generic counter #6 (`INCR`/`INCRBY`/`EXPIRE`), fetcher seen-counter
#11 (`INCR`/`EXPIRE` 3d).
**Today:** waveSpan only offers atomic increment on a **hash field**
(`HIncrBy` on the CP tier). There is no standalone scalar `Incr`/`IncrBy`, and the
hot-path KV tier has no atomic increment at all.
**Proposal:** add a KV-tier counter operation:
```
rpc IncrBy(IncrByRequest) returns (IncrByResponse);
message IncrByRequest  { string namespace; bytes key; int64 delta; optional int64 ttl_ms; optional string idempotency_key; }
message IncrByResponse { int64 value; Version version; ResponseMeta meta; }
```
- **Hot-path variant:** eventually-consistent / approximate (sum-of-deltas
  reconciled like other KV writes) — acceptable for caps and counters #6.
- **Exact variant:** route to the CP tier (same engine as `HIncrBy`) for callers
  that opt in.
- `ttl_ms` set only when the counter is created (mirrors vires' "EXPIRE on
  count==1" pattern in counting.go).

### G2. Atomic test-and-set / compare-and-set  *(new KV primitive)*
**Driven by:** win debounce #9 (`GETSET` then `EXPIRE`), fetcher debounce #11.
**Today:** no CAS, no `SETNX`, no `GETSET`. **Idempotency keys do not substitute** —
two *different* request IDs racing on the same `winSeen:<id>` key must
deterministically resolve to exactly one winner; idempotency only collapses retries
of the *same* logical write.
**Proposal:** add a conditional write to the KV tier:
```
rpc PutIfAbsent(PutIfAbsentRequest) returns (PutIfAbsentResponse);
message PutIfAbsentResponse { bool stored; bytes existing_value; Version version; ResponseMeta meta; }
// optional generalisation: CompareAndSwap(expected_version | expected_value)
```
- `stored=false` + `existing_value` reproduces `GETSET` semantics for dedup.
- Must be **linearizable** to be correct as a debounce/lock — i.e. backed by the CP
  tier (note: CAS was listed in `design/03` but never implemented; this revives it
  on the consensus tier). Document that win-debounce correctness requires CP, not
  eventual KV.

### G3. TTL precision & visibility  *(harden existing)*
**Driven by:** debounce 1-min window #9, DigiSeg 5-min #10, prebid ~30–60m #8,
frequency 24h #1, set cache #7, seen 3d #11.
**Today:** TTL is **lazy/best-effort** (background sweeper) with optional
`hideExpiredOnRead` / `WithHideExpired()` to suppress unswept-but-expired records at
read time.
**Proposal:**
- Mandate `hideExpiredOnRead: true` on every namespace that replaces a TTL'd Redis
  key, so reads never observe logically-expired data even before the sweeper runs.
- Tighten sweeper cadence/SLA for short-TTL namespaces (debounce/DigiSeg) and
  document the guarantee ("expired data is never *returned*; physical reclamation is
  best-effort").
- For the 1-min debounce specifically, correctness rests on read-time expiry
  (G3) combined with the atomic CAS (G2), not on physical eviction timing.

### G4. Page-match cache — native redesign (no Lua)  *(modelling + optional primitive)*
**Driven by:** match cache #12, which today uses two Lua scripts (`add`,
`invalidate`) for multi-key atomicity across hashes (`+<hash>`/`-<hash>`) and sets
(`M<id>`/`N<id>`).
**Today:** waveSpan has **no scripting and no multi-key transactions** — and per the
user decision we do **not** add scripting.
**Proposal — map directly onto Collections:**
- Article → matches: a **hash** per article (`HSet` field=`pageMatchID`,
  value=`score`; `HGetAll` on read — replaces the current `HGETALL`).
- Inverse index: a **set** per page-match (`SAdd` article hashes; `SMembers` to find
  affected articles).
- **Invalidation** of a page-match (the script's hard part) becomes:
  `SMembers(M<id>)` → `BulkRemove(namespace, collections=[+<hash> …], members=[<id>])`
  → `SRem`/delete the `M<id>`/`N<id>` sets. `BulkRemove` already removes a member
  from many collections in one call (design 30 §13.7), which is exactly the
  fan-out the Lua `invalidate` performed.
- **Atomicity note (the one residual gap):** the `add` path (`SADD` + `HSET` across
  different keys) is not single-shard atomic on waveSpan. Two options, in order of
  preference:
  1. **Accept eventual consistency** here — a reader may briefly see a page-match in
     the article hash before the inverse set, or vice versa; given the 24h TTL and
     read-mostly access this is tolerable. *(Recommended.)*
  2. **Optional future primitive:** a scoped *multi-collection atomic batch* within
     one namespace/shard, if testing shows the eventual window causes incorrect
     serving. Flagged as optional, not required for migration.

### G5. Collections-tier hardening (M30)  *(productionise existing)*
**Driven by:** pacing/budget #2 (exact, money) and the high-write hot-path hashes
#3/#4.
**Today:** the Collections SDK contract is complete, but the server (M30) is
*in-progress* (see `design/30`, `33_consensus_throughput_results_and_hardening.md`).
**Proposal / acceptance criteria:**
- Validate `HIncrBy`/`HIncrByFloat` **throughput and p99 latency** under ad-serving
  write rates; confirm the budget path fits the request latency budget when using
  linearizable writes.
- Confirm **idempotency-key** behaviour under retry for counters (exactly-once
  increment) — critical so a retried revenue event does not double-count spend.
- Confirm bounded-stale reads are acceptable for #3/#4 and linearizable reads for
  #2's pre-serve budget checks.

### G6. `KEYS` → `Scan` convention  *(modelling + minor API)*
**Driven by:** counting.go #6 uses `KEYS <prefix>*` (production-unsafe).
**Proposal:** replace with KV-tier range `Scan(WithRange(prefix, prefixEnd))`.
Requires a documented **key-prefix → namespace** convention so a prefix scan maps to
a bounded key range; confirm scan completeness mode (cache-fast vs routed-eventual)
expectations for the caller. This removes the `KEYS` anti-pattern entirely.

### G7. Client & operability ergonomics  *(SDK / integration)*
**Driven by:** the `redisio` seam, its per-DB pools, and `metrics.go`.
**Proposal:**
- A vires-side adapter implementing the new backend interface (§6) over
  `wavespan.Client`, mapping DB numbers to namespaces.
- **Metrics parity:** expose connection/RPC metrics equivalent to the existing
  `vires_redis_*` pool gauges so dashboards/alerts survive the migration.
- Read-mode mapping (bounded-stale vs `linearizable`) configurable per cache, so the
  master/slave semantics are preserved where they matter.
- Config parity for endpoints/TLS/auth (`wavespan.Options`).

---

## 5. What is explicitly *not* needed

- **Pub/Sub, Streams** — not used anywhere in vires; waveSpan's lack of channel
  pub/sub is irrelevant here.
- **Sentinel/Cluster** — vires does not use them; waveSpan's built-in distribution
  replaces the master/slave topology.
- **Server-side Lua / general transactions** — deliberately out of scope; the only
  consumer (match cache) is redesigned natively in G4.
- **Geo / bitmap / HyperLogLog** — not used in vires.

---

## 6. Migration strategy

**Seam-first.** Extract a backend-agnostic interface at the `redisio.Client` seam
(the cache structs in `pkg/data/cache/*` and `pkg/components/cache/*` already
isolate Redis behind per-cache types, so the blast radius is small). Provide two
implementations — the existing `redigo` one and a new `wavespan` adapter (G7) —
selectable by config, enabling dual-write / shadow-read validation per use case.

Phased rollout, each phase gated on the features it needs:

- **Phase 0 — Seam & harness.** Define interfaces; build the `wavespan` adapter;
  add dual-write + shadow-read comparison + metrics parity (G7). No behaviour
  change.
- **Phase 1 — Pure KV + TTL.** Migrate creative data #5, prebid #8, DigiSeg #10.
  Needs: KV tier (exists) + G3. Lowest risk, validates TTL behaviour in production.
- **Phase 2 — Approximate hot-path counters.** Migrate frequency #1, creative
  stats #3, delivery stats #4, generic counter #6 (+ G6 for `KEYS`). Needs: G1
  and/or G5 (eventual/approx mode), G3.
- **Phase 3 — Exact + atomic.** Migrate pacing/budget #2 (CP `HIncrBy`, G5) and win
  debounce #9 (CAS, G2 + G3). Highest correctness bar; depends on M30 hardening.
- **Phase 4 — Collections-modelled & redesigns.** Migrate set cache #7, fetcher
  state #11, and the page-match cache #12 (G4) once the eventual-consistency
  window is validated under real serving.

Decommission Redis per use case as each phase's shadow comparison stays clean.

---

## 7. Risks & non-goals

- **Eventual-consistency overcount (hot path).** Approximate counters (#1/#3/#4) may
  slightly overcount under concurrency. Bounded and acceptable for caps/reporting;
  *not* used for budget (which stays exact via CP).
- **Dependency on M30 hardening.** Phases 3–4 cannot ship before the Collections
  tier meets the G5 throughput/latency/idempotency criteria.
- **TTL is best-effort physically.** Correctness for short-TTL keys relies on
  read-time expiry (`hideExpiredOnRead`, G3), not on prompt physical eviction.
- **Match-cache add is not atomic** across the hash + inverse set (G4); we accept a
  brief eventual window unless testing proves otherwise.
- **Win-debounce requires CP.** Treating it as an eventual KV write would break
  dedup; it must use the linearizable CAS (G2).
- **Cross-region behaviour.** vires runs against a single Redis topology today; if
  waveSpan spans regions, per-namespace replication/conflict policy must be set
  deliberately (counters and debounce should not be global active-active without
  review).

---

## 8. Required waveSpan work items (checklist)

| ID | Item | Type | Tier | Blocks vires use case |
|----|------|------|------|------------------------|
| G1 | Scalar `IncrBy` with optional TTL (approx KV + exact CP variants) | new | KV + CP | #6, #11 |
| G2 | `PutIfAbsent` / `CompareAndSwap` (linearizable) | new | CP | #9, #11 |
| G3 | TTL read-time visibility (`hideExpiredOnRead` mandate) + sweeper SLA | harden | KV/CP | #1, #7, #8, #9, #10, #11 |
| G4 | Page-match redesign on hashes+sets+`BulkRemove`; (optional) scoped multi-collection atomic batch | model + optional | CP | #12 |
| G5 | Collections-tier `HIncrBy` throughput/latency/idempotency hardening (M30) | harden | CP | #2, #3, #4 |
| G6 | `KEYS`→range-`Scan` convention + key-prefix/namespace mapping | model | KV | #6 |
| G7 | vires `wavespan` backend adapter, DB→namespace mapping, metrics parity, read-mode mapping | integration | — | all |

This checklist is intended to slot alongside `18_implementation_roadmap.md` and
`19_agent_work_items.md`.
