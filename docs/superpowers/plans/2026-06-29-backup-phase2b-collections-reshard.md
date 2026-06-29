# Backup Phase 2b — collections re-shard on restore — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore a logical backup into a cluster with a **different collections shard count** (`N`). Collections (`CFReplData`) are the only hash-routed datatype; on a same-N restore they're copied verbatim (Phase 2a), but restoring into a different `N` requires re-routing every collection key to its new shard. This phase adds that re-route transform so clones/restores aren't pinned to the source's shard count.

**Why only collections:** KV, graph, and vector live in their own CFs served by the global (origin+1) tier — they are never hash-routed, so they restore verbatim into any cluster shape (Phase 2a already handles them correctly, including their persisted indexes; the vector ANN `LiveIndex` is in-memory and reconstructed from `CFVectorRaw` at node startup, never in the backup). Re-sharding is therefore a `CFReplData`-only transform.

**Architecture:** Collections data lives in `CFReplData` under an 8-byte big-endian shard prefix (`shardID = FirstDataShard + fnv64a(routeKey(ns,coll)) % N`). The key suffix after the prefix begins with a sub-prefix byte identifying the row type; rows that belong to a collection embed `chunk(ns)||chunk(coll)` and are re-routable, while the per-shard Raft `applied`-index row (`subMeta`) is shard-local and must be dropped on re-shard (the target shard starts fresh). A new collections-package function `RerouteSuffix` decides, per suffix, the target shard under a new `N` (or drop). `RestoreLogical` gains a target-`N` input: when set, it rewrites each `CFReplData` row's shard prefix via `RerouteSuffix`; when unset (0), it restores verbatim as today.

**Tech Stack:** Go (`github.com/yannick/wavespan`), `internal/collections` (codec + routing), `internal/backup`. Spec: `docs/superpowers/specs/2026-06-27-snapshot-backups-design.md` (§7 re-shard, "Phase 2b grounding findings").

**Phase roadmap:** Phase 1 ✅, Phase 2a ✅ (logical core, same-shape verbatim). **Phase 2b (this doc)** = collections re-shard on restore. Phase 2c = partial selection (namespace/collection) + optional backup-size optimization (exclude the rebuildable graph index). Phase 3 = cluster coordinator + incrementals + physical plane. Phase 4 = operator + CLI.

## Where to work
Worktree **/Volumes/HOME/code/storage-engines/waveSpan-backup** (branch `backup`). Commit per task. Tests: `go test ./internal/collections/... ./internal/backup/...`.

## Confirmed API reference
- Shard prefix = `be8(shardID)`. The COMPLETE shard-level sub-prefix space is `0x00..0x06` (first byte of the suffix after the prefix):
  - `subMeta=0x00` (base_sm.go:14) — per-shard `applied` raft index (+ `subMeta|"dedupseq"`, dedup.go:25); **drop** on re-shard.
  - `subData=0x01` (statemachine.go:34) — collection data (incl. budget pool/lease/tomb, which build on `collScope=dataSpace+chunk(ns)+chunk(coll)`); re-route, body=`suffix[1:]`.
  - `subTTL=0x02` (ttl.go:16) — re-route, body=`suffix[9:]` (skip 8-byte expiry).
  - `subDedup=0x03` (dedup.go:19) — idempotency dedup, keyed `prefix|subDedup|<client-idempotency-key>` (NO ns/coll); **drop** (target rebuilds dedup window fresh).
  - `subDedupRing=0x04` (dedup.go:20) — dedup FIFO ring, keyed `prefix|subDedupRing|be(slot)` (NO ns/coll); **drop**.
  - `subBudExp=0x05` (budget.go:28) — re-route, body=`suffix[9:]` (skip 8-byte reclaimMs).
  - `subBudTombGC=0x06` (budget.go:29) — re-route, body=`suffix[9:]` (skip 8-byte gcDueMs).
- Routing-key extraction model: `routeKeyOf(suffix []byte) ([]byte, bool)` (migrate.go:39) already handles `subData` (`body=suffix[1:]`) and `subTTL` (`body=suffix[9:]`, skip the 8-byte expiry), then `takeChunk`→ns, `takeChunk`→coll. The budget shard-level index keys are `…|subBudExp|be8(reclaimMs)|chunk(ns)|chunk(coll)|leaseID` and `…|subBudTombGC|be8(gcDueMs)|chunk(ns)|chunk(coll)|leaseID` (budget.go:25-26) — i.e. `body=suffix[9:]` like `subTTL`. (Budget pool/lease/tomb DATA rows live under `subData`, so they're already covered by the `subData` case.)
- `ShardForKey(ns, coll []byte, dataShards uint64) uint64` (directory.go:62) = `FirstDataShard + fnv64a(routeKey(ns,coll)) % dataShards`. `RouteKey(ns,coll)` (meta.go:150). `FirstDataShard` (directory.go:56). Codec helpers `takeChunk`/`appendChunk` (command.go:312/319), `prefixEnd` (command.go:358) — all package `collections`.
- Backup (Phase 2a): `RestoreLogical(dst storage.LocalStore, store ObjectStore, keyPrefix string, reg *Registry, ri RestoreInfo) error`; `RestoreInfo` struct (add a field here); CFReplData rows restored via batched `storage.StoreOp{CF, Key, Value}`.

---

## Task 1: `collections.RerouteSuffix`

A pure function deciding, for a `CFReplData` key suffix (the bytes after the 8-byte shard prefix) and a target shard count, the new shard id — or that the row should be dropped, or that it cannot be re-routed.

@superpowers:test-driven-development

**Files:**
- Create: `internal/collections/reshard.go`
- Test: `internal/collections/reshard_test.go`

- [ ] **Step 1: failing test**

```go
package collections

import (
	"encoding/binary"
	"testing"
)

func be8(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

func dataSuffix(ns, coll string) []byte {
	// subData || chunk(ns) || chunk(coll) || <rest>
	s := []byte{subData}
	s = appendChunk(s, []byte(ns))
	s = appendChunk(s, []byte(coll))
	return append(s, []byte("rest")...)
}

func TestRerouteSuffix(t *testing.T) {
	const newN = 8
	want := ShardForKey([]byte("ns1"), []byte("c1"), newN)

	// subData row re-routes by (ns,coll).
	id, keep, err := RerouteSuffix(dataSuffix("ns1", "c1"), newN)
	if err != nil || !keep || id != want {
		t.Fatalf("subData reroute: id=%d keep=%v err=%v want id=%d", id, keep, err, want)
	}

	// subBudExp index row (subBudExp || be8(reclaim) || chunk(ns) || chunk(coll) || leaseID) re-routes the same.
	exp := append([]byte{subBudExp}, be8(123)...)
	exp = appendChunk(exp, []byte("ns1"))
	exp = appendChunk(exp, []byte("c1"))
	exp = append(exp, []byte("lease")...)
	id2, keep2, err2 := RerouteSuffix(exp, newN)
	if err2 != nil || !keep2 || id2 != want {
		t.Fatalf("subBudExp reroute: id=%d keep=%v err=%v want id=%d", id2, keep2, err2, want)
	}

	// Shard-local bookkeeping is dropped: applied index + dedup window/ring (no (ns,coll)).
	for _, sp := range []byte{subMeta, subDedup, subDedupRing} {
		if _, keep, err := RerouteSuffix([]byte{sp, 'a'}, newN); err != nil || keep {
			t.Fatalf("sub-prefix %#x should drop: keep=%v err=%v", sp, keep, err)
		}
	}

	// A genuinely unused sub-prefix is a loud error (never silently dropped or misplaced).
	if _, _, err4 := RerouteSuffix([]byte{0x7f, 'x'}, newN); err4 == nil {
		t.Fatal("unknown sub-prefix must return an error")
	}
}
```

- [ ] **Step 2: run → FAIL** (`RerouteSuffix` undefined).

- [ ] **Step 3: implement** `internal/collections/reshard.go`:

```go
package collections

import "fmt"

// RerouteSuffix decides the target shard for a CFReplData key suffix (the bytes after the
// 8-byte shard prefix) under a new shard count newN. Collection rows that embed (ns,coll) are
// re-routed; the per-shard applied-index row (subMeta) is dropped (keep=false, nil err); an
// unrecognized sub-prefix returns an error so re-shard fails loudly rather than dropping or
// misplacing data.
func RerouteSuffix(suffix []byte, newN uint64) (shardID uint64, keep bool, err error) {
	if len(suffix) == 0 {
		return 0, false, fmt.Errorf("collections: empty CFReplData suffix")
	}
	var body []byte
	switch suffix[0] {
	case subMeta, subDedup, subDedupRing:
		// Shard-local bookkeeping with no (ns,coll): the applied raft index and the
		// idempotency dedup window/ring. Not re-routable; the target shards rebuild them
		// fresh (mirrors how the dropped applied index is re-established).
		return 0, false, nil
	case subData:
		body = suffix[1:]
	case subTTL, subBudExp, subBudTombGC:
		if len(suffix) < 9 {
			return 0, false, fmt.Errorf("collections: short %#x suffix", suffix[0])
		}
		body = suffix[9:] // skip sub-prefix byte + 8-byte timestamp
	default:
		return 0, false, fmt.Errorf("collections: cannot re-route unknown CFReplData sub-prefix %#x", suffix[0])
	}
	ns, rest, err := takeChunk(body)
	if err != nil {
		return 0, false, fmt.Errorf("collections: reroute decode ns: %w", err)
	}
	coll, _, err := takeChunk(rest)
	if err != nil {
		return 0, false, fmt.Errorf("collections: reroute decode coll: %w", err)
	}
	return ShardForKey(ns, coll, newN), true, nil
}
```

- [ ] **Step 4: run → PASS.**
- [ ] **Step 5: commit** — `git commit -am "feat(collections): RerouteSuffix — map a CFReplData key to its shard under a new N (re-shard primitive)"`

---

## Task 2: `RestoreLogical` honors a target shard count

@superpowers:test-driven-development

**Files:**
- Modify: `internal/backup/contributor.go` (add `CollectionsDataShards uint64` to `RestoreInfo`)
- Modify: `internal/backup/logical_restore.go`
- Test: extend `internal/backup/logical_restore_test.go`

- [ ] **Step 1: failing test** — seed `CFReplData` rows in `src` at an explicit N=4 layout (compute the prefix yourself: `key = be8(collections.ShardForKey(ns,coll,4)) || subData-suffix`), plus a `subMeta` row under some shard. Export. Restore into a fresh `dst` with `RestoreInfo{CollectionsDataShards: 8}`. Assert: (a) each collection row now lives under `be8(collections.ShardForKey(ns,coll,8)) || <same suffix>`; (b) the `subMeta` row is absent in `dst`; (c) a `RestoreInfo{}` (zero N) restore is byte-for-byte verbatim (regression-guard the Phase 2a path). Use `collections.ShardForKey`/`RerouteSuffix` (now exported) to compute expectations.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** — add `CollectionsDataShards uint64` to `RestoreInfo` (0 = verbatim, same as today). The actual per-key loop is in `restoreCFObject` (logical_restore.go:84-133), which is CF-generic — thread the target-N (and the CF identity) into it, so that when `cf == storage.CFReplData && ri.CollectionsDataShards > 0`, it transforms each key before adding the `StoreOp`:
  - split `key` into `prefix8 := key[:8]` and `suffix := key[8:]` (guard `len(key) >= 8`);
  - `newShard, keep, err := collections.RerouteSuffix(suffix, ri.CollectionsDataShards)`; on `err` → return it (abort restore — loud, never silent); on `!keep` → skip the row (do not emit a `StoreOp`); else emit `StoreOp{CF: CFReplData, Key: append(be8(newShard), suffix...), Value: value}`.
  - For all other CFs, and for `CFReplData` when `CollectionsDataShards == 0`, keep the existing verbatim path. Add a small `be8(uint64) []byte` helper in `internal/backup`.
  - **Entry-count integrity (preserve the Phase 2a check):** keep incrementing `decoded` at logical_restore.go:117 (right after a successful decode) — BEFORE the re-route drop/skip — so the existing `decoded != wantEntries` check still compares decoded-count to the manifest, exactly as the identity-key skip is already counted. The drop/skip affects only which `StoreOp`s are emitted, not the decoded count.

- [ ] **Step 4: run → PASS** (re-shard places rows correctly; subMeta dropped; zero-N still verbatim).
- [ ] **Step 5: commit** — `git commit -am "feat(backup): RestoreLogical re-shards CFReplData to a target N (RestoreInfo.CollectionsDataShards)"`

---

## Task 3: End-to-end re-shard round-trip across multiple collections

Prove a realistic re-shard: several collections whose shard assignment actually changes between N=4 and N=8, including a budget secondary-index row, survive and land correctly.

@superpowers:test-driven-development

**Files:**
- Test: `internal/backup/reshard_roundtrip_test.go`

- [ ] **Step 1: failing test** — pick ~6 `(ns,coll)` pairs; for each, at source N=4 write a `subData` row and a `subBudExp` index row under `be8(ShardForKey(ns,coll,4))`. Also write one `subMeta` (`prefix|subMeta|"applied"`) and one `subDedup` (`prefix|subDedup|<idem-key>`) row under some shard — to exercise the drop path on data a real cluster would contain. Export to an FS object store. Restore into `dst` with `CollectionsDataShards: 8`. Assertions: (a) for every `(ns,coll)` both data + budget-index rows are present in `dst` under `be8(ShardForKey(ns,coll,8))` with original suffixes+values, and absent under the old N=4 prefix when the shard differs; (b) at least one pair actually changed shard between N=4 and N=8 (real movement, not a no-op); (c) the `subMeta` and `subDedup` rows are ABSENT in `dst` (dropped); (d) no `(ns,coll)` data row is lost.

- [ ] **Step 2: run → FAIL** (until Tasks 1–2 correct).
- [ ] **Step 3: make it pass.**
- [ ] **Step 4:** `go test ./internal/collections/... ./internal/backup/...` green; `go vet ./...` clean; `go build ./...` green.
- [ ] **Step 5: commit** — `git commit -am "test(backup): collections re-shard round-trip N=4 -> N=8 (data + budget index re-routed, subMeta dropped)"`

---

## Done criteria for Phase 2b
- [ ] `collections.RerouteSuffix` maps every `(ns,coll)`-bearing `CFReplData` row (`subData`/`subTTL`/`subBudExp`/`subBudTombGC`) to its shard under a new `N`, drops shard-local bookkeeping (`subMeta`/`subDedup`/`subDedupRing`), and errors loudly on a genuinely unknown sub-prefix.
- [ ] `RestoreLogical` re-shards `CFReplData` when `RestoreInfo.CollectionsDataShards > 0`; verbatim (Phase 2a behavior) when 0; entry-count integrity check preserved.
- [ ] Re-shard round-trip test passes with real shard movement; no rows lost; budget index rows re-routed; `subMeta` dropped.
- [ ] All collections + backup tests green; vet + build clean. No hot-path change.

Phase 2b lets a backup restore/clone into a different collections shape. Phase 2c adds partial (namespace/collection) selection and the optional graph-index size optimization. Note for Phase 2c/3: re-routed budget leases keep their absolute-time fields (see spec §7.1) — the destination's expiry sweep reconciles them on resume; this is unchanged by re-shard.
