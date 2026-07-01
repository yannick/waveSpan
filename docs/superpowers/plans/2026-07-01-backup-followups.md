# Backup follow-ups — implementation plan (4 features)

> **For agentic workers:** superpowers:subagent-driven-development. Steps `- [ ]`. Each feature is independently shippable; TDD + `make lint` + `make test-race` per feature.

Follow-ups tracked after the backup feature shipped (main + OVH stag). Order: F1 (real PARTIAL) → F4 (sibling reconstruction) → F2 (BackupSummary UI) → F3 (alternate-destination deploy config).

---

## F1 — Real `PARTIAL` via held-range coverage
**Goal:** A backup is `PARTIAL` when the live cluster did not cover the full expected keyspace (a node/shard down at backup time), not just when the assigner supplies a gap list. Today `commit()` decides on `len(in.Gaps)>0` and `AllExportAssigner` yields none → always `COMPLETE`.

**Approach:** `PrepareLocal` returns each node's real held ranges; `commit()` unions them and cross-checks vs expected coverage → gaps → `PARTIAL` (gaps recorded in `cluster.manifest`).

**Grounding to confirm (bk3a — explore first):**
- How to derive a node's authoritative ranges: collections shard IDs it hosts (the shard state machines it runs / placement), and KV/global ownership (holder directory / replicate-everywhere namespaces). Check `internal/collections` (placement/ownership), the meta-shard range directory (`NewHashDirectory`/`ShardForKey`), and membership.
- Expected coverage source: collections data shards `FirstDataShard..N` from the range directory; KV hash-space + per-namespace replica expectation from membership/config.

**Tasks:**
- [ ] Explore + note the exact held-range + expected-coverage APIs.
- [ ] `Agent.PrepareLocal` returns real `HeldRanges` (shard ids + KV ranges/namespaces this node owns), replacing the `nil` today. Test with a real placement/directory.
- [ ] `Coordinator.commit`: union live nodes' `HeldRanges`; compute uncovered ranges vs expected; set `PARTIAL` + record gaps in the manifest. Test: all nodes present → COMPLETE; a shard/range with no covering live node → PARTIAL with that gap.
- [ ] Keep the assigner-gap path too (union both). `make lint` + `make test-race`. Commit.

## F4 — Sibling reconstruction in a `≤T` cut
**Goal:** When `RepairCutMeta` repoints a dangling (after-T) winner, preserve the key's `≤T` concurrent siblings instead of dropping them. The concurrency relationship is already recorded in the verbatim-exported `CFKVMeta` `SiblingVersions` — filter it to `≤T`, don't discard.

**Approach:** In `repair.go`'s "winner absent" branch: gather `{winner} ∪ SiblingVersions`, filter to versions present in the restored (`≤T`) CFKVData, pick the max as the new winner, retain the remaining `≤T` ones as siblings (tombstone/TTL from the winner). A `>T` sibling is correctly excluded (genuinely after the cut).

**Files:** `internal/recordstore/repair.go` + test.
**Tasks:**
- [ ] Failing test: a key whose current winner is after T AND which has a `≤T` concurrent sibling → after restore the repointed pointer KEEPS the `≤T` sibling (`ConflictNone` stays false / sibling present); a `>T` sibling is dropped; winner = max `≤T`.
- [ ] Implement the `≤T`-sibling-preserving repoint. Update the doc note (repoint preserves `≤T` siblings; only genuinely-after-T conflict versions are lost). `make lint` + `make test-race`. Commit.

## F2 — `BackupSummary` enrichment (richer UI list)
**Goal:** The Backups list shows planes, size, destination, retain-until, full-vs-incremental, and PARTIAL gaps — not just id/status/times.

**Approach:** Add fields to the `BackupSummary` proto; populate in `ListBackups` from the `BackupIntent` + cluster manifest; render columns in the SPA.

**Files:** `proto/wavespan/v1/backup.proto` (+ regen: Go-only buf path AND `cd ui && npm ci && npm run gen` for TS), `internal/backup/coordinator.go` (ListBackups populate), `ui/src/views/Backups.tsx` + `backupModel.ts`.
**Tasks:**
- [ ] proto: add `planes`, `size_bytes`, `destination` (bucket/prefix — NO creds), `retain_until_ms`, `parent`, `partial`/`gaps_count` to `BackupSummary`. Regen Go + TS.
- [ ] `ListBackups`: populate from the intent (destination descriptor, retainUntil, planes, parent) + aggregate total bytes from per-node records. Test.
- [ ] UI: add columns in `Backups.tsx`; PARTIAL rows show gap count; pure formatters in `backupModel.ts` with vitest. `npm run typecheck && npm test`; `make ui`; `make lint`. Commit.

## F3 — Alternate-destination deploy config (named destination)
**Goal:** Enable backups to a second bucket (`wavespan2-de-stag`) via a **named** destination (secure — keeps named-only mode; no inline creds).

**Approach:** Mostly config (code is complete from 3e). Confirm the named-destination config path resolves creds from an env/secret-ref (small code fix + test IF there's a gap), then wire gitops + a 2nd Secret + validate.
**Tasks:**
- [ ] (bk3a) Verify/close the named-destination code path: `cfg.Backup.NamedDestinations` parses (from config/env) with a per-destination cred env/secret-ref; `ResolveDestination({name:"alt"})` resolves creds from that ref (not inline); descriptor persists the ref only. Add a test if missing.
- [ ] (controller) gitops: add the named destination (bucket2 + cred env-refs) to the stag config/env + a `wavespan-backup-s3-alt` Secret (kubectl, not gitops). Validate: `BeginBackup{destination:{name:"alt"}}` lands objects in `wavespan2-de-stag`, persists + GC-reconciles in that bucket, no creds in manifest.

## Done criteria
- [ ] F1: PARTIAL reflects real coverage gaps; F4: `≤T` siblings preserved on repoint; F2: enriched list renders; F3: a named-destination backup to bucket2 verified on stag. `make lint` + `make test-race` + UI gates green each.
