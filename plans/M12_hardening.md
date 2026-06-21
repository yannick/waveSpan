# M12 — Hardening Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Production-harden WaveSpan — mTLS + role-based auth on every API surface, a backup/restore prototype that rebuilds vector indexes, a Jepsen-style chaos suite that asserts convergence, load tests, observability dashboards + alerts, and documentation.

**Architecture:** mTLS wraps gateway↔data, data↔data, replication, admin, and cross-cluster traffic; an auth middleware enforces five roles (admin/reader/writer/replicator/operator) with internal/public separation so replication credentials cannot drive public writes. Backup leverages `wavesdb`'s object-store mode + `PromoteToPrimary`; restore opens the object-store replica into a fresh cluster and rebuilds derived vector indexes. The chaos / convergence gate is **not built here** — it is the model-aware correctness harness specified in `25_correctness_harness.md` and built by **M14** (`tests/harness/`). M12 **consumes** that harness for TS-102: it runs the harness's convergence workloads + nemeses nightly and gates the release on green. Dashboards and alerts cover the required signals across all subsystems.

**Tech Stack:** Go, `github.com/cwire/wavespan`, `wavesdb` object-store mode, gRPC + crypto/tls, Prometheus + Grafana, the `testing-waves` harness (`/Volumes/HOME/code/storage-engines/testing-waves`), docker-compose for chaos/load.

**Depends on:** M03/M04 (StoreReplica internal API), M07 (global replication + ClusterPeer, anti-entropy), M09/M10 (vector indexes to rebuild), M11 (operator manages Secrets), **M14 (the correctness harness consumed by TS-102)**. TS-100/101/102.

---

## Context

Roadmap M12, docs `15_security.md`, `14_observability.md`, `13_failure_model.md`, `16_testing_strategy.md`. Three tickets:

- **TS-100 mTLS and auth** — transport security + roles; unauthenticated internal calls rejected; peer replication requires a valid cert.
- **TS-101 observability dashboards** — required metrics exist; alerts are generated.
- **TS-102 chaos test suite** — nightly chaos passes convergence properties. **The suite is the M14 correctness harness (`25_correctness_harness.md`, `tests/harness/`), not a bespoke M12 suite.** M12 wires its nightly invocation and the release gate.

Plus the M12 deliverable list adds **backup/restore prototype** (acceptance: backup/restore validates data and rebuilds vector indexes) and **load tests**.

Key constraints:

- Roles (doc 15): `admin` (all), `reader` (KV get/scan, Cypher read), `writer` (KV put/delete, Cypher writes), `replicator` (internal replication only), `operator` (admin lifecycle). **Replication credentials must not be usable for public writes** — this separation is the point of the milestone, not a nicety.
- Docker insecure mode is allowed **only** behind explicit `security.insecureDevMode: true` (doc 15). Production requires mTLS.
- Logs/metrics must redact raw keys/values by default; use `key_hash = base64url(blake3(namespace + key))` (doc 15 "Data redaction"). Debug endpoints require `includeValue=true` + admin auth.
- Cross-cluster peers authenticate via mTLS + a peer allowlist + replay protection via mutation IDs (already enforced in M07) + per-peer rate limits + audit logs.
- Backup/restore leverages `wavesdb` object-store mode and `PromoteToPrimary`; vector indexes are derived and **rebuilt on restore** (doc 08, CRD `rebuildVectorIndexes: true`), not backed up by default (`WaveSpanBackup.includeVectorIndexes: false`).
- The chaos harness is **owned by M14** (`25_correctness_harness.md`, `tests/harness/`), itself seeded from `testing-waves` (the Jepsen-style bank test that found a real wavesdb skiplist bug). Its bank invariant — sum of balances constant — is the convergence oracle, asserted *after* faults heal (eventual, not linearizable). M12 does not reimplement it; M12 runs the harness's convergence configuration nightly and gates on it.

## File Structure

```
internal/security/tls.go                    # mTLS config loading (certs from Secrets / dev CA), gRPC creds
internal/security/auth.go                   # role model, token/cert -> role mapping
internal/security/middleware.go             # gRPC interceptors: public vs admin vs internal role enforcement
internal/security/redact.go                 # key_hash = base64url(blake3(ns+key)); value redaction
internal/security/audit.go                  # connection + apply-error audit log for peer replication
internal/security/ratelimit.go              # per-peer replication rate limiter
internal/backup/backup.go                   # object-store backup driver (uses wavesdb object-store mode)
internal/backup/restore.go                  # restore: open object-store replica, PromoteToPrimary, rebuild vector indexes
cmd/wavespanctl/backup.go                   # CLI: backup/restore subcommands
observability/dashboards/*.json             # Grafana dashboards (local, global, graph, vector, repair)
observability/alerts/wavespan_alerts.yaml   # Prometheus alert rules
tests/chaos/convergence_gate.go             # thin wrapper invoking the M14 harness (tests/harness/) convergence config
tests/chaos/convergence_test.go             # nightly gate: run M14 harness, assert no convergence/durability violations
tests/load/load_test.go                     # throughput/latency load test
docs/operations.md                          # runbook: deploy, backup/restore, scale, drain, peer setup
docs/security.md                            # mTLS + roles + redaction operator guide
```

## Tasks

### Task 1: mTLS transport (TS-100)

**Files:**
- Create: `internal/security/tls.go`
- Test: `internal/security/tls_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestMTLSHandshakeSucceedsWithValidCerts` — server + client built from a test CA complete an mTLS handshake on a gRPC call.
  - `TestMTLSRejectsUntrustedClient` — a client with a cert from a different CA is rejected.
  - `TestInsecureDevModeGated` — `insecureDevMode:true` permits a plaintext connection; default config does not.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `tls.go`: load CA/cert/key (from mounted Secret paths in K8s, dev CA in Docker), build `credentials.TransportCredentials` requiring + verifying client certs, gate plaintext behind `insecureDevMode`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 2: Roles + auth middleware with internal/public separation (TS-100)

**Files:**
- Create: `internal/security/auth.go`, `internal/security/middleware.go`
- Test: `internal/security/middleware_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestRoleCapabilities` — reader can `KV.Get`/Cypher-read, cannot `KV.Put`; writer can put; replicator can call `StoreReplica` but **cannot** call public `KV.Put` (the credential-separation rule); operator can call admin lifecycle; admin can do all.
  - `TestUnauthenticatedInternalCallRejected` — a call to the internal `StoreReplica`/`PushGlobal` API without a `replicator`/valid cert identity is rejected (TS-100 acceptance).
  - `TestPeerReplicationRequiresValidCert` — `PushGlobal` from an identity not on the peer allowlist is rejected.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the role model (map cert SAN/SPIFFE id or bearer token -> role), and gRPC `UnaryInterceptor`/`StreamInterceptor` per surface (public KV/Cypher, admin, internal replication) enforcing the capability matrix. Internal services accept only `replicator`/`admin`; public services reject `replicator`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Redaction, audit, per-peer rate limit (TS-100, doc 15)

**Files:**
- Create: `internal/security/redact.go`, `internal/security/audit.go`, `internal/security/ratelimit.go`
- Test: `internal/security/redact_test.go`, `internal/security/audit_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestKeyHashRedaction` — `KeyHash(ns, key) == base64url(blake3(ns+key))`; logs/metric labels use the hash, never raw key/value; `includeValue` requires admin.
  - `TestPeerAuditLog` — a peer connection and an apply error each emit an audit record.
  - `TestPerPeerRateLimit` — replication apply from one peer is throttled at the configured rate.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement blake3-based `KeyHash`, the redacting log/metric helpers, the peer audit log, and a token-bucket per-peer rate limiter wired into the M07 receiver.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: Backup driver (object-store mode)

**Files:**
- Create: `internal/backup/backup.go`, `cmd/wavespanctl/backup.go`
- Test: `internal/backup/backup_test.go`

- [ ] **Step 1:** Write failing test `TestBackupToObjectStore` — write data, run a backup to a local object-store target, assert a consistent snapshot (MANIFEST + segments) lands at the destination prefix; `includeVectorIndexes:false` excludes ANN segment data.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `backup.go` using `wavesdb` object-store mirroring (the same mechanism `testing-waves -objstore` exercises): drive a checkpoint/mirror to the configured destination (matches `WaveSpanBackup` CRD: s3 bucket/prefix). Add the `wavespanctl backup` subcommand.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: Restore + vector index rebuild

**Files:**
- Create: `internal/backup/restore.go`
- Modify: `cmd/wavespanctl/backup.go` (restore subcommand)
- Test: `internal/backup/restore_test.go`

- [ ] **Step 1:** Write failing tests:
  - `TestRestoreValidatesData` — restore from the Task 4 backup into a fresh store; assert every KV/graph record matches the source (data validation).
  - `TestRestoreRebuildsVectorIndexes` — the backup excluded ANN segments; after restore with `rebuildVectorIndexes:true`, `vector.searchApprox` returns correct results (indexes rebuilt from raw records via M10 `RebuildPartition`).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `restore.go`: open the object-store replica read-only, `PromoteToPrimary` into the fresh cluster's storage, then invoke the M10 per-partition vector rebuild for each `VectorIndex`. Add the `wavespanctl restore` subcommand (maps to `WaveSpanRestore` CRD).
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: Nightly convergence gate via the M14 harness (TS-102)

> The chaos suite itself is built in **M14** (`25_correctness_harness.md`, `tests/harness/`).
> This task makes `tests/chaos/` a thin build-tagged entry point that invokes the harness's
> convergence configuration and wires it as the nightly release gate. Do **not** reimplement
> bank/nemeses here.

**Files:**
- Create: `tests/chaos/convergence_gate.go` (thin wrapper invoking `tests/harness/runner` with the convergence config)
- Test: `tests/chaos/convergence_test.go`
- Reference: `25_correctness_harness.md`, `plans/M14_correctness_harness.md`, the harness under `tests/harness/`

- [ ] **Step 1:** Write failing test `TestConvergenceGate` (`//go:build chaos`): invoke the M14 harness with the bank/register/set workloads and the `{node-kill, partition-halves, cluster-partition, latency, kill-origin-after-ack}` nemeses across the multi-node + two-cluster docker-compose clusters; **heal**; assert the harness's `convergence` + `durability` checkers report no violations (eventual, post-heal — not during the partition).
- [ ] **Step 2:** Run, expect FAIL (harness not yet wired / M14 incomplete).
- [ ] **Step 3:** Implement `convergence_gate.go` calling `tests/harness/runner.Run` with the nightly-soak convergence config (doc 24 Docker/Linux CI path). No bank/nemesis code is duplicated — it lives in `tests/harness/`. Fail the gate on any harness `Violation`; on violation, surface the harness forensic dump + the shrunk repro path.
- [ ] **Step 4:** Run `go test -tags chaos ./tests/chaos -run Convergence`. Expect PASS (`Test OK` style invariant).
- [ ] **Step 5:** Commit.

### Task 7: Load tests (M12 deliverable)

**Files:**
- Create: `tests/load/load_test.go`
- Test: same file (`//go:build load`)

- [ ] **Step 1:** Write `load_test.go`: drive sustained KV put/get and a vector/graph query mix at configurable concurrency against the compose cluster; record throughput and p50/p95/p99 latency; assert no errors and latencies under a configured ceiling.
- [ ] **Step 2:** Run `go test -tags load ./tests/load`. Expect PASS + a latency/throughput report.
- [ ] **Step 3:** Commit.

### Task 8: Dashboards + alerts (TS-101)

**Files:**
- Create: `observability/dashboards/*.json`, `observability/alerts/wavespan_alerts.yaml`
- Test: `observability/alerts/alerts_test.go` (promtool-style rule validation)

- [ ] **Step 1:** Write a failing test `TestAlertRulesValid` asserting the alert rules file parses and references real metric names (the M07 `global_repl_*`, M08 query metrics, M10 `vector_*`, repair/under-replication, membership).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Author Grafana dashboards (local replication + repair, global replication lag, graph query, vector search, membership/latency) covering the required signals from doc 14/the per-subsystem metric lists. Author Prometheus alert rules (high global replication lag, sustained under-replication, repair unhealthy, apply errors, vector delta-index lag). Validate with `promtool check rules`.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 9: Documentation (M12 deliverable)

**Files:**
- Create: `docs/operations.md`, `docs/security.md`

- [ ] **Step 1:** Write `docs/operations.md` — deploy via the operator, configure ReplicationPolicy/peers, backup/restore runbook, scale-up/down, drain/rolling upgrade, reading dashboards/alerts.
- [ ] **Step 2:** Write `docs/security.md` — mTLS cert provisioning (Secrets/cert-manager), the role matrix and credential separation, redaction behavior, peer allowlisting, dev insecure-mode caveats.
- [ ] **Step 3:** Commit.

## Acceptance Criteria

From roadmap M12 + TS-100/101/102:

- **Nightly chaos passes convergence properties** — the M14 correctness harness, invoked via the `tests/chaos/` gate under killed pods, pod partitions, and cross-cluster partitions, reports no `convergence`/`durability` violations after faults heal (`TestConvergenceGate`) (TS-102).
- **Metrics dashboards cover required signals** — dashboards cover local/global replication, repair, graph, vector, and membership; alert rules fire on the documented conditions and validate with `promtool` (`TestAlertRulesValid`) (TS-101).
- **Backup/restore validates data and rebuilds vector indexes** — restore reproduces all KV/graph records and rebuilds working vector indexes from raw records (`TestRestoreValidatesData`, `TestRestoreRebuildsVectorIndexes`).
- **Unauthenticated internal call rejected; peer replication requires a valid cert** — internal `StoreReplica`/`PushGlobal` reject unauthenticated/unauthorized callers; off-allowlist peers are rejected (`TestUnauthenticatedInternalCallRejected`, `TestPeerReplicationRequiresValidCert`) (TS-100).
- Replication credentials cannot drive public writes; logs/metrics redact raw keys/values; load tests produce a throughput/latency report.

## Verification

1. **Unit:** `go test ./internal/security/... ./internal/backup/...` — mTLS handshake + rejection, role matrix + internal/public separation, redaction, audit, rate limit, backup snapshot, restore data validation + vector rebuild.
2. **Chaos (nightly):** `docker compose -f docker/docker-compose.global.yaml up -d` then `go test -tags chaos -timeout 30m ./tests/chaos -run Convergence`. This invokes the M14 harness (`25_correctness_harness.md`); confirm its `convergence` + `durability` checkers report no violations post-heal and `Test OK`-style output. The harness — not this task — owns the bank/nemesis implementation (seeded from `testing-waves`).
3. **Load:** `go test -tags load ./tests/load` — inspect the throughput/latency report; confirm no errors and latencies under the ceiling.
4. **Backup/restore drill:** run `wavespanctl backup` against a populated cluster to a local object store; `wavespanctl restore --rebuildVectorIndexes` into a fresh cluster; diff KV/graph records against the source and run `vector.searchApprox` to confirm rebuilt indexes return correct results.
5. **Security drill:** attempt an internal `StoreReplica` call with no cert (rejected); attempt a public `KV.Put` with a `replicator` identity (rejected); confirm logs show `key_hash` not raw keys; validate alerts with `promtool check rules observability/alerts/wavespan_alerts.yaml`.
