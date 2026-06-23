# M11 — Kubernetes Operator Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Kubebuilder operator that deploys and reconciles a WaveSpan cluster on Kubernetes — CRDs, StatefulSet, headless + gateway Services, PVCs, topology spread, PDB, status conditions, and a drain protocol for rolling upgrades.

**Architecture:** A separate Go module (`operator/`, its own `go.mod`) built with Kubebuilder. CRDs are defined as Go API types under `operator/api/`; the `WaveSpanCluster` controller reconciles the data StatefulSet, ConfigMaps/Secrets, headless Service (peer discovery), gateway Service/Deployment, PVCs (via volumeClaimTemplates), topology spread constraints, a PodDisruptionBudget, and writes status conditions. A drain protocol annotates pods before deletion so they leave gossip cleanly during rolling upgrades. CRD validation (webhook + CEL) rejects invalid policies per doc 12.

**Tech Stack:** Go, Kubebuilder/controller-runtime, `sigs.k8s.io/controller-tools`, `kind`/`k3d` for the test cluster, `envtest` for controller unit tests.

**Depends on:** M00 (node image, runtime config, env-var Kubernetes mode), M03/M04 (the data node honors the reconciled config). Conceptually consumes the M07 ClusterPeer config and M12 security Secrets, but the operator only needs the generated config/API types — never data-node internals (doc 17 forbidden dependency). TS-090/091/092/093.

---

## Context

Roadmap M11, doc `09_kubernetes_operator.md`, CRDs in doc `12_crds.md`. Four tickets:

- **TS-090 CRD definitions** — `WaveSpanCluster`, `ReplicationPolicy`, `KVNamespace`, `Graph`, `VectorIndex`, `ClusterPeer`, `WaveSpanBackup`, `WaveSpanRestore` (doc 12), installable into a test cluster.
- **TS-091 StatefulSet reconcile** — the `WaveSpanCluster` controller creates data pods and PVCs.
- **TS-092 Services + topology spread** — headless service (peer discovery), gateway service, topology spread constraints, PDB.
- **TS-093 drain + rolling upgrade** — drain annotation protocol and rolling update behavior keeping the cluster available under eventual semantics.

Key constraints:

- Topology spread uses `ScheduleAnyway` (not `DoNotSchedule`) for spot-heavy clusters (doc 09) — runtime repair, not scheduler perfection, is authoritative. The data layer's own distinct-node durable-replica rule is separate from K8s spread.
- **PVCs are retained on scale-down in v1** — never auto-delete PVCs (doc 09 "Scale down").
- Drain protocol (doc 09 "Rolling upgrades"): operator sets `wavespan.io/drain-requested=true`; the pod marks itself `DRAINING` in gossip, flushes its mutation log, rejects new coordinator writes when alternatives exist; operator waits for the readiness gate or timeout, then terminates; replacement rejoins.
- **CRD validation rules (doc 12 "Operator validation rules")** must reject: `minAckNearbyReplicas < 1` for local-cache mode; **`require-local-geo` with no compliance boundary label/value**; vector dimensions missing/zero; Cypher graph referencing a missing cluster; global peers enabled without TLS config; `targetNearbyReplicaCount < minAckNearbyReplicas`; `requireDistinctNodes=true` with replicas exceeding known schedulable nodes when strict scheduling requested.

## File Structure

```
operator/go.mod                                          # separate module (github.com/yannick/wavespan/operator)
operator/PROJECT                                         # Kubebuilder project marker
operator/api/v1alpha1/wavespancluster_types.go          # WaveSpanCluster spec/status
operator/api/v1alpha1/replicationpolicy_types.go        # ReplicationPolicy (local/cache/ttl/conflict/global, compliance boundary)
operator/api/v1alpha1/kvnamespace_types.go
operator/api/v1alpha1/graph_types.go
operator/api/v1alpha1/vectorindex_types.go
operator/api/v1alpha1/clusterpeer_types.go
operator/api/v1alpha1/backup_types.go                   # WaveSpanBackup + WaveSpanRestore
operator/api/v1alpha1/groupversion_info.go
operator/api/v1alpha1/zz_generated.deepcopy.go          # controller-gen output
operator/internal/controller/wavespancluster_controller.go
operator/internal/controller/resources_statefulset.go  # build data StatefulSet (+ topology spread, volumeClaimTemplates)
operator/internal/controller/resources_services.go     # headless + gateway Services
operator/internal/controller/resources_pdb.go          # PodDisruptionBudget
operator/internal/controller/resources_config.go       # ConfigMap/Secret rendering from spec
operator/internal/controller/drain.go                  # drain annotation protocol + readiness gate wait
operator/internal/controller/status.go                 # status conditions/phases
operator/internal/webhook/replicationpolicy_webhook.go # validating webhook (doc 12 rules) + CEL markers
operator/config/crd/...                                 # generated CRD manifests
operator/config/rbac/...                                # generated RBAC
operator/config/samples/...                             # sample CRs from doc 12
operator/charts/wavespan-operator/...                   # Helm chart packaging the operator
tests/integration/operator_kind_test.go                # kind/k3d e2e (build-tagged)
```

## Tasks

### Task 1: Scaffold the operator module + CRD API types (TS-090)

**Files:**
- Create: `operator/go.mod`, `operator/PROJECT`, all `operator/api/v1alpha1/*_types.go`, `operator/api/v1alpha1/groupversion_info.go`
- Test: `operator/api/v1alpha1/types_test.go`

- [ ] **Step 1:** Write failing test `TestCRDTypesRoundTrip` — construct each CR (`WaveSpanCluster`, `ReplicationPolicy`, `KVNamespace`, `Graph`, `VectorIndex`, `ClusterPeer`, `WaveSpanBackup`, `WaveSpanRestore`) from the doc-12 samples, marshal/unmarshal, assert fields survive (group `db.wavespan.io`, version `v1alpha1`).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Scaffold with Kubebuilder (`kubebuilder init`, `kubebuilder create api` per kind) and fill the `Spec`/`Status` structs to match doc 12 exactly (including `ReplicationPolicy.local.complianceBoundary`, `global.peers`, `conflict`, `antiEntropy`; `VectorIndex.approximate.*`; standard status conventions: `observedGeneration`, `phase`, `conditions`). Run `make generate manifests` (controller-gen deepcopy + CRD YAML).
- [ ] **Step 4:** Run test, expect PASS; `kubectl apply --dry-run=server -f operator/config/crd` against a kind cluster installs the CRDs (TS-090 acceptance).
- [ ] **Step 5:** Commit.

### Task 2: ConfigMap/Secret rendering + StatefulSet reconcile (TS-091)

**Files:**
- Create: `operator/internal/controller/resources_config.go`, `operator/internal/controller/resources_statefulset.go`, `operator/internal/controller/wavespancluster_controller.go`
- Test: `operator/internal/controller/statefulset_test.go` (envtest)

- [ ] **Step 1:** Write failing envtest `TestReconcileCreatesStatefulSetAndPVCs` — apply a `WaveSpanCluster` (replicas 3); reconcile; assert a StatefulSet named per the cluster exists with 3 replicas, the node image, the ports (gossip 7700, data 7800, repl 7801, admin 7900), the `WAVESPAN_*` env (runtime=kubernetes, POD_NAME/NODE_NAME via fieldRef), the data volumeMount, and a `volumeClaimTemplates` entry sized from `spec.storage.volumeSize` with `storageClassName`.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement config rendering (ConfigMap from spec: clusterId, ports, replication policyRef, security) and the StatefulSet builder (doc 09 layout) plus the controller `Reconcile` loop steps 1–5 (validate, ConfigMap, Secret, headless Service placeholder, StatefulSet) with `SetControllerReference` for GC.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 3: Headless + gateway Services, topology spread, PDB (TS-092)

**Files:**
- Create: `operator/internal/controller/resources_services.go`, `operator/internal/controller/resources_pdb.go`
- Modify: `operator/internal/controller/resources_statefulset.go` (topology spread)
- Test: `operator/internal/controller/services_test.go` (envtest)

- [ ] **Step 1:** Write failing tests:
  - `TestReconcileHeadlessService` — a `ClusterIP: None` Service selecting the data pods exists and is set as the StatefulSet `serviceName` (peer discovery).
  - `TestReconcileGatewayService` — when `spec.gateway.enabled`, a gateway Deployment (replicas from spec) + Service exist.
  - `TestTopologySpreadConstraints` — the pod template carries hostname `maxSkew:1` and zone `maxSkew:2` constraints, both `whenUnsatisfiable: ScheduleAnyway` (doc 09).
  - `TestPodDisruptionBudget` — a PDB with `maxUnavailable: 1` (from `spec.disruption.maxUnavailable`) selecting the data pods.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the Services, gateway Deployment, topology spread injection (using the configured topology labels with `ScheduleAnyway`), and PDB.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 4: Status conditions + scale paths (TS-091/092)

**Files:**
- Create: `operator/internal/controller/status.go`
- Modify: `operator/internal/controller/wavespancluster_controller.go`
- Test: `operator/internal/controller/status_test.go` (envtest)

- [ ] **Step 1:** Write failing tests:
  - `TestStatusConditions` — status reports `phase` (Ready/Degraded/Scaling/Upgrading/Error), `readyMembers`/`desiredMembers`, and conditions (`DataPodsReady`, `RepairHealthy`) per doc 09.
  - `TestScaleUp` — increasing `spec.replicas` scales the StatefulSet and phase passes through `Scaling`.
  - `TestScaleDownRetainsPVC` — decreasing replicas does **not** delete the orphaned PVC (doc 09 v1 rule).
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement status derivation from StatefulSet status + (optionally) the node admin endpoint, and ensure scale-down leaves PVCs intact.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 5: Drain protocol + rolling upgrade (TS-093)

**Files:**
- Create: `operator/internal/controller/drain.go`
- Modify: `operator/internal/controller/wavespancluster_controller.go`
- Test: `operator/internal/controller/drain_test.go` (envtest + fake node readiness)

- [ ] **Step 1:** Write failing tests:
  - `TestDrainAnnotationSet` — before a pod is replaced in a rolling update, the controller sets `wavespan.io/drain-requested=true` on it.
  - `TestRollingUpgradeWaitsForReadinessGate` — the controller waits for the pod's drain readiness gate (or timeout) before allowing termination; it does not delete a pod whose drain is incomplete unless the timeout elapses.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement `drain.go`: annotate, watch the pod's gossip/readiness gate condition (`wavespan.io/drained`), wait with a bounded timeout, then permit termination; orchestrate one-pod-at-a-time updates (respect `OrderedReady`/partition or manage deletions manually) so the cluster stays available under eventual semantics.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 6: Validating webhook for CRD policies (TS-090, doc 12 rules)

**Files:**
- Create: `operator/internal/webhook/replicationpolicy_webhook.go`
- Test: `operator/internal/webhook/replicationpolicy_webhook_test.go`

- [ ] **Step 1:** Write failing tests (one per doc-12 rule):
  - rejects `minAckNearbyReplicas < 1` in local-cache mode;
  - **rejects `require-local-geo` without a `complianceBoundary` label/value** (the require-local-geo compliance-boundary presence check);
  - rejects `targetNearbyReplicaCount < minAckNearbyReplicas`;
  - rejects `global.enabled` peers without TLS config;
  - rejects a `Graph`/`VectorIndex` referencing a missing cluster;
  - rejects `VectorIndex` with dimensions 0/missing;
  - accepts the valid doc-12 samples.
- [ ] **Step 2:** Run, expect FAIL.
- [ ] **Step 3:** Implement the validating webhook (`ValidateCreate`/`ValidateUpdate`) covering every rule; add CEL `+kubebuilder:validation:XValidation` markers where a rule is expressible declaratively (and regenerate manifests). Cross-resource checks (missing cluster ref) use a cached client lookup.
- [ ] **Step 4:** Run, expect PASS.
- [ ] **Step 5:** Commit.

### Task 7: Helm chart + kind/k3d end-to-end test

**Files:**
- Create: `operator/charts/wavespan-operator/...`
- Create: `tests/integration/operator_kind_test.go`

- [ ] **Step 1:** Package the operator (manager Deployment, RBAC, CRDs, webhook config) into a Helm chart.
- [ ] **Step 2:** Write `operator_kind_test.go` (`//go:build e2e`): spin up `kind` (or `k3d`), install CRDs + operator, apply a `WaveSpanCluster` sample, assert pods+PVCs come up and peers discover via the headless service; scale up and assert new pods join; trigger a one-pod rolling restart and assert it completes without permanent under-replication; apply an invalid `ReplicationPolicy` (require-local-geo without boundary) and assert the webhook rejects it.
- [ ] **Step 3:** Run `go test -tags e2e ./tests/integration -run OperatorKind`. Expect PASS.
- [ ] **Step 4:** Commit.

## Acceptance Criteria

From roadmap M11 + TS-090/091/092/093:

- **Cluster deploys in Kubernetes** — applying a `WaveSpanCluster` brings up the data StatefulSet, PVCs, headless + gateway Services, PDB, and topology spread; peers discover each other through the headless service (`operator_kind_test.go`).
- **Scale-up works** — increasing `spec.replicas` adds pods that join the cluster (`TestScaleUp`, e2e scale step).
- **Rolling restart keeps the cluster available under expected eventual semantics** — a one-pod rolling restart drains via the annotation protocol and completes without permanent under-replication (`TestRollingUpgradeWaitsForReadinessGate`, e2e).
- **CRD validation rejects invalid policies** — the webhook rejects every doc-12 invalid case, including `require-local-geo` without a compliance boundary (`replicationpolicy_webhook_test.go`).
- CRDs install into a test cluster (TS-090); data pods + PVCs created (TS-091); pods schedule and discover peers through the headless service (TS-092); one-pod rolling restart completes without permanent under-replication (TS-093).
- Scale-down retains PVCs (no auto-delete in v1).

## Verification

1. **Unit (envtest):** `cd operator && make test` — CRD round-trip, StatefulSet/PVC reconcile, Services, topology spread (`ScheduleAnyway`), PDB, status/scale, drain protocol, webhook rules.
2. **CRD install:** `kubectl apply -f operator/config/crd` into a kind cluster; `kubectl get crd | grep wavespan.io` shows all eight kinds.
3. **End-to-end:** `kind create cluster` (or `k3d cluster create`), `helm install` the operator, apply `operator/config/samples`, then `go test -tags e2e ./tests/integration -run OperatorKind`. Confirm pods Ready, PVCs Bound, peers discovered, scale-up adds members, rolling restart completes, and the invalid policy is rejected.
4. **Scale-down PVC drill:** scale replicas down by one; `kubectl get pvc` confirms the orphaned PVC is retained, not deleted.
