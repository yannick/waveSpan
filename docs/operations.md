# Operations runbook

This is the operator runbook for running WaveSpan in production. For local development see
[`getting-started-mac.md`](getting-started-mac.md).

## Deploy via the operator

WaveSpan ships a Kubernetes operator (`operator/`, group `db.wavespan.io`).

```bash
# 1. install the CRDs
kubectl apply -f operator/config/crd/bases

# 2. deploy the operator (manager Deployment + RBAC)
#    (helm chart / kustomize under operator/config)

# 3. apply a ReplicationPolicy + WaveSpanCluster
kubectl apply -f operator/config/samples/wavespancluster.yaml
```

The `WaveSpanCluster` controller reconciles:

- a data **StatefulSet** (image, ports 7700/7800/7900, `WAVESPAN_*` env, `volumeClaimTemplates`);
- a headless **Service** (`<cluster>-peers`, `ClusterIP: None`) for gossip peer discovery;
- a **PodDisruptionBudget** (`maxUnavailable: 1`);
- topology spread (`ScheduleAnyway` — spot-friendly; runtime repair is authoritative);
- an optional **gateway** Deployment + Service.

Check status: `kubectl get wavespancluster demo` shows `Phase` and `Ready/Desired` members.

## Configure replication

A `ReplicationPolicy` sets the durability and conflict behavior. Key fields:

- `local.targetNearbyReplicas` / `local.minAckNearbyReplicas` — origin+N target and the write-ACK
  threshold (`minAck: 1` = origin+1; must be ≥1 in local-cache mode).
- `local.geo` — `prefer-local-geo` | `require-local-geo` | `latency-only`. `require-local-geo`
  **requires** a `local.complianceBoundary` (label + value) or the webhook rejects it.
- `conflict.policy` — `hlc-last-write-wins` (default) or `keep-siblings`.
- `global` — active-active peers; `global.enabled` **requires** `global.tlsSecretName`.

The validating webhook rejects invalid policies (minAck < 1, require-local-geo without a boundary,
target < minAck, global without TLS, unknown conflict policy).

## Scale

```bash
kubectl patch wavespancluster demo --type=merge -p '{"spec":{"replicas":5}}'
```

- **Scale up** adds pods that join via gossip and receive repaired replicas.
- **Scale down retains PVCs** — the operator never deletes orphaned PVCs in v1. Delete them by hand
  if you are sure the data is not needed.

## Rolling upgrades / drain

A rolling update drains one pod at a time:

1. the operator sets `wavespan.io/drain-requested=true` on the next pod;
2. the pod marks itself `DRAINING` in gossip, flushes its mutation log, and stops accepting new
   coordinator writes when alternatives exist;
3. the operator waits for the pod's `wavespan.io/drained` gate (or a timeout) before terminating;
4. the replacement rejoins; repair restores any briefly under-replicated keys.

The cluster stays available under eventual semantics throughout.

## Backup & restore

Backups snapshot the **authoritative** column families. Derived ANN vector indexes are **not**
backed up — they are rebuilt from the raw vector records on restore.

```bash
# node-local backup (run against a stopped node or a volume snapshot)
wavespanctl backup --storage /var/lib/wavespan --out /backups/demo.wsb

# restore into a fresh storage dir; the node rebuilds vector ANN indexes on startup
wavespanctl restore --storage /var/lib/wavespan-new --in /backups/demo.wsb
```

(Production backup uses wavesdb object-store mode + `PromoteToPrimary`; the CLI is the logical
equivalent for the prototype.)

## Observe

- Dashboards: `observability/dashboards/wavespan_overview.json` (under-replication, repair queue,
  global lag, anti-entropy, TTL).
- Alerts: `observability/alerts/wavespan_alerts.yaml` — validate with
  `promtool check rules observability/alerts/wavespan_alerts.yaml`.

Watch `kv_under_replicated_keys_estimate` (should drain to zero after churn),
`global_repl_out_lag_seconds` (peer replication health), and `global_repl_apply_errors_total`
(should stay flat).

## Nightly convergence gate

The release gate runs the model-aware correctness harness (bank/register/set workloads under
node-kill / partition / cross-cluster-partition nemeses) and asserts convergence + durability
**after faults heal**. See [`development.md`](development.md) and `tests/chaos/`.
