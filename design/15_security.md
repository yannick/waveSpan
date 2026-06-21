# 15. Security

## Scope

Single tenant does not mean no security. Production must secure pod-to-pod, gateway-to-data, and cross-cluster replication traffic.

## Transport security

Use mTLS for:

- gateway to data pod;
- data pod to data pod;
- replication API;
- gossip API if metadata sensitivity requires it;
- cross-cluster global replication;
- admin API.

## Identity

Kubernetes mode:

- use ServiceAccount identity;
- mount certs through Secrets or cert-manager;
- optionally support SPIFFE/SPIRE.

Docker mode:

- use local dev CA;
- allow insecure mode only with explicit flag:

```yaml
security:
  insecureDevMode: true
```

## Authorization

V1 roles:

| Role | Capabilities |
|---|---|
| `admin` | all operations |
| `reader` | KV get/scan, Cypher read queries |
| `writer` | KV put/delete, Cypher writes |
| `replicator` | internal replication only |
| `operator` | admin lifecycle endpoints |

Even single-tenant systems need internal endpoint separation. Replication credentials must not be usable for public writes.

## Encryption at rest

V1 should support volume-level encryption through cloud/Kubernetes storage class.

Application-level encryption can be added later.

## Secrets

Operator manages:

- node TLS certs;
- gateway TLS certs;
- peer-cluster TLS certs;
- admin tokens;
- backup credentials.

Never store secrets in CRD plain text.

## Data redaction

Logs and metrics must not include raw keys or values by default.

Use:

```text
key_hash = base64url(blake3(namespace + key))
```

Debug endpoints require explicit `includeValue=true` and admin auth.

## Global replication security

Peer clusters must authenticate each other.

Requirements:

- mTLS;
- peer cluster allowlist;
- replay protection through mutation IDs;
- per-peer rate limits;
- audit logs for connection and apply errors.

## Implementation checklist

- [ ] mTLS support implemented.
- [ ] Auth middleware implemented for public/admin/internal APIs.
- [ ] Internal service role separation implemented.
- [ ] Logs redact raw keys/values.
- [ ] Operator creates or references Secrets.
- [ ] Docker insecure mode clearly gated.
- [ ] Cross-cluster peer authentication implemented.

