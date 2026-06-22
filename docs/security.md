# Security

WaveSpan secures every API surface with mTLS and a five-role authorization model. The headline
guarantee: **replication credentials cannot drive public writes.**

## Transport: mTLS

All traffic — gateway↔data, data↔data replication, cross-cluster replication, and admin — uses
mutual TLS (`tls.RequireAndVerifyClientCert`, TLS 1.3). Both ends present a certificate signed by a
trusted CA.

- In Kubernetes, certs are mounted from Secrets (provision with cert-manager or your PKI).
- Plaintext is permitted **only** behind an explicit `security.insecureDevMode: true` (local dev /
  the Docker compose clusters). Production config with no certs and no dev mode **fails to start**.

## Roles

| Role | Capabilities |
|---|---|
| `admin` | everything |
| `reader` | KV `Get`/`Scan`, Cypher read |
| `writer` | KV `Put`/`Delete`, Cypher writes |
| `replicator` | **internal replication only** (`StoreReplica`, `PushGlobal`, …) |
| `operator` | admin lifecycle |

A caller's role is derived from its **verified client certificate** (by SAN/SPIFFE id). In dev mode
only, an `X-WaveSpan-Role` header is accepted.

### Internal / public separation (the point)

API surfaces are classified public-read, public-write, internal, or admin. The capability matrix
enforces:

- **`replicator` may call ONLY the internal surface** — never public `Put`/`Delete`. A leaked
  replication credential cannot be used to write application data.
- The **internal surface accepts only `replicator` and `admin`** — an unauthenticated or public-only
  caller hitting `StoreReplica`/`PushGlobal` is rejected (`Unauthenticated`/`PermissionDenied`).
- Unclassified procedures default to the admin surface (deny-by-default).

## Cross-cluster peers

Peers authenticate with mTLS **and** must appear on the per-cluster **peer allowlist** — a
`replicator` identity not on the allowlist is downgraded to no access. Replay is prevented by
mutation IDs (`cluster+member+sequence`); each peer is rate-limited (per-peer token bucket); peer
connections and apply errors are written to the **audit log**.

## Data redaction

Logs and metrics **never** contain raw keys or values by default. The redacted identifier is:

```
key_hash = base64url(blake3(namespace + key))[:128 bits]
```

Values are shown only on debug endpoints that require `includeValue=true` **and** an `admin`
identity. Everything else renders `<redacted>`.

## Operator checklist

- Provision a CA and per-pod certs (cert-manager recommended); reference them via
  `security.tlsSecretName`.
- Never set `insecureDevMode: true` in production.
- Set `global.tlsSecretName` whenever `global.enabled` (the webhook enforces this).
- Maintain the peer allowlist; review the audit log for unexpected peer identities.
- Validate alert rules with `promtool` and confirm dashboards show `key_hash`, not raw keys.
