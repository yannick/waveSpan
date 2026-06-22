---
title: Security
section: Operations
order: 13
summary: mTLS between pods and clients, TLS 1.3 with session resumption and connection pooling for cheap handshakes, and the v1 auth model.
---

# Security

WaveSpan secures all RPC traffic with **mutual TLS** (`internal/security`). In production every connection — client-to-pod and pod-to-pod — is authenticated with certificates.

## mTLS everywhere

- All gRPC traffic runs over **TLS 1.3**.
- Pods present certificates to each other; the operator provisions the TLS secrets.
- `insecureDevMode: true` disables mTLS for local development **only** — never enable it in production.

```yaml
security:
  insecureDevMode: false
  certFile: /etc/wavespan/tls/tls.crt
  keyFile: /etc/wavespan/tls/tls.key
  caFile: /etc/wavespan/tls/ca.crt
```

## Cheap handshakes

mTLS handshakes are expensive if done naively. WaveSpan amortizes them (design doc 27):

- **TLS 1.3 session resumption** avoids full handshakes on reconnect.
- **Connection pooling** — one shared HTTP/2 client per node — reuses connections across many RPCs.
- Combined with `WithCompressMinBytes` (skip gzip for small payloads) and HTTP/2 keepalive, this keeps secure transport fast enough for point-read workloads.

## Authentication & authorization

- **v1** ships basic authentication. Admin credentials ride along with the browser's same-origin cookies/headers for the console.
- **Full RBAC** is a post-v1 enhancement.
- **Encryption at rest** is planned (the storage layer is designed to accommodate it).

## The console's transport

This UI authenticates by being served *from* the admin port and making **same-origin** requests:

```ts
const transport = createConnectTransport({
  baseUrl: window.location.origin,
  fetch: (input, init) =>
    fetch(input, { ...init, credentials: "same-origin" }),
});
```

So whatever protects the admin port (network policy, auth proxy, mTLS at the ingress) also protects the console — there is no separate credential path to manage.

## Operational guidance

- Treat the admin port (7900) as privileged — it serves metrics, the data browser (which can reveal values), and write capability via the KV Writer. Put it behind your cluster's auth proxy.
- Rotate TLS certificates via the operator; pods pick up new secrets on rolling restart.
- Keep `insecureDevMode` out of any non-local config — validation will warn, but it is your responsibility to enforce.
