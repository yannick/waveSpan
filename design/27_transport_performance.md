# 27. Transport performance: cheap mTLS

## Goal

Keep the mTLS that design/15 mandates on all machine link classes, while making it **as cheap as
possible**. The lever is *not* weaker encryption — it is eliminating handshakes through long-lived,
pooled, multiplexed connections and TLS 1.3 session resumption.

## Why "low encryption" is the wrong lever

Steady-state TLS cost is dominated by the **handshake** (asymmetric crypto + extra round trips +
certificate verification), paid **per connection** — not by bulk encryption. With AES-NI, AES-128-GCM
(and ChaCha20-Poly1305 on non-AES hardware) runs at multiple GB/s, negligible next to storage I/O.
The TLS 1.3 cipher suites are all fast AEADs and are not safely tunable "down". So we spend our
effort on **connection reuse**, not on the cipher.

## Link classes and their TLS mode

| Link class | TLS mode | Where |
|---|---|---|
| gateway → data pod | mTLS (require+verify client cert) | data server `TLSConfig` |
| data pod ↔ data pod (replication, cache fetch/subscribe, vector scatter) | mTLS | data server + shared client |
| gossip | mTLS | gossip server `TLSConfig` |
| admin API (machine operators) | mTLS when a client cert is presented | admin server `TLSConfig` |
| admin UI (human, browser) | server-side TLS; authorised by admin-token middleware | admin server `TLSConfig` |

The admin port is the one nuance: it serves both machine operators and the human-facing embedded UI
(design/26). It uses `tls.VerifyClientCertIfGiven` (`security.TLSConfig.ServerTLSOptionalClient`) so
the channel is always encrypted and an operator cert is still verified when offered, but a browser
without a client cert can still reach the UI (then authorised by the role/identity middleware).
Data and gossip remain strict mTLS (`RequireAndVerifyClientCert`).

## The five cost-removing techniques

1. **One shared, pooled, keepalive HTTP client** for every inter-node RPC. All subsystems (gossip,
   replication, cache fetch/subscribe, vector scatter) dial through the single
   `security.TLSConfig.NewHTTPClient`, so a completed handshake is reused across subsystems instead
   of each opening its own connection.
2. **HTTP/2 multiplexing** (`ForceAttemptHTTP2`, h2 via ALPN). Many concurrent RPCs ride one
   connection → one handshake. Negotiated automatically by net/http on the server and by the
   transport on the client; we do not hand-set `NextProtos`.
3. **TLS 1.3** (`MinVersion: VersionTLS13`): 1-RTT full handshake, 0-RTT-capable resumption.
4. **Session resumption**: the server issues TLS 1.3 session tickets (default on); each client
   carries a `ClientSessionCache` scoped to its own identity, so a reconnect resumes without a full
   handshake. The cache is per-client-identity, never process-global — a shared cache would let one
   identity resume another's authenticated session and skip client-cert verification.
5. **Fast ECDHE curves**: `CurvePreferences = {X25519, P-256}`. Pair with **ECDSA P-256** leaf/CA
   certs (cheap signatures) rather than RSA-2048/4096 for a markedly cheaper handshake.

## Configuration

```yaml
security:
  insecureDevMode: false        # dev-only plaintext escape hatch; never in prod
  certFile: /certs/tls.crt      # mTLS material (k8s Secret / cert-manager mount)
  keyFile:  /certs/tls.key
  caFile:   /certs/ca.crt
  transport:                    # connection-pool tuning (all optional; defaults below)
    maxIdleConns: 512
    maxIdleConnsPerHost: 64
    idleConnTimeoutSeconds: 600 # 10m: keep connections warm across gossip ticks / bursts
    tcpKeepAliveSeconds: 30
    dialTimeoutSeconds: 5
```

Environment overrides: `WAVESPAN_TLS_CERT_FILE`, `WAVESPAN_TLS_KEY_FILE`, `WAVESPAN_TLS_CA_FILE`,
`WAVESPAN_INSECURE_DEV_MODE`.

### Defaults (`security.DefaultTransportTuning`)

| Setting | Default | Rationale |
|---|---|---|
| `maxIdleConns` | 512 | total warm connections across all peers |
| `maxIdleConnsPerHost` | 64 | peers are long-lived; keep several warm per peer |
| `idleConnTimeout` | 10m | survive idle gaps so the next RPC skips the handshake |
| `tcpKeepAlive` | 30s | keep NAT/conntrack and the socket alive |
| `dialTimeout` | 5s | bound on a new connection incl. handshake |
| TLS min version | 1.3 | fast handshake; modern AEAD only |
| client `Client.Timeout` | unset | cache subscriptions are long-lived streams; rely on context |

## Validation rules

- `certFile`, `keyFile`, `caFile` must be set **together or not at all** (config fail-fast;
  `SecurityConfig.validate`).
- No certs **and** not `insecureDevMode` → the node refuses to start
  (`ErrPlaintextRequiresDevMode`), enforced when transports are wired so there is one source of truth.

## What we deliberately did NOT do

- **No weakened ciphers / null encryption** — ~0 throughput gain on AES-NI, loses confidentiality.
- **No global client session cache** — security hazard across identities (see technique 4).
- **No per-request client timeout** — would sever long-lived cache subscription streams.
- **No mTLS opt-out on data/gossip** — that removes caller identity, collapsing the role model
  ("replication credentials cannot drive public writes", design/15). The only opt-out is
  `insecureDevMode`, gated for dev/compose.

## Implementation

- `internal/security/tls.go` — `ServerTLS` (mTLS), `ServerTLSOptionalClient` (admin), `ClientTLS`
  (cheap-handshake settings + per-identity session cache).
- `internal/security/transport.go` — `TransportTuning`, `DefaultTransportTuning`, `NewHTTPClient`
  (shared pooled keepalive client).
- `internal/config/config.go` — `SecurityConfig` (cert paths + `TransportTuningConfig`), env
  overrides, incomplete-material validation.
- `cmd/wavespan-node/main.go` — builds the shared client once and passes it to every transport;
  serves data/gossip under mTLS and admin under optional-client TLS; `serve()` picks
  `ListenAndServeTLS` vs `ListenAndServe` from the presence of a `TLSConfig`.

## Implementation checklist

- [x] Shared pooled keepalive client wired into all inter-node RPC callers.
- [x] TLS 1.3 + fast curves + session resumption on client and server.
- [x] mTLS on data + gossip; optional-client TLS on admin (browser UI).
- [x] Cert paths + transport tuning in config with env overrides and fail-fast validation.
- [ ] HTTP/2 ping-based keepalive (`http2.Transport.ReadIdleTimeout`) — deferred; needs x/net/http2.
- [ ] Handshake-rate / connection-reuse metrics (`tls_handshakes_total`, idle-conn gauge).
