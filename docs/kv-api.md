# KV API

The public key-value API is the `wavespan.v1.KvService`, served over [Connect](https://connectrpc.com)
on each data pod's **data port** (default `:7800`). Connect speaks gRPC, gRPC-Web, and its own
HTTP/1.1-friendly protocol (including JSON), so you can call it from generated clients, `grpcurl`,
or plain `curl`.

Keys and values are arbitrary bytes. Namespaces are logical partitions of the keyspace.

## Operations

| RPC | Semantics |
|---|---|
| `Put(namespace, key, value, ttl?, idempotencyKey?)` | Eventual. ACKs after origin + `minAck` nearby durable replicas. |
| `Get(namespace, key)` | Eventual, local-first; on a miss, fetches from the closest holder and caches it. |
| `Delete(namespace, key)` | Eventual tombstone write (a `Put` with `tombstone=true`). |
| `Scan(namespace, start, end, limit, mode)` | Streaming range scan; declares completeness. |

### With `wavespanctl`

```bash
wavespanctl kv put default user:1 alice          # --ttl <ms> for an expiring key
wavespanctl kv get default user:1
wavespanctl kv delete default user:1
wavespanctl kv scan default --mode cache-fast     # or routed | cache-complete | local
wavespanctl --addr localhost:7811 kv get default user:1   # target a specific pod
```

### Over Connect JSON (curl)

`bytes` fields are base64 in Connect JSON:

```bash
# Put default/foo = bar
curl -sS localhost:7800/wavespan.v1.KvService/Put \
  -H 'Content-Type: application/json' \
  -d "{\"namespace\":\"default\",\"key\":\"$(printf foo|base64)\",\"value\":\"$(printf bar|base64)\",\"requireOriginPlusOne\":true}"

# Get default/foo
curl -sS localhost:7800/wavespan.v1.KvService/Get \
  -H 'Content-Type: application/json' \
  -d "{\"namespace\":\"default\",\"key\":\"$(printf foo|base64)\"}"
```

## Response metadata — reads are honest

Every read carries a `ResponseMeta`. WaveSpan never hides that a value may be stale or fetched
remotely. The key field is the **read source**:

| `source` | Meaning |
|---|---|
| `LOCAL_DURABLE` | served from a durable copy on the serving pod |
| `LOCAL_DYNAMIC_CACHE` | served from a dynamic cache replica on the serving pod |
| `FETCHED_CLOSEST_HOLDER` | this read missed locally and fetched from the closest holder |

`ResponseMeta` also reports `observed_version` (the HLC version observed), `conflict_state`
(`CONFLICT_NONE` / `CONFLICT_SIBLINGS_PRESENT`), and `completeness`.

## Writes — origin+1 and `acked_nearby_replicas`

`PutResult` reports `acked_nearby_replicas`: how many nearby durable replicas acknowledged before
the write returned. Under the default policy that is `1` (origin+1). The background fanout then
fills the cluster to the target replica count.

A write **fails** with `Unavailable: insufficient nearby durable replicas for origin+1` if no
nearby replica can be reached — durability is never silently dropped. (Set `minAck=0` for single-
node / local-only development; see [Configuration](configuration.md).)

### Idempotency

Pass `idempotency_key` on a `Put`/`Delete` to make retries safe: the same key collapses to exactly
one logical mutation across retries, reconnects, and replica receivers. The mutation identity is
`cluster + member + writer_sequence`, so re-sending an originated mutation is a no-op on the receiver.

## TTL

Pass `ttl_ms` on a `Put` to set an expiry. TTL is **lazy and best-effort** (design choice):

- on read, a node may hide a record it detects as expired (no promise all nodes detect expiry at
  the same instant);
- a background sweeper tombstones expired keys in coarse buckets; the tombstone replicates and
  participates in conflict resolution like any delete;
- physical cleanup happens later via compaction.

So an expired key stops being returned within roughly `bucketSize + sweepInterval + replicationLag`
of its deadline. Do not rely on TTL for exact-time deletion.

## Scans and completeness

`Scan` streams a header, then rows, then a trailer. The header and trailer declare the
**completeness** actually achieved — a partial cache scan is never presented as complete:

| Mode | Completeness | Notes |
|---|---|---|
| `cache-fast` (default) | `BEST_EFFORT` | local cache/durable only; fast, may be incomplete |
| `routed-eventual` | `PARTIAL` | contacts known holders, k-way merges sorted/deduped keys |
| `cache-complete` | `COMPLETE` only with a valid range-coverage certificate, else `BEST_EFFORT` | strong completeness gate |
| `local-only` | `BEST_EFFORT` | local store only; debugging/analytics |

```bash
wavespanctl kv scan default --mode routed
# mode=ROUTED_EVENTUAL completeness=PARTIAL
# a	1
# b	2
# rows=2 completeness=PARTIAL
```

Range scans within a namespace return keys in ascending byte order.

## Errors

Errors use Connect/gRPC status codes:

- `Unavailable` — origin+1 could not be satisfied (no nearby replica).
- `Internal` — a storage or transport error.
- `Unimplemented` — a capability not enabled on this build (e.g. `Scan` if the scanner is off).
