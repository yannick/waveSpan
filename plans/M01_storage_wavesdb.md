# M01 - Local storage on wavesdb: LocalStore, envelopes, storage UUID

**Milestone:** M1 (`design/18_implementation_roadmap.md` "Milestone 1")
**Tickets:** TS-010, TS-011, TS-012 (`design/19_agent_work_items.md`)
**Depends on:** M0 (module graph, `internal/version`, proto `common.proto`)
**Enables:** M3 (KV), M8 (graph), and forks the graph/vector track
(`IMPLEMENTATION_STRATEGY.md` section 2)

## Context

WaveSpan needs a storage abstraction so the distributed layers are not coupled to the exact
`wavesdb` API (`design/02_storage_wavesdb.md` "Storage wrapper"). M1 delivers that boundary
as an idiomatic Go interface (`LocalStore`), a `wavesdb`-backed implementation, an in-memory
implementation for tests, the versioned record envelopes that every replicated write uses,
and durable storage identity (storage UUID) in column family `sys`.

`wavesdb` (imported via the M0 `replace wavesdb => ../wavesdb`) exposes: `Open(Options)`,
`CreateColumnFamily(name, ColumnFamilyOptions)`, `GetColumnFamily(name)`, `Begin()` /
`BeginWithIsolation(Snapshot|Serializable)`, and on a `*Txn`:
`Put(cf, key, value, ttl time.Duration)`, `Get`, `Delete`, `SingleDelete`,
`NewIterator(cf)`, `Commit`, `Rollback`. The `*Iterator` exposes
`Seek`/`SeekForPrev`/`SeekToFirst`/`SeekToLast`/`Next`/`Prev`/`Key`/`Value`/`Valid`/`Err`/`Close`.
The DB exposes `FlushMemtable(cf)`, `Compact(cf)`, `Checkpoint(dir)`, `DropColumnFamily`,
`CloneColumnFamily`, `Close`. Native LSM TTL is available through the `ttl` argument on
`Txn.Put` and the iterator's TTL/tombstone surface (used later by M6 TTL).

WaveSpan's logical column families (`design/02_storage_wavesdb.md` "Column families") map to
real `wavesdb` column families: `sys`, `kv_data`, `kv_meta`, `graph_data`, `graph_index`,
`vector_raw`, `vector_index`, `repl_log`, `cache_meta`.

## Files to create

```
internal/storage/store.go          LocalStore interface, ColumnFamily enum, StoreOp, StoreKV, Snapshot
internal/storage/errors.go         storage error mapping (NotFound, Conflict, Closed, ...)
internal/storage/cf.go             logical CF registry -> wavesdb CF names + ColumnFamilyOptions
internal/storage/wavesdb_store.go  wavesdb-backed LocalStore (Open/Begin/Txn/Iterator/Flush/Compact)
internal/storage/mem_store.go      in-memory LocalStore (ordered map; for unit tests)
internal/storage/identity.go       storage UUID persistence in CF sys
internal/storage/store_test.go     shared conformance suite run against BOTH stores
internal/storage/identity_test.go  UUID persists across reopen
proto/wavespan/v1/common.proto     EXTEND: StoredRecord, LatestPointer, MutationEnvelope, ValueBody, enums
internal/storage/envelope.go       Go wrappers + encode/decode for the above proto messages
internal/storage/envelope_test.go  envelope round-trip, latest-pointer rebuild, tombstone-under-LWW
```

## Steps

1. **Extend `proto/wavespan/v1/common.proto`** (regenerate with `make proto`) with the
   envelope messages from `design/02_storage_wavesdb.md`:
   - `StoredRecord` (logical_key, value, version, expires_at_unix_ms, tombstone, kind,
     namespace, origin_cluster_id, origin_member_id, local_apply_unix_ms, conflict_state);
   - `LatestPointer` (winner, sibling_versions, expires_at_unix_ms, tombstone,
     local_generation);
   - `MutationEnvelope` (mutation_id, kind, logical_key, value, version,
     expires_at_unix_ms, tombstone, namespace, origin_cluster_id, origin_member_id,
     origin_sequence, causal_parents);
   - `ValueBody { oneof { bytes inline; ExternalPointer external } }` (inline only in v1;
     keep the oneof for future object-storage offload).
   Reuse `Version` from M0.

2. **`LocalStore` interface, `internal/storage/store.go`.** Translate the Rust trait sketch
   in `design/02_storage_wavesdb.md` into idiomatic Go (this is the canonical boundary; the
   Rust is spec, not language — `IMPLEMENTATION_STRATEGY.md` section 1):

   ```go
   type ColumnFamily int // Sys, KVData, KVMeta, GraphData, GraphIndex, VectorRaw, VectorIndex, ReplLog, CacheMeta

   type LocalStore interface {
       Put(cf ColumnFamily, key, value []byte) error
       Get(cf ColumnFamily, key []byte) ([]byte, bool, error)
       Delete(cf ColumnFamily, key []byte) error
       Batch(ops []StoreOp) error              // atomic via a single wavesdb Txn
       Scan(cf ColumnFamily, start, end []byte, limit int) (Iterator, error)
       Snapshot() (Snapshot, error)
       Flush(cf ColumnFamily) error
       CompactRange(cf ColumnFamily, start, end []byte) error
       Close() error
   }
   ```
   Define `StoreOp` (cf, key, value, isDelete), `StoreKV` (key, value), `Iterator`
   (`Next`/`Key`/`Value`/`Valid`/`Err`/`Close`), and `Snapshot` (a read-consistent view that
   produces iterators).

3. **CF registry, `internal/storage/cf.go`.** Map each logical `ColumnFamily` to a `wavesdb`
   CF name and a `ColumnFamilyOptions`. On open, `GetColumnFamily` or `CreateColumnFamily`
   each required family. Keep options conservative (default comparator; bytewise order so the
   internal keyspace in `design/01_architecture.md` sorts correctly).

4. **`wavesdb` implementation, `internal/storage/wavesdb_store.go`.**
   - `Open(path)` -> `wavesdb.Open(Options{...})`; ensure the nine CFs exist.
   - `Put`/`Get`/`Delete`: short transactions (`Begin` -> `Txn.Put/Get/Delete` -> `Commit`),
     mapping `wavesdb` not-found to `(nil, false, nil)`.
   - `Batch`: one `Begin`, apply all ops, `Commit` (atomic) or `Rollback` on error. This is
     the primitive M3/M5 use to write record + latest pointer + mutation-log entry together
     (`design/05_special_cache_replication.md` write algorithm).
   - `Scan`: `Begin` (snapshot isolation) -> `NewIterator(cf)` -> `Seek(start)`; yield until
     `>= end` or `limit`. Reuse the iterator and avoid per-row allocation (GC-pause risk,
     `IMPLEMENTATION_STRATEGY.md` section 4); `Close` the iterator and roll back the read txn.
   - `Snapshot`: `BeginWithIsolation(Snapshot)`; iterators derive from it.
   - `Flush`/`CompactRange`: delegate to `FlushMemtable(cf)` / `Compact(cf)` (the M1
     "compaction hook" of TS-011; range-scoped compaction approximated by full-CF compact
     where `wavesdb` lacks range compaction).
   - Map `wavesdb` errors to `internal/storage/errors.go` typed errors.

5. **In-memory implementation, `internal/storage/mem_store.go`.** An ordered structure (e.g.
   a B-tree or sorted slice per CF guarded by a mutex) implementing `LocalStore` with the
   same semantics, including atomic `Batch` and snapshot iterators. This is the M0/TS-010
   target store that KV unit tests run against without `wavesdb`.

6. **Storage identity, `internal/storage/identity.go`.** Per
   `design/02_storage_wavesdb.md` "Crash recovery" steps 1-3: on open, read the storage UUID
   from CF `sys` (key `/sys/storage_uuid`); if absent, generate a UUID v4 and persist it.
   Expose `StorageUUID()`. This is durable storage identity, distinct from the runtime
   `memberId` (`design/04_membership_latency_gossip.md` "Member identity"); M2 consumes it.

7. **Envelope helpers, `internal/storage/envelope.go`.** Encode/decode `StoredRecord`,
   `LatestPointer`, `MutationEnvelope`; build the canonical keys from
   `design/01_architecture.md` "Internal keyspace" (`/kv/{ns}/data/{key}/{version}`,
   `/kv_meta/latest/{ns}/{key}`, `/repl_log/local/{partition}/{seq}`). Provide a function to
   **rebuild the latest pointer** from the versioned records + mutation log for a key
   (`design/02_storage_wavesdb.md` "Required local invariants" 3): pick the LWW winner via
   `version.Compare`, collect siblings under a siblings policy, and carry tombstone/expiry.

## Acceptance criteria

From `design/18_implementation_roadmap.md` Milestone 1 and the TS tickets:

- Put/get/scan works on one node; restart preserves data; storage UUID persists. (M1)
- The KV conformance suite passes against the in-memory store. (TS-010)
- Put/get/scan persists across an `Open`/`Close`/`Open` cycle on the `wavesdb` store; storage
  UUID persists across restart. (TS-011)
- Latest pointer rebuilds from records/log; a tombstone hides an older winner under LWW when
  the tombstone version wins. (TS-012)

## Verification

1. **Shared conformance suite (`store_test.go`)** is table-driven and parameterized over a
   store factory, run once for `mem_store` and once for `wavesdb_store` (the latter against a
   temp dir). It covers put/get/delete, ordered scan with `start`/`end`/`limit`, atomic
   `Batch` (all-or-nothing on injected mid-batch error), and snapshot isolation (a scan
   started before a concurrent write does not observe it).
2. **Restart durability:** write keys, `Close`, reopen the same path, assert reads return the
   same values and `StorageUUID()` is unchanged (TS-011, M1 acceptance).
3. **Envelope tests:** round-trip each proto envelope; rebuild a latest pointer from a
   shuffled set of versioned records + log entries and assert it matches the `version.Compare`
   winner; assert a winning tombstone hides an older value (TS-012, and the storage half of
   property 2/3 in `IMPLEMENTATION_STRATEGY.md` section 3).

No cluster or docker run yet; M1 is single-node storage. The 3-node docker-compose and bank
invariant first run end-to-end at M3.
