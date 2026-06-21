# 02. Local storage on wavesdb

## Role of wavesdb

`wavesdb` is the embedded local storage engine. It is a Go LSM-tree key-value engine — a mature, tested Go rewrite of the C engine `tidesdb` — and WaveSpan imports it in-process as a Go library. It is not the distributed database, and the data path never crosses an FFI boundary.

`tidesdb` (the C ancestor) is reference-only and is not part of the build. See `adr/0005_go_and_wavesdb_engine.md`.

WaveSpan must build these above `wavesdb`:

- membership;
- replication;
- cache subscriptions;
- conflict resolution;
- range routing;
- graph indexes;
- vector indexes;
- global replication;
- repair;
- backup coordination;
- observability.

## What wavesdb already provides

These are real, tested `wavesdb` features that WaveSpan builds on rather than reimplements:

- column families (`CreateColumnFamily` / `GetColumnFamily` / `ListColumnFamilies`);
- MVCC transactions as a batch + snapshot (`Begin` / `BeginWithIsolation`, then `Txn.{Put,Get,Delete,Commit,Rollback,NewIterator}`);
- five isolation levels: `ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `Snapshot` (default), `Serializable`;
- bidirectional iterators (`Seek`, `SeekForPrev`, `Next`, `Prev`, `SeekToFirst`, `SeekToLast`, `Valid`, `Key`, `Value`);
- native per-key TTL (`Put(cf, key, value, ttl)` with a `time.Duration`);
- `Checkpoint`, `Compact`, `FlushMemtable`;
- an object-store replica with `PromoteToPrimary`.

## Storage wrapper

Implement a thin Go storage abstraction so the distributed layers are not tightly coupled to the exact `wavesdb` API and so tests can inject a fake. This lives in `internal/storage` and is a wrapper, not a portability layer: `wavesdb` is the engine in v1, not a swappable backend.

```go
package storage

import (
	"context"
	"time"
)

// CF names a logical column family. The adapter maps each to a real
// wavesdb column family created at open time.
type CF string

const (
	CFSys         CF = "sys"
	CFKVData      CF = "kv_data"
	CFKVMeta      CF = "kv_meta"
	CFGraphData   CF = "graph_data"
	CFGraphIndex  CF = "graph_index"
	CFVectorRaw   CF = "vector_raw"
	CFVectorIndex CF = "vector_index"
	CFReplLog     CF = "repl_log"
	CFCacheMeta   CF = "cache_meta"
)

// KV is one decoded key/value pair returned by a scan.
type KV struct {
	Key   []byte
	Value []byte
}

// WriteOp is one mutation inside an atomic batch.
type WriteOp struct {
	CF     CF
	Key    []byte
	Value  []byte // ignored for Delete
	TTL    time.Duration // 0 = no expiry; native wavesdb per-key TTL
	Delete bool
}

// Snapshot is a read-only, point-in-time view. Backed by a read-only
// Snapshot-isolation wavesdb transaction.
type Snapshot interface {
	Get(cf CF, key []byte) ([]byte, bool, error)
	Scan(cf CF, start, end []byte, limit int) ([]KV, error)
	Close() error
}

// LocalStore is the in-process boundary over wavesdb. Every method is a
// direct Go call; there is no FFI.
type LocalStore interface {
	Put(ctx context.Context, cf CF, key, value []byte, ttl time.Duration) error
	Get(ctx context.Context, cf CF, key []byte) (value []byte, found bool, err error)
	Delete(ctx context.Context, cf CF, key []byte) error

	// Batch applies all ops atomically across keys and column families
	// in a single wavesdb transaction (Begin -> ops -> Commit).
	Batch(ctx context.Context, ops []WriteOp) error

	// Scan returns an ordered slice. For streaming, use NewIterator.
	Scan(ctx context.Context, cf CF, start, end []byte, limit int) ([]KV, error)

	// NewIterator exposes the bidirectional wavesdb iterator for the CF.
	NewIterator(cf CF) (Iterator, error)

	// Snapshot opens a read-only Snapshot-isolation transaction.
	Snapshot(ctx context.Context) (Snapshot, error)

	// CompactRange triggers compaction for the CF.
	CompactRange(ctx context.Context, cf CF) error

	// Flush forces the active memtable of the CF to an SST.
	Flush(ctx context.Context, cf CF) error

	Close() error
}

// Iterator mirrors the wavesdb iterator surface.
type Iterator interface {
	Seek(key []byte)
	SeekForPrev(key []byte)
	SeekToFirst()
	SeekToLast()
	Next()
	Prev()
	Valid() bool
	Key() []byte
	Value() []byte
	Err() error
	Close()
}
```

### Mapping to the real wavesdb API

The `wavesdb` adapter maps each `LocalStore` method onto the engine surface directly:

| `LocalStore` method | wavesdb call(s) |
|---|---|
| `Put` | `DB.Put(cf, key, value, ttl)`, or a `Txn` when grouped with other ops |
| `Get` | `DB.Get(cf, key)` |
| `Delete` | `DB.Delete(cf, key)` |
| `Batch` | `db.Begin()` → `Txn.Put` / `Txn.Delete` per op → `Txn.Commit()`; atomic across keys and CFs. On conflict (`Snapshot`/`Serializable`) the commit returns `ErrConflict` and the batch is retried |
| `Scan` | `db.BeginWithIsolation(Snapshot)` (read-only) → `Txn.NewIterator(cf)` → `Seek(start)` then `Next` until `>= end` or `limit` |
| `NewIterator` | `Txn.NewIterator(cf)` exposing `Seek`/`SeekForPrev`/`Next`/`Prev`/`SeekToFirst`/`SeekToLast`/`Valid`/`Key`/`Value` |
| `Snapshot` | `db.BeginWithIsolation(Snapshot)` held read-only; reads are repeatable for the snapshot's lifetime |
| `CompactRange` | `DB.Compact(cf)` |
| `Flush` | `DB.FlushMemtable(cf)` |

Open-time setup uses `wavesdb.Open` and creates each logical CF with `CreateColumnFamily`; `GetColumnFamily` resolves the handle thereafter, and `ListColumnFamilies` is used by diagnostics.

Isolation: WaveSpan opens read paths at `Snapshot` for repeatable point-in-time reads, and uses `Snapshot` (or `Serializable` where write-skew matters, e.g. graph index maintenance) for write batches that must detect write-write conflicts. `ReadCommitted` is used for cheap single-key reads where staleness within the call is acceptable.

## Column families / logical keyspaces

The column families below map 1:1 onto real `wavesdb` column families created via `CreateColumnFamily` at open time. There is no prefixed-key fallback in v1 — `wavesdb` has native column families.

| Family | `CF` constant | Purpose |
|---|---|---|
| `sys` | `CFSys` | local member metadata, storage UUID, config snapshot |
| `kv_data` | `CFKVData` | user KV versions |
| `kv_meta` | `CFKVMeta` | latest-version pointers, holder summaries, TTL metadata |
| `graph_data` | `CFGraphData` | node and relationship records |
| `graph_index` | `CFGraphIndex` | label, property, adjacency indexes |
| `vector_raw` | `CFVectorRaw` | raw vector payloads |
| `vector_index` | `CFVectorIndex` | ANN/exact index metadata and segment data |
| `repl_log` | `CFReplLog` | local and global mutation logs |
| `cache_meta` | `CFCacheMeta` | dynamic cache subscriptions, watermarks, cache leases |

## Local record format

Use a versioned envelope for all replicated records. The protobuf definitions below are language-neutral; the accompanying access code is Go.

```protobuf
message StoredRecord {
  bytes logical_key = 1;
  bytes value = 2;
  Version version = 3;
  optional int64 expires_at_unix_ms = 4;
  bool tombstone = 5;
  RecordKind kind = 6;
  string namespace = 7;
  string origin_cluster_id = 8;
  string origin_member_id = 9;
  int64 local_apply_unix_ms = 10;
  ConflictState conflict_state = 11;
}

message Version {
  uint64 hlc_physical_ms = 1;
  uint32 hlc_logical = 2;
  string writer_cluster_id = 3;
  string writer_member_id = 4;
  uint64 writer_sequence = 5;
  bytes vector_clock = 6;
}
```

The `Version` semantics (HLC, vector clocks, comparison) are specified in `22_versioning_and_hlc.md`.

## Latest pointer

For fast reads, maintain a latest pointer in `kv_meta`:

```text
/kv_meta/latest/{namespace}/{user_key} -> LatestPointer
```

```protobuf
message LatestPointer {
  Version winner = 1;
  repeated Version sibling_versions = 2;
  optional int64 expires_at_unix_ms = 3;
  bool tombstone = 4;
  uint64 local_generation = 5;
}
```

If conflict policy is `siblings`, reads return all siblings unless the client specifies a resolver.

## Mutation log

Every local write and every applied remote mutation must append to a local mutation log (in `repl_log`) before dependent indexes/caches are updated. The append and the data write go in the same `wavesdb` transaction so they commit atomically.

```text
/repl_log/local/{partition}/{seq} -> MutationEnvelope
```

Purpose:

- local repair;
- dynamic subscription replay;
- global replication;
- graph index maintenance;
- vector index maintenance;
- crash recovery.

Mutation envelope:

```protobuf
message MutationEnvelope {
  string mutation_id = 1;
  MutationKind kind = 2;
  bytes logical_key = 3;
  bytes value = 4;
  Version version = 5;
  optional int64 expires_at_unix_ms = 6;
  bool tombstone = 7;
  string namespace = 8;
  string origin_cluster_id = 9;
  string origin_member_id = 10;
  uint64 origin_sequence = 11;
  repeated string causal_parents = 12;
}
```

## TTL storage

TTL is lazy and best effort at the WaveSpan layer, with two complementary mechanisms:

1. **Native per-key TTL in `wavesdb`** handles physical GC where it fits. Single-key, single-namespace writes pass the record's remaining lifetime as the `ttl time.Duration` to `Put`/`Txn.Put`, and `wavesdb` drops the entry during compaction once expired. Use this for the common case so WaveSpan does not carry its own physical reclamation for those keys.

2. **The WaveSpan TTL-bucket index** sits on top for the cross-replica lazy-sweeper semantics that native TTL cannot express — coordinating expiration visibility and tombstone propagation across replicas that may not have observed expiry yet.

Store TTL bucket metadata in coarse buckets in `kv_meta`:

```text
/kv_meta/ttl/{bucket_start_unix_ms}/{namespace}/{key_hash}/{key} -> version
```

Use buckets instead of exact timestamps to reduce write amplification.

Recommended default bucket size:

```yaml
ttlBucketSeconds: 60
```

Read behavior:

- if local node notices `expires_at <= now`, it may hide the record;
- if it has not noticed yet, stale reads are allowed by default;
- strict namespaces may set `hideExpiredOnRead: true`.

Compaction behavior:

- native `wavesdb` per-key TTL reclaims expired entries during normal compaction;
- the WaveSpan TTL sweeper scans old buckets and writes tombstones for expired current versions so the deletion propagates to replicas;
- later `wavesdb` compaction removes old versions and tombstones.

The two mechanisms are not redundant: native TTL gives cheap local physical reclamation; the bucket index gives cross-replica convergence of expiry. A record may be physically gone locally while a lagging replica still serves it until the tombstone arrives — which is exactly the lazy, best-effort contract.

## Value size policy

Default v1 stores values and raw vectors inside `wavesdb`.

Add an abstraction for future object-storage offload:

```protobuf
message ValueBody {
  oneof body {
    bytes inline = 1;
    ExternalPointer external = 2;
  }
}
```

Do not implement object storage in v1 unless needed by benchmarks. Note that `wavesdb` itself can maintain an object-store replica and supports `PromoteToPrimary`; this is the basis for backup/restore and disaster recovery (see below) and is distinct from application-level large-value offload.

## Backup, restore, and object-store replica

`wavesdb` provides two engine-level primitives WaveSpan builds backup/restore on:

- **`Checkpoint`** produces a consistent on-disk snapshot of the engine state for a point-in-time local backup.
- **object-store replica + `PromoteToPrimary`** lets a replica be backed by object storage and promoted to primary, which is the basis for off-cluster backup and restore: a pod can be reconstituted from the object-store replica and promoted rather than rebuilt purely from peer repair.

The operator's backup/restore orchestration (see `09_kubernetes_operator.md`) drives these primitives; WaveSpan does not implement its own SST shipping in v1.

## Crash recovery

On startup:

1. open `wavesdb` via `wavesdb.Open` and resolve/create the logical column families;
2. read local storage UUID from `sys`;
3. if absent, initialize new storage identity;
4. replay unapplied mutation logs into latest pointers and derived indexes;
5. rebuild cache subscription state as inactive;
6. rejoin membership gossip;
7. advertise held key/range summaries;
8. trigger repair for under-replicated local data.

Because data and mutation-log writes commit in the same `wavesdb` transaction, recovery never sees a data write without its log entry. Dynamic cache subscriptions are not durable across restart. Cached data may remain on disk, but subscription freshness is invalid until renewed.

## Required local invariants

1. A local write is not considered durable until `wavesdb` confirms the commit.
2. A mutation is not globally streamable until its mutation-log entry is durable — guaranteed by committing it in the same transaction as the data write.
3. Latest pointers must be derivable from versioned records and mutation logs.
4. Vector indexes and graph secondary indexes are derived and rebuildable.
5. Cache metadata is advisory and can be discarded.
