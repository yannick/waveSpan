---
title: Backup & Restore
section: Operations
order: 16
summary: Consistent point-in-time cluster snapshots to S3 (logical and/or physical), incremental chains, restore and clone into a fresh cluster, and the Backups admin tab.
---

# Backup & Restore

WaveSpan takes a **consistent point-in-time snapshot of the whole cluster** and writes it to
object storage (S3-compatible; a local filesystem fallback is used in dev). A snapshot can be
**restored** into the same cluster (disaster recovery) or **cloned** into a brand-new one. The
protocol is coordinated through the collections meta-shard, so a single admin call fans the work
out to every live node and commits one authoritative manifest.

Every backup chooses along four axes — **what** (selection), **how** (planes), **where**
(destination), and **full vs incremental** (type). They are independent; the defaults (full,
logical, default destination) are the common case.

## Selection — WHAT to back up

- **Full** (the default): omit the selection and everything is captured.
- **Partial**: narrow to any combination of `namespaces` (KV), `graphs`, and `vectorCollections`.
  Use this to extract a subset — e.g. one namespace to seed a staging cluster.

System/cluster configuration (`CFSys`) is **always** included regardless of selection, so a
restore always has the identity/config it needs.

## Planes — HOW it is captured

You can run either plane, or both:

- **Logical** — record-level `(column-family, key, value, version)` streams, one object per
  column family per node. Portable and **re-shardable**: it can be restored into a cluster with a
  *different* collections shard count. Logical backups are **full-only** and carry the
  cluster-wide consistency cut (below).
- **Physical** — a per-node SSTable checkpoint of the storage engine. Same-shape (restores to a
  cluster of the same topology) and supports **incrementals** (upload only the SSTables that
  changed since a parent).

Logical is the portable/clone path; physical is the efficient same-shape DR path. Both together
give you a portable snapshot and a fast physical restore from one run.

## Destination — WHERE it is written

- **Default** — the node's configured store. Set via `WAVESPAN_BACKUP_BUCKET`,
  `WAVESPAN_BACKUP_ENDPOINT`, `WAVESPAN_BACKUP_REGION`, `WAVESPAN_BACKUP_PREFIX`,
  `WAVESPAN_BACKUP_USE_SSL`, `WAVESPAN_BACKUP_USE_PATH_STYLE`, with credentials in
  `WAVESPAN_BACKUP_ACCESS_KEY` / `WAVESPAN_BACKUP_SECRET_KEY`. With no bucket configured, a local
  filesystem store under the storage path is used (dev).
- **Named** — an operator-pre-registered alternate, selected by name. Declared in the config YAML
  under `backup.namedDestinations` (each entry has `name`, `bucket`, `prefix`, `region`,
  `endpoint`, `useSSL`, `usePathStyle`, and `accessKeyEnv` / `secretKeyEnv` naming the env vars
  that hold its credentials). Named destinations work in **named-only mode** (below) — they carry
  their own credentials and require no inline secrets.
- **Explicit** — an ad-hoc bucket/prefix/region/endpoint supplied with the request, with a
  credential reference *or* transient inline access/secret keys. Inline credentials require
  `allowInlineDestinationCreds` to be enabled (otherwise the node is in **named-only mode** and
  rejects them). Inline credentials travel only with the request over the authenticated data port
  and are **never persisted or logged** — the catalog and manifest store only a non-secret
  descriptor (bucket/prefix/region/endpoint) plus a credential *reference*, never a raw key.

## Type — full vs incremental

- **Full** — standalone; contains everything it needs to restore on its own. Logical backups are
  always full.
- **Incremental** — physical-only. Give a `parent` (a prior backup id); the backup uploads only
  the SSTables absent from the parent. Backups chain `full → incremental → incremental → …`, and a
  restore replays the whole chain in order. Taking an incremental requires the parent to itself
  carry a physical plane.

## Consistency

- **KV (logical)** is sealed to a **cluster-wide HLC frontier `T`**: every node includes only
  records with `Version ≤ T`, so the union across nodes is a single point-in-time instant for KV.
- **Graph and vector** are single-slot (no version history); they are captured at each node's
  export snapshot rather than sealed to `T` — a documented limitation that avoids losing the only
  copy of a just-overwritten value.
- **Collections** (the strongly-consistent tier) are consistent per shard via their Raft
  applied-index, orthogonal to the HLC cut.

## Status & coverage

A committed backup is one of:

- **COMPLETE** — every expected (live) member exported and every collections data shard was hosted
  by an exporting node.
- **PARTIAL** — the live cluster did not cover the full expected keyspace. Gaps are enumerated in
  the manifest: `member:<id>` (an expected member did not export) or `collections-shard:<id>` (a
  data shard had no live host). An **unreachable member is skipped**, degrading the backup to
  PARTIAL rather than failing it — one dead node never blocks a backup.
- **FAILED** — the backup could not be produced: the coordinator's own node failed, or zero
  members exported.

## Lifecycle & GC

- **Retention**: terminal backups carry a retain-until (default ~30 days); the leader-driven sweep
  deletes them (and their objects) once expired.
- **In-progress lease**: a running backup holds a lease; if a coordinator dies and no one resumes
  it, the sweep marks it FAILED (reclaim).
- **Delete**: `DeleteBackup` removes a backup's catalog entry and its objects, **chain-aware** — it
  refuses to delete a backup a live incremental still depends on unless `force` cascades the whole
  chain. Deletion re-resolves the backup's own destination, so an alt-destination backup is deleted
  in its own bucket.
- **Orphan reconciliation**: the sweep removes objects under the cluster prefix that no live
  backup references (failed/partial-export debris), fail-safe (never reaps when the catalog is
  unexpectedly empty, and honours an object-age grace period).

## Restore & clone

Restore is driven at node startup by environment:

- `WAVESPAN_RESTORE_FROM=s3://<bucket>/<backup-id>` — the backup to restore (unset = normal boot).
- `WAVESPAN_RESTORE_INTENT=dr|clone` (default `dr`).
  - **`dr`** — physical, same-shape disaster recovery: each node restores its matching checkpoint
    (matched by node identity) into the same topology.
  - **`clone`** — logical restore into a **fresh cluster** with a new identity; re-shardable via
    `WAVESPAN_RESTORE_SHARDS=<N>` to a different collections shard count.
- One immutable backup can seed **many independent clones** (each gets its own new identity).

> **Note:** a restore resets the meta shard, so the **backup catalog and schedule are not carried
> over** — the S3 backups themselves remain, but you must **re-register backup intents after a
> restore/clone**. This is correct for a clone (it should not inherit the source's schedule); for
> same-cluster DR the catalog is rebuildable by listing the store.

## Using the Backups tab

The **Backups** admin tab drives all of the above via the authenticated admin API:

- **New backup** — a form for selection, planes, type (full/incremental), and destination
  (default / named / explicit). Each option group has a **?** help button.
- **List** — known backups with status, kind (full/incremental), planes, size, destination,
  retain-until, and PARTIAL gap counts.
- **Watch** — live per-node progress (phase, objects, bytes, done) for a running backup, polled
  until it reaches a terminal state.
- **Delete** — chain-aware; prompts to force-cascade when a backup has incremental children.

## Examples

A **full logical + physical** backup to the default destination (the safe everything-snapshot):

```
selection: (none — full)
planes:    logical + physical
type:      full
dest:      default
```

A **partial logical** backup of one namespace to a **named** destination:

```
selection: namespaces = ["orders"]
planes:    logical
type:      full
dest:      named "alt"
```

A **physical incremental** off a prior physical backup:

```
planes: physical
type:   incremental, parent = "bk-20260701-…"
dest:   (same as the parent)
```

A **clone restore** into a fresh 8-shard cluster:

```
WAVESPAN_RESTORE_FROM=s3://wavespan-backups/bk-20260701-…
WAVESPAN_RESTORE_INTENT=clone
WAVESPAN_RESTORE_SHARDS=8
```
