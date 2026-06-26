# 36 — Disk-pressure admission control

Status: implemented. Code in `internal/health` (monitor), `internal/storage` (Statfs wrapper), gate hooks
in `internal/collections` + `internal/kv`, env wiring in `cmd/wavespan-node/main.go`.

Companion hardening to [33 — consensus tier hardening](33_consensus_throughput_results_and_hardening.md).
That doc shut the *load*-driven crash paths (encode-buffer reuse, unbounded propose backlog). This doc
shuts the *disk*-driven one: a node must never crash because its volume filled.

## 1. The failure

A heavy write burst filled the 5Gi PVC. The collections-raft LogDB is **pebble** (an LSM, ADR 0008 /
dragonboat's default LogDB), and pebble **panics** on `no space left on device` when it cannot write a
WAL record or flush an SSTable. That panic fires *below* WaveSpan — it is not a returned error the
collections state machine can catch — so:

1. The leader (and every follower applying its log) panics mid-write. All 3 voters go down.
2. On restart, the volume is **still full**. Replay re-applies the same entries, pebble hits the same
   ENOSPC, and panics again → **crash-loop**. The cluster does not self-heal: it is wedged until an
   operator grows the PVC or deletes data out of band.

This is distinct from everything doc 33 hardened. The state machine's defensive skip-on-corrupt
(`sm_fatal.go`) and the gRPC/proposer admission (`ErrBusy` → `ResourceExhausted`) all act *at or above*
the apply boundary. The ENOSPC panic is *underneath* it, in the LogDB itself, so none of them can catch
it. It is a genuine "never bring it down" gap.

## 2. The mechanism

Stop the log from growing **before** the volume fills. A per-node monitor watches free space on the
storage volume and flips an atomic flag; the write path checks that flag and **sheds new writes at
admission — before the write is proposed OR forwarded** — returning a transient `ResourceExhausted`. No
propose means no new log entry; the log stops growing; pebble's own background compaction reclaims space;
the flag clears; writes resume.

Crucially this is gated at the **consensus/admission layer**, never inside the storage engine. We only
`Statfs` the path. The engine internals (wavesdb/pebble — a separate rewrite) are untouched.

**Gate placement — every node, at the write ENTRY (not just the leader).** Raft is leader-driven, so the
common client write does not land on the shard leader: it hits a non-leader (or a `wavespan-local` spot)
which **forwards** the write to the leader. But a forwarding node is itself a **follower/applier** of the
target shard — the leader's committed entry replicates back to it and grows **its** disk too. So the gate
must reject on the entry node *before it forwards*, regardless of who leads. The gate therefore sits at:

1. **`Collections.proposeCmd`** — the write entry on every node, **before** the route-or-forward decision.
   A node under its own pressure sheds here and never forwards. (PRIMARY gate.)
2. **`Collections.ProposeRaw`** — the *server* side of a forwarded write. A node receiving a forward while
   under its own pressure (e.g. the pressured leader) sheds here too — it is the applier whose volume grows.
3. **`Manager.Propose`** — the leader-local backstop, just before the Raft propose (data + meta shards).

A forwarded write that the leader shed comes back over gRPC as `ResourceExhausted`; the **forwarder maps
that terminal** (it does not retry another peer — backpressure isn't a "wrong leader" signal) to
`ErrDiskPressure`, so the forwarding node's handler re-maps it to `ResourceExhausted` (not `Internal`).

```
client write ─▶ Service ─▶ Collections.proposeCmd ── gate? ──┬─ yes ─▶ ErrDiskPressure (ResourceExhausted)
   (any node)                                                │
                                                             no
                                              ┌──────────────┴───────────────┐
                                       this node leads?                 not leader
                                              │                              │
                                   Manager.Propose ── gate(backstop) ──▶     forwarder.Forward ─▶ peer (leader)
                                              │                              │   ProposeForward ─▶ Collections.ProposeRaw ── gate? ─▶ shed
                                       proposer/SyncPropose                  │
                                              └─▶ pebble LogDB        leader shed ⇒ gRPC ResourceExhausted
                                                                       ⇒ forwarder maps ⇒ ErrDiskPressure ⇒ ResourceExhausted
reads ─────────▶ Service ─▶ Manager.Read  (never consults the gate)
```

### What is shed vs allowed

| Path | Under pressure |
|------|----------------|
| Collections data-shard writes (SAdd/HSet/ZAdd/…, via the proposer) | **shed** (`ErrDiskPressure`) |
| Collections meta-shard writes (range-directory split/merge proposes) | **shed** (they also grow the LogDB) |
| KV tier writes (Put / Delete / PutTo) | **shed** (`kv.ErrDiskPressure`) |
| KV idempotent **replay** of an already-applied write | **allowed** (adds no new bytes; checked before the gate) |
| Reads (collections + KV: SIsMember, Get, Scan, stale/linearizable) | **allowed** |
| Control-plane RPCs that don't propose (AdmitLearner, membership, gossip, leader status) | **allowed** |

The rule: anything that **grows the volume** is shed; everything that only **reads** or frees space is
allowed. Reads are how an operator and the cluster diagnose and recover, so they must never be gated.

The KV tier writes records into the **same wavesdb store** the consensus tier shares
(`internal/kv/coordinator.go` → `recordstore.Store` → `storage.LocalStore`), so a KV burst can fill the
same volume. It is gated too (`Coordinator.WithDiskGate`). See §7 for the one path left out of scope.

## 3. Thresholds + hysteresis

Three watermarks on the **free fraction** (free bytes / capacity), plus an optional absolute byte floor:

- **low watermark** `MinFreePct` (default **8%**): drop below → enter `pressure`, start shedding.
- **resume watermark** `ResumeFreePct` (default **12%**): pressure clears only once free climbs back
  *above* this. The gap between low and resume is the **hysteresis band** — it prevents flapping
  (sheds/un-sheds every poll) when free space hovers right at the low line. `withDefaults` forces
  `resume > low` (it bumps resume to `low + 4` if mis-set).
- **critical watermark** `CriticalFreePct` (default **3%**): below → `critical`. Behaviour for the write
  path is identical to `pressure` (shed); the distinct level exists for **alerting** and so operators can
  see how close the node came to ENOSPC. `withDefaults` forces `critical < low`.
- **byte floor** `MinFreeBytes` (default **0 = off**): OR-ed with the percentage. On a large volume 8%
  may still be many GB; the floor lets you say "also shed below 2GiB free" regardless of percentage.

State machine (in `Monitor.transition`), driven by each `Sample()`:

```
none  ── free < critical ───────────▶ critical
none  ── free < low ────────────────▶ pressure
pressure ── free < critical ────────▶ critical
pressure ── free ≥ resume ──────────▶ none      (hysteresis: NOT at free ≥ low)
pressure ── otherwise ──────────────▶ pressure
critical ── free < critical ────────▶ critical
critical ── free ≥ resume ──────────▶ none
critical ── otherwise ──────────────▶ pressure  (recovered past critical, still in the shed band)
```

A **Statfs error** (transient stat failure) does **not** engage the shed and keeps the previous level —
we never block writes on a free-space number we could not read (that would self-DoS). On platforms with
no Statfs syscall the wrapper reports zero capacity, which `FreeFraction` reads as `1.0` (no pressure):
the monitor **fails open**.

## 4. Recovery

Once writes are shed the LogDB stops appending. pebble's background compaction merges SSTables and drops
obsolete/overwritten keys and applied-and-snapshotted log entries, **reclaiming space**. The monitor's
next `Sample()` sees free climb past the resume watermark and clears the flag; the write path admits
again. No restart, no operator action — the node sheds, waits, and resumes on its own. If the working set
genuinely exceeds the volume, writes stay shed (correctly) until the operator grows the PVC; the node
keeps **serving reads** and stays up the whole time instead of crash-looping.

## 5. Scope + the known follower-disk-full limitation

This admission control is **LOCAL / per-node**. Each node gates its **own** writes based on its **own**
volume. That fully covers the reported incident (a write burst filling every voter symmetrically) and
any node whose own disk is the bottleneck.

**Known limitation — a follower whose disk fills before its leader's.** Raft replication is leader-driven:
the leader proposes entries and replicates them to followers, which **apply them as they arrive** — a
follower does not get to refuse an entry the leader already committed. So if one follower's volume is
smaller / fuller than the leader's, the leader (seeing its *own* disk healthy) keeps proposing, and the
lagging follower's pebble can still hit ENOSPC on apply and panic. Our local gate on that follower stops
*client* writes it would coordinate, but it cannot stop the **leader's replication stream**.

Why we accept this for now:

- It requires **asymmetric** volumes/fill across the Raft group. In the standard deployment every voter
  is an identical StatefulSet replica with an identically-sized PVC, so they fill together and the local
  gate on the leader (and on each voter as a would-be coordinator) catches it before any single disk is
  critical. The watermarks give margin (8% free) for small per-node skew.
- A single follower crash-looping is **survivable**: the shard keeps quorum on the other two voters and
  serves; the wedged follower is a degraded replica, not a cluster outage. The catastrophic case doc 33
  + this doc target is *all* voters down, which the symmetric local gate prevents.

A proper fix (out of scope here, noted for a future doc) is **propagating disk pressure into Raft**: a
follower under pressure signals the leader (e.g. via a health-gossiped flag or a reject-with-backpressure
on the replication RPC) so the leader **throttles proposes** cluster-wide to the slowest healthy disk,
or transfers leadership / removes the unhealthy replica. That couples admission to the consensus layer
and is a larger change; the local gate is the correct, shippable first layer.

## 6. Tunables (env)

Wired in `cmd/wavespan-node/main.go`, pointed at `cfg.Storage.Path` (the volume holding
`collections-raft/` + the wavesdb store). All optional; unset → default.

| Env var | Default | Meaning |
|---------|---------|---------|
| `WAVESPAN_DISK_MIN_FREE_PCT` | `8` | low watermark — shed below this free % |
| `WAVESPAN_DISK_RESUME_FREE_PCT` | `12` | high watermark — resume above this free % (hysteresis) |
| `WAVESPAN_DISK_MIN_FREE_BYTES` | `0` (off) | absolute free-byte floor, OR-ed with the % |
| `WAVESPAN_DISK_CRITICAL_FREE_PCT` | `3` | critical watermark (alerting; same shed behaviour) |
| `WAVESPAN_DISK_CHECK_INTERVAL` | `5s` | Go-duration poll interval |

## 7. Metrics

Registered by `health.NewPromMetrics` against the process registry:

- `wavespan_disk_pressure` (gauge): `0` none, `1` pressure (writes shed), `2` critical. Alert on `>= 1`
  sustained, page on `2`.
- `wavespan_disk_pressure_shed_writes_total` (counter): writes shed at admission due to disk pressure
  (incremented from both the collections gate and the KV gate).

## 8. Out of scope / notes

- **Storage engine internals untouched.** We only `Statfs` the path and gate at admission. The
  wavesdb/pebble rewrite is a separate effort; making pebble *return* ENOSPC instead of panicking would
  be a defense-in-depth complement but is not this change.
- **Global replication ingest** (`internal/replication/global`) applies peer-cluster mutations into the
  same store and can grow the volume. It is **not** gated here: dropping cross-cluster replication on
  local pressure would silently diverge clusters (a correctness hazard worse than a degraded node), so it
  is deliberately left to the global-replication backpressure design. Noted as future work alongside the
  follower-disk fix in §5.
- The monitor is a single goroutine polling every ~5s; the hot write path only does an **atomic load**
  (`Monitor.UnderPressure`), so gating adds no lock and no syscall to per-write latency.
