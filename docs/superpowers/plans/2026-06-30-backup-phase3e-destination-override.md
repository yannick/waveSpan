# Backup Phase 3e — destination override — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** A backup can be written to a destination **other than** the config/env bucket — the default, a pre-registered **named** alternate, or an **explicit** ad-hoc bucket/prefix/region/endpoint+credential — without ever persisting or logging raw credentials.

**Architecture:** `BackupSpec.destination` (3a proto) selects the object-store target. The coordinator **resolves** the destination at `Begin` (named refs → server-side config; inline creds → held transiently), persists only a non-secret **descriptor + credential reference** in the `BackupIntent`/`cluster.manifest`, and ships the resolved (possibly transient-cred) destination to each node's agent via `PrepareBackup` over the authenticated inter-node channel. Each node constructs an `objstore.S3` for that destination. Admin-auth gates the whole flow.

**Tech Stack:** Go; `objstore.NewS3(S3Config{...})`; 3a `BackupSpec`/`Destination`/coordinator/agent; config for named destinations. Spec §1.1.

## Where to work
`waveSpan-backup` / `backup`. Depends on **3a**. Tests: `go test ./internal/backup/... ./internal/config/...`.

## Confirmed API / facts (grounded)
- `objstore.NewS3(objstore.S3Config{Endpoint, Bucket, Prefix, AccessKey, SecretKey, Region, UseSSL, UsePathStyle}) (*S3, error)` — static SigV4 creds; `Endpoint` host:port no scheme; creates the bucket if missing (15s round-trip at construction). `*S3` satisfies `backup.ObjectStore`.
- 3a already has `Destination` proto + the coordinator resolving a default destination from config. The node agent already constructs/receives an object store.
- Admin port enforces identity via `adminIdentity.EnforceHTTP`; `BeginBackup` is an admin RPC.

## File structure
- `internal/config/config.go` (extend) — `Backup.DefaultDestination` + `Backup.NamedDestinations []NamedDestination{Name, Bucket, Prefix, Region, Endpoint, UseSSL, UsePathStyle, CredentialRef}` (creds resolved from a secret ref / env, never inline in config plaintext where avoidable).
- `internal/backup/destination.go` — `ResolveDestination(cfg, spec.Destination) (objstore.S3Config + transient creds, descriptor, error)`; descriptor = the non-secret fields + cred reference for persistence.
- `internal/backup/coordinator.go` (extend) — resolve at Begin; persist descriptor only; pass resolved config to agents.
- `internal/backup/agent.go` (extend) — build the object store from the received resolved destination.

---

## Task 1: Config — default + named destinations
- [ ] **Failing test** (`config_dest_test.go`): config with a default destination + two named destinations parses into `cfg.Backup`; a named lookup by name returns its descriptor + resolves its credential reference (from env/secret); an unknown name errors.
- [ ] **Implement** the config structs + env overrides (mirror existing `WAVESPAN_*` parsing); credential reference resolves to creds via a secret/env indirection, never stored as plaintext in the parsed struct beyond what config already holds.
- [ ] Run → PASS. Commit `feat(config): backup default + named alternate destinations`.

## Task 2: Destination resolution (default | named | explicit) + secret handling
- [ ] **Failing test** (`destination_test.go`): `ResolveDestination` with: (a) empty `Destination` → default config destination; (b) `Destination{name:"alt"}` → the named config destination (no secrets in the input); (c) explicit `Destination{bucket,prefix,region,endpoint, credential:{inline:{accessKey,secretKey}}}` → an `S3Config` with those creds AND a **descriptor that contains NO secrets** (only bucket/prefix/region/endpoint + a credential-kind marker). Assert the returned descriptor (the thing persisted) never contains the raw secret.
- [ ] **Implement** `destination.go`: resolve to `(s3cfg objstore.S3Config, descriptor DestinationDescriptor, error)`. Inline creds are carried in `s3cfg` (transient, for this run) but excluded from `descriptor`. A policy flag (`cfg.Backup.AllowInlineDestinationCreds`, default per deployment) can reject inline creds (named-only mode) — test that path.
- [ ] Run → PASS. Commit `feat(backup): resolve backup destination (default/named/explicit); secrets excluded from descriptor`.

## Task 3: Coordinator + agent thread the destination; intent/manifest store descriptor only
- [ ] **Failing test** (fake multi-node): `BeginBackup({destination: explicit alt bucket})` → objects land in the **alt** FS object store (not the default); the persisted `BackupIntent` + `cluster.manifest` record the destination **descriptor** (bucket/prefix/endpoint) but **no raw creds**; a log capture shows creds never logged.
- [ ] **Implement:** coordinator calls `ResolveDestination` at Begin, persists the descriptor in the intent, and passes the resolved `S3Config` (incl. transient creds) to each node via `PrepareBackup` (over the authenticated inter-node gRPC). Each agent does `objstore.NewS3(received)` and exports there. `DeleteBackup`/GC/retention (3d) operate on the recorded descriptor (re-resolving creds from the named ref / re-prompting is out of scope — for explicit-inline destinations, deletion requires the creds supplied again or a named ref; document this).
- [ ] Run → PASS. `go build ./... && go vet ./...`. Commit `feat(backup): backups to alternate destinations (descriptor-only persistence, transient creds)`.

## Done criteria (3e)
- [ ] A backup can target default / named / explicit destinations; objects land in the chosen bucket; the intent/manifest/logs never contain raw credentials; named-only policy enforceable.
- [ ] `go test ./internal/backup/... ./internal/config/...` green; vet+build clean.

## Open (for the plan/3d interaction)
- Deleting an **explicit-inline-credential** backup later needs the creds re-supplied (they weren't persisted) or the destination promoted to a named ref. Named/default destinations have no such issue. Decide the UX (the UI's delete for an inline-cred backup may require re-entering creds) — note for 3f.
