# Backup Phase 3f — admin Backup UI — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** A "Backups" page in the embedded admin SPA: see which backups exist, watch an in-progress backup's live per-node progress, trigger a backup (including to an alternate destination), and delete one — all via the `BackupService` Connect endpoint.

**Architecture:** A new React view in `ui/src/views/`, registered as a tab in `App.tsx`, using a generated Connect-ES client (`buf generate` → `ui/src/gen/.../backup_pb.ts`) wired into `transport.ts`. It polls `BackupStatus` while a backup is `RUNNING` (the `setInterval(load, 3000)` pattern). The backend handler is the `BackupService` Connect mount from 3a (already on the admin port behind `EnforceHTTP`).

**Tech Stack:** React 18 + TypeScript + Vite 6 + Connect-ES v2 (`@connectrpc/connect`); `buf generate`; `make ui`. Template: `ui/src/views/CollectionsExplorer.tsx`. Spec §11.

## Where to work
`waveSpan-backup` / `backup`, dir `ui/`. Depends on **3a** (BackupService RPCs: BeginBackup/BackupStatus/ListBackups/DeleteBackup) and benefits from **3d** (DeleteBackup) and **3e** (destination in the trigger form). Build/test: `cd ui && npm ci && npm run gen && npm run build && npm run typecheck && npm test` (vitest); `make ui` embeds it.

## Confirmed facts (grounded)
- UI: React+Vite+Connect-ES; clients are singletons in `ui/src/transport.ts` (`createClient(Service, transport)`, `baseUrl: window.location.origin`, `credentials: "same-origin"`). `buf generate` (root `buf.gen.yaml` TS target → `ui/src/gen/wavespan/v1/<svc>_pb.ts`) requires `cd ui && npm ci` first.
- Template `ui/src/views/CollectionsExplorer.tsx`: a `run(fn)` wrapper for busy/err/msg; list/fetch + mutation calls; `TierPanel` shows the canonical poll (`useEffect` + `setInterval(load, 3000)` + `live` guard + `clearInterval`).
- Nav: `App.tsx` — add to the `Tab` union, the `tabs` array, and the `<main>` switch; hash router in `router.tsx`.
- Backend mount already exists from 3a (`collectionsSvc.BackupHandler()` at `bckPath`).

## File structure
- `proto/wavespan/v1/backup.proto` — already exists (3a); `make proto` regenerates the TS client.
- `ui/src/transport.ts` (modify) — add `export const backup = createClient(BackupService, transport);`.
- `ui/src/views/Backups.tsx` (create) — the page.
- `ui/src/App.tsx` (modify) — register the `"backups"` tab.

---

## Task 1: Generate the TS client + wire the transport
- [ ] **Step 1:** `cd ui && npm ci` then `npm run gen` (or `make proto`) → confirm `ui/src/gen/wavespan/v1/backup_pb.ts` appears with `BackupService`.
- [ ] **Step 2:** add `import { BackupService } from "./gen/wavespan/v1/backup_pb";` + `export const backup = createClient(BackupService, transport);` to `transport.ts`. `npm run typecheck` passes.
- [ ] **Step 3:** commit `feat(ui): generate BackupService client + wire transport`.

## Task 2: Backups list view
- [ ] **Step 1 (test, vitest):** a component test renders rows from a mocked `backup.listBackups()` (id, status, planes, full/incremental+parent, started/finished, size, destination, retainUntil); a `PARTIAL` row shows its gaps.
- [ ] **Step 2:** create `Backups.tsx` with a list section calling `backup.listBackups({})` via the `run()` wrapper (mirror CollectionsExplorer); render the table. Register the `"backups"` tab in `App.tsx` (`Tab` union + `tabs` + `<main>` switch line `{tab === "backups" && <Backups />}`).
- [ ] **Step 3:** `npm test` + `npm run typecheck` pass; commit `feat(ui): Backups list view`.

## Task 3: Live progress for a RUNNING backup
- [ ] **Step 1 (test):** with a mocked `backup.backupStatus()` returning `RUNNING` then `COMPLETE`, the progress panel renders phase + overall % + per-node rows, and **stops polling** once status leaves `RUNNING`.
- [ ] **Step 2:** add a progress panel using the `useEffect`+`setInterval(load, 3000)`+`live`+`clearInterval` pattern (from `TierPanel`); poll `backup.backupStatus({backupId})`; render `BackupState.phase`, `overallPct`, and `perNode[]{memberId, phase, objects, bytes, done}`; clear the interval when status is terminal.
- [ ] **Step 3:** tests pass; commit `feat(ui): live backup progress (poll BackupStatus, per-node breakdown)`.

## Task 4: Trigger form (selection / planes / destination) + delete
- [ ] **Step 1 (test):** the trigger form builds a `BeginBackupRequest` from inputs — selection (full | namespaces/graphs/collections), planes (logical/physical/both), full-vs-incremental (parent dropdown from the list), and destination (default | named-dropdown | explicit bucket/prefix/region/endpoint+credential); submitting calls `backup.beginBackup(spec)` and switches to the new backup's progress view. A delete button calls `backup.deleteBackup({backupId, force})` and warns when a backup has incremental children (force).
- [ ] **Step 2:** implement the form + submit + delete via `run()`; show the returned `backupId`; for an explicit-destination with inline creds, keep the secret only in the request (never stored in UI state beyond submit). Disable explicit-creds inputs if the server reports named-only policy (3e).
- [ ] **Step 3:** tests + typecheck pass; commit `feat(ui): trigger backup (selection/planes/destination) + delete`.

## Task 5: Build + embed
- [ ] **Step 1:** `make ui` (= `cd ui && npm ci && npm run build` → `internal/ui/dist`); `make build` (embeds the SPA); `go build ./...` green.
- [ ] **Step 2:** commit `feat(ui): build embedded Backups view`.

## Done criteria (3f)
- [ ] The admin SPA has a "Backups" tab: lists backups, shows live per-node progress for a RUNNING backup (polling, stops when terminal), triggers a backup (with destination override), and deletes (chain-aware warning). All via the `BackupService` Connect endpoint (admin-auth enforced); creds never rendered in list/status responses.
- [ ] `npm test` + `npm run typecheck` green; `make ui` + `go build ./...` green.
