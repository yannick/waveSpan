# Data Workbench Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the unified Data workspace + a reusable type-aware Inspector/Editor drawer (approach C), v1 = Phases 1+2, and run a node so it can be tested.

**Architecture:** A new React **Data** tab (`DataWorkbench`) with a source tree (KV namespaces / collections / graphs / vector collections), a type-aware results table, and a shared `InspectorDrawer` opened on row activation. A pure `valuecodec` module sniffs/decodes/encodes opaque bytes (json/number/text/hex). Writes reuse `AdminPut`/`AdminDelete` + collection/vector ops; a new `AdminPutGraph` RPC makes graph edits symmetric. Spec: `docs/superpowers/specs/2026-06-26-data-workbench-design.md`.

**Tech Stack:** Go (connectrpc, protobuf, prometheus), React + TypeScript + Vite, the existing component kit + theme tokens.

---

## File structure

- `ui/src/lib/valuecodec.ts` (+ `valuecodec.test.ts`) — pure codec/sniff/preview.
- `ui/src/components/InspectorDrawer/` — `index.tsx` (shell), `KvBody.tsx`, `CollectionsBody.tsx`, `VectorBody.tsx`, `GraphBody.tsx`, `JsonTree.tsx`, `TypedRows.tsx`, `DiffConfirm.tsx`.
- `ui/src/views/DataWorkbench.tsx` — the new tab (tree + results + drawer host).
- `ui/src/App.tsx` / `ui/src/router.tsx` — register the Data tab.
- `ui/src/transport.ts` — add a vector client if needed (else route find-similar via cypher).
- `proto/wavespan/v1/observability.proto` — `AdminPutGraph` RPC + messages (regen).
- `internal/observability/inspect_write.go` — `AdminPutGraph` handler (+ test in `inspect_graphwrite_test.go`).
- `cmd/wavespan-node/main.go` — wire graph writer into the obs service.

---

## Backend (independent — can run in parallel with the codec lib)

### Task 1: AdminPutGraph proto + regen
**Files:** Modify `proto/wavespan/v1/observability.proto`; regen with `PATH="$(go env GOPATH)/bin:$PATH" make proto` (needs protoc-gen-go v1.36.11).
- [ ] Add `AdminPutGraphRequest { string graph_id; oneof target { NodeRecord node; EdgeRecord edge; } bool delete; string target_member_id; }` and `AdminPutGraphResponse { bool ok; Version version; string error; }` (reuse `cypher.proto` NodeRecord/EdgeRecord/Value).
- [ ] Add `rpc AdminPutGraph(...)` to `ObservabilityService`.
- [ ] Regen; confirm only admin/observability generated files change.

### Task 2: AdminPutGraph handler + wiring + test
**Files:** `internal/observability/inspect_write.go`, `internal/observability/inspect_graphwrite_test.go`, `cmd/wavespan-node/main.go`.
- [ ] Handler: validate graph_id + target; on delete set tombstone; write via the graph store `Batch` (`PutNode`/`PutEdge`) the obs service already holds (`s.graph`); stamp a version via `newGraphVersion`. Return ok/version/error in the body (mirror AdminPut error style).
- [ ] Confirm `s.graph` (graph.Store) is set; if a coordinator-forward is needed, mirror `resolveTarget` but local-apply is acceptable for v1 (note: graph writes are local-store; document the replication caveat in the handler comment).
- [ ] Test: upsert node (labels+props round-trip via AllNodes/GetNode), upsert edge, delete node→tombstone, empty graph_id error.
- [ ] `go build ./... && go test ./internal/observability/ && golangci-lint run ./internal/observability/...`
- [ ] Commit.

## UI foundation (independent)

### Task 3: valuecodec module (TDD)
**Files:** `ui/src/lib/valuecodec.ts`, `ui/src/lib/valuecodec.test.ts`.
- [ ] `sniff(bytes): "json"|"number"|"text"|"binary"`, `preview(bytes,codec,max)`, `decode(bytes,codec)`, `encode(value,codec): Uint8Array` (validates; throws on bad json/number/hex).
- [ ] Tests: json object/array, integer/float, utf-8 text, binary (non-utf8) → hex; round-trips; invalid inputs throw. Run with the UI test runner (`cd ui && npx vitest run` — add vitest dev dep + script if absent; else a tiny node test).
- [ ] Commit.

## UI components & view (sequential, on top of 1–3)

### Task 4: InspectorDrawer shell + KvBody
**Files:** `ui/src/components/InspectorDrawer/{index,KvBody,JsonTree,DiffConfirm}.tsx`.
- [ ] Shell: header (identity/type badge/version/TTL/holders/scope), body slot, footer (Delete/Save). Props: record descriptor + onClose + onSaved.
- [ ] KvBody: codec segmented control (JSON/Text/Number/Hex, default = `sniff`), JsonTree editor + raw toggle (invalid blocks save), coordinator + TTL pickers, Save→DiffConfirm→`obs.adminPut`, Delete→confirm→`obs.adminDelete`.
- [ ] `tsc --noEmit`; commit.

### Task 5: CollectionsBody + VectorBody + GraphBody
**Files:** `ui/src/components/InspectorDrawer/{CollectionsBody,VectorBody,GraphBody,TypedRows}.tsx`.
- [ ] CollectionsBody: set/hash/zset list editors via existing `collections` client ops.
- [ ] VectorBody: dims/dtype, value sparkline (reuse Sparkline), metadata via TypedRows; find-similar via cypher `vector.*` (best-effort; degrade gracefully if unavailable).
- [ ] GraphBody: labels chips + TypedRows (proto Value field names), Save→`obs.adminPutGraph`.
- [ ] TypedRows: typed key/value rows mapping to the Value oneof (`string_value`/`int_value`/`double_value`/`bool_value`/`bytes_value`/`list_value`/`map_value` + `null`).
- [ ] `tsc --noEmit`; commit.

### Task 6: DataWorkbench view + nav
**Files:** `ui/src/views/DataWorkbench.tsx`, `ui/src/App.tsx`, `ui/src/router.tsx`.
- [ ] Source tree: KV namespaces (from `obs.getClusterView().namespaces` + counts), collections, graphs, vector collections.
- [ ] Results pane per source kind with the type-aware preview column (KV via `obs.inspectLocal` streamed, cursor/scroll paging); New + row-click → InspectorDrawer; scope + selection in URL.
- [ ] Register the **Data** tab (first in the data group).
- [ ] `tsc --noEmit`; commit.

### Task 7: Phase-2 reach (drawer from Cypher + 3D)
**Files:** `ui/src/views/CypherConsole.tsx`, `ui/src/views/NodeExplorer.tsx`.
- [ ] Add "inspect" affordance on a Cypher result row / a 3D node → open the shared InspectorDrawer.
- [ ] `tsc --noEmit`; commit.

## Build & run

### Task 8: Build, verify, run a node
- [ ] `cd ui && npm run build` (tsc + vite → `internal/ui/dist`).
- [ ] `go build ./... && go vet ./... && go test ./...`; `golangci-lint run`.
- [ ] Launch a node serving the admin UI (single-node), load the sample dataset, print the URL for the user to test.
- [ ] Report what's fully working vs. simplified.
