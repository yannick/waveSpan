# Data Workbench — unified explorer + type-aware visual editor

- **Date:** 2026-06-26
- **Status:** Approved design (v1 = Phases 1 + 2)
- **Owner:** UI / observability
- **Branch:** `feature/data-workbench`

## 1. Problem

waveSpan's embedded SPA exposes its data through one tab per data model — Data
Browser (KV), Collections, Node Explorer (graph), Cypher Console — plus a
separate KV Writer for writes. Exploring data is expensive:

- **Datatype silos.** No single "what's in this namespace / cluster?" view; you
  tab-hop between models.
- **Opaque values.** KV and collection values are decoded as raw UTF-8. JSON,
  numbers, and binary all render identically — no pretty-print, no hex, no
  vector rendering.
- **Read ≠ write.** Editing a key means leaving the browser for KV Writer, whose
  state resets. No edit-in-place.
- **Key-only discovery.** You scan by key prefix; you can't preview a value
  without committing to a column, and can't search value content.
- **No structure awareness.** Graph properties and vector metadata are typed on
  the server (a real `Value` union) but the UI flattens everything to strings.

## 2. Goals / Non-goals

**Goals**

- Make data exploration economical: one place to browse every model, type-aware
  previews, cheap incremental fetching, keyboard-first navigation.
- Provide a visual, type-aware editor for each datatype (KV, collections,
  vector, graph), usable in-place from wherever you're browsing.
- Reuse the existing component kit, theme tokens, transport, and admin
  write/delete RPCs; follow existing UI patterns.

**Non-goals (this spec)**

- Replacing the operate-oriented tabs (Metrics, Topology, Gossip, Config, Docs).
- Authn/z changes — the workbench rides the existing admin-port authorization.
- Value-content search, bulk actions, blob/image previews (Phase 3, deferred).
- A schema registry. Values stay opaque bytes server-side; the UI infers/edits
  with an explicit, overridable codec.

**Audience:** general workbench serving dev, operator, and demo use equally. The
design favors a common core with light mode-specific affordances (e.g. scope
selector for ops, polish for demo).

## 3. Approach (chosen: C — unified workspace + shared drawer)

Three approaches were weighed:

- **A — Unified Data workspace.** One new tab with a source tree + results pane;
  replaces per-type browsing. Great discovery, biggest change.
- **B — Upgrade tabs in place.** Add a smart value viewer + inline edit to each
  existing tab. Lowest risk, but silos remain and effort duplicates per tab.
- **C — Unified workspace + shared Inspector/Editor drawer (chosen).** The
  unified workspace from A *plus* one reusable, type-aware Inspector/Editor
  drawer that any view opens (Data rows, Cypher results, 3D nodes). Browsing is
  unified and editing is consistent everywhere; the drawer is the reusable core.

C was chosen because it directly satisfies both asks — the workspace makes
exploration economical, and the shared drawer is the "visual editor for the
different datatypes," usable from anywhere.

## 4. Components

### 4.1 Unified Data workspace (`ui/src/views/DataWorkbench.tsx`, new)

A new **Data** tab with three regions:

- **Left source tree.** Browsable, filterable list grouped by model:
  - KV namespaces (from the gossiped namespace list + live key counts already
    surfaced via `GetClusterView`/`kv_*` gauges).
  - Collections (set / hash / zset) by namespace + name.
  - Graphs (by `graph_id`).
  - Vector collections (name + dimensions).
  Selecting a source loads it in the center pane. Counts shown inline where
  cheaply available.
- **Center results pane.** Adapts to the selected source's model:
  - KV: virtualized table — `key | type badge | value preview | version · TTL`.
    The **value preview** is produced by a shared codec sniffer (§4.3).
  - Collections: member / field+value / member+score rows per sub-type.
  - Graph: node/edge rows (with a "open in 3D" affordance to Node Explorer).
  - Vector: vector list with a value sparkline + a "find similar" entry point.
  A top filter bar holds a key-prefix box (value-content search is Phase 3) and a
  **New** button (opens the drawer in create mode).
- **Right Inspector/Editor drawer** (§4.2), opened on row activation.

Exploration economy:

- **Cursor paging + virtualized scroll** instead of a fixed server limit — fetch
  only what's shown, keep scrolling to load more. Built on the existing streamed
  `InspectLocal`/scan APIs.
- **Type-aware preview column** so a row is readable without opening it.
- **Keyboard-first**: `j`/`k` move, `⏎` inspect, `e` edit; scope (node / cluster
  / global), selected source, and selected row persist in the URL (reuse
  `router.tsx`) so a link restores the exact view.

The legacy Data Browser and Collections tabs remain available and unchanged
until Phase 3 retires them; the workbench is additive.

### 4.2 Inspector / Editor drawer (`ui/src/components/InspectorDrawer/`, new)

One component, type-aware body. Shared **header** (identity, type badge,
version, TTL, holder count, scope) and **footer** (Delete / Save). Bodies:

- **KV body.** A "view / edit as" segmented control: **JSON** (structured tree
  editor + raw-text toggle; invalid JSON blocks save), **Text**, **Number**,
  **Hex**. Codec is auto-detected (§4.3) but user-overridable so binary is never
  silently mangled. Shows byte size.
- **Graph body.** Labels as removable chips; properties as **typed rows** (a
  `Value`-union type picker), validated. Edges show start → end + type. The
  picker maps to the proto `Value` oneof field names: `string_value` /
  `int_value` / `double_value` / `bool_value` / `bytes_value` / `list_value`
  (`ValueList`) / `map_value` (`ValueMap`), plus the explicit `null` variant.
- **Vector body.** Dimensions + dtype; the vector rendered as a sparkline / stats
  (read-mostly; raw array editable via paste); metadata as typed rows; payload as
  hex. A **find similar** (k-NN) action.
- **Collection body.** set = member list with add/remove; hash = field→value
  rows; zset = member·score rows (sortable). Shows the consensus-tier badge.

The drawer is consumed by the workbench in v1, and additionally by the Cypher
Console results and the 3D Node Explorer in Phase 2.

### 4.3 Value codec module (`ui/src/lib/valuecodec.ts`, new)

Pure, tested functions shared by the preview column and the KV/collection
editors:

- `sniff(bytes): "json" | "number" | "text" | "binary"` — cheap heuristic
  (valid-UTF-8 → JSON.parse attempt → numeric → printable-ratio → binary).
- `preview(bytes, codec, maxLen): string` — compact, truncated preview.
- `decode(bytes, codec)` / `encode(value, codec): bytes` — round-trip for the
  chosen codec; `encode` validates (e.g. JSON parse, number range, hex parity)
  and surfaces errors the editor shows inline.

Keeping this isolated and pure makes it independently testable and reusable
across views.

### 4.4 Editing & safety model (all types)

- **Save shows a diff** (old → new, decoded per codec) and an explicit confirm.
- **Coordinator + TTL pickers** on write (reuse the KV Writer coordinator
  selector).
- KV / collection / vector writes reuse existing RPCs (`AdminPut`, collection
  ops, vector put); deletes reuse `AdminDelete` with confirm.
- All writes are admin-port gated (unchanged authorization).

### 4.5 Graph write RPC (`AdminPutGraph`, new — Phase 2)

Graph nodes/edges are the only model without a direct admin write today (they go
through Cypher). Add an `AdminPutGraph` RPC on the ObservabilityService,
mirroring `AdminPut`/`AdminDelete`: upsert/delete a node or edge with labels +
typed properties, coordinator-aware. Proto addition + handler in
`internal/observability`, wired in `cmd/wavespan-node/main.go`. This keeps graph
editing symmetric with the other models rather than string-building Cypher.

## 5. Phasing

- **Phase 1 — Core workbench.** Drawer component; KV / collections / vector
  bodies (view + edit); unified workspace shell with type-aware preview table +
  cursor paging; codec module; save-diff-confirm; graph body **view-only**.
- **Phase 2 — Graph editing + reach.** `AdminPutGraph` RPC + editable graph body;
  open the drawer from Cypher results and the 3D Node Explorer; vector "find
  similar."
- **Phase 3 (deferred, out of v1 scope).** Retire/merge legacy Data Browser +
  Collections tabs; value-content search; bulk actions; blob/image previews.

**v1 = Phases 1 + 2.**

## 6. Affected / new files (indicative)

- New: `ui/src/views/DataWorkbench.tsx`,
  `ui/src/components/InspectorDrawer/*`, `ui/src/lib/valuecodec.ts` (+ tests).
- Modified: `ui/src/App.tsx` / router (add the Data tab); `ui/src/transport.ts`
  if new client needed; `ui/src/views/CypherConsole.tsx` &
  `ui/src/views/NodeExplorer.tsx` (open drawer — Phase 2).
- Backend (Phase 2): `proto/wavespan/v1/observability.proto` (+ regen),
  `internal/observability/inspect_write.go` (handler),
  `cmd/wavespan-node/main.go` (wiring).

## 7. Testing strategy

- **Unit (Vitest or existing TS test setup):** `valuecodec` sniff/encode/decode
  round-trips incl. JSON/number/binary edge cases and invalid-input errors.
- **Backend (Go):** `AdminPutGraph` handler — upsert + delete node/edge,
  coordinator resolution, error paths (mirrors existing AdminPut/Delete tests).
- **Build gates:** `tsc --noEmit` + `vite build` for the SPA; `go build/vet/test`
  for backend; lint clean.
- **Manual:** drawer open/edit/save/diff across each datatype against a local
  node with the sample dataset.

## 8. Open questions / risks

- **Vector value editing** is awkward by hand (high-dim float arrays); v1 treats
  the vector as read-mostly (paste-to-replace) and focuses edit on metadata.
- **Cursor paging** depends on stable scan ordering from the inspect APIs;
  confirm the scan cursor semantics during planning.
- **Counts in the source tree** should reuse already-gossiped numbers; avoid
  per-source scans just to show a count.
