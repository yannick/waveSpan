# Global Data Browser Resolution — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Data Browser's **Global** scope resolve a key's holders/version/value across every alive member of this cluster (fixes the single-cluster "different nodes, different results" bug) and across configured peer clusters (active-active), with honest `COMPLETE`/`PARTIAL` completeness.

**Architecture:** `InspectGlobal` orchestrates two layers. **Layer 1** (always) fans a point `FetchReplica` out to every alive member of *this* cluster and merges holders. **Layer 2** (when `include_peer_clusters` and global replication is enabled) calls a new `GlobalReplication.InspectKey` RPC on each configured peer; each peer runs *its own* Layer 1 and returns holders tagged with its `cluster_id`. Single hop, no recursion. The serving node is owned entirely by Layer 1 (no double-count).

**Tech Stack:** Go, gRPC-go (`google.golang.org/grpc`), buf/protoc, Connect (admin/UI port only), React/TS UI. Module path `github.com/yannick/wavespan`.

**Spec:** `docs/superpowers/specs/2026-06-26-global-inspect-design.md`

**Key existing code to mirror:**
- `internal/observability/inspect_local.go` — the proven cluster-wide fan-out + deterministic merge this design specializes to a point lookup.
- `internal/replication/global/sender.go:23-44` — the `GlobalReplicationClient` pool pattern (`rpcopts.GRPCConn` + `wavespanv1.NewGlobalReplicationClient`) that Layer 2's client mirrors.
- `internal/replication/local/connect.go:30-67` — the gRPC `ReplicationServiceClient` pool + `ScanLocal`/`StoreReplica` client methods to mirror for `FetchReplica`.
- `internal/grpcsrv/global.go` — the **mounted** gRPC `GlobalReplication` adapter that must host the new `InspectKey` method (the Connect `global.Server` is not the served handler post-migration).

**Conventions:**
- `membership.Member{ClusterID, MemberID, DataAddr, ...}`; `membership.MemberView{Member, State}`; alive = `membership.StateAlive`.
- `StoredRecord` (common.proto): `version`, `value` (ValueBody, `.GetInline()`), `tombstone`, `expires_at_unix_ms`, `logical_key`.
- `InspectHolder` (observability.proto): `member_id`, `peer_cluster_id`, `version`, `holder_class`, `global_repl_lag_ms`.
- Version compare: `version.FromProto(a).Compare(version.FromProto(b))` (`internal/version`).
- Proto regen: `make proto` (runs `buf generate`; CI installs `protoc-gen-go-grpc`). Run from repo root.
- Unit tests: `go test ./internal/<pkg>/...`. Build: `go build ./...`. Lint: `make lint`.
- Integration tests: `//go:build integration`; run `go test -tags integration -timeout 600s ./tests/integration/...`. Helpers `composeGlobal`, `kvClient(port)`, `membership(t, port)` already exist in the package. Admin ports: test-a = 7951/7952, test-b = 7953/7954.
- Commit message footer (every commit): `Claude-Session: https://claude.ai/code/session_01Ncscy9pK1dtCvYW4pLyhoe`.

---

## File structure

| File | Responsibility |
|------|----------------|
| `proto/wavespan/v1/replication.proto` | + `InspectKey` RPC on `GlobalReplication`; `InspectKeyRequest`/`InspectKeyResponse` messages |
| `internal/replication/local/connect.go` | + `FetchReplica` **client** method on `ConnectReplicator` |
| `internal/holderinspect/resolver.go` (new) | Layer 1: single-key within-cluster point fan-out + deterministic merge; **owns the self-holder** |
| `internal/holderinspect/resolver_test.go` (new) | Layer 1 unit tests (fake fetcher + member source) |
| `internal/replication/global/inspect.go` (new) | Layer 2 client `PeerInspector` (calls peers' `InspectKey`) + the peer-side response builder |
| `internal/replication/global/inspect_test.go` (new) | Layer 2 unit tests (fake `GlobalReplicationClient`) |
| `internal/grpcsrv/global.go` | + `InspectKey` method on the gRPC adapter, delegating to an injected peer-side inspector |
| `internal/observability/obsservice.go` | replace `globalInspector` seam with `clusterResolver` + optional `peerInspector` seams |
| `internal/observability/inspect_global.go` | orchestrate Layer 1 + Layer 2; **drop the stub branch and the pre-inspector self-append** |
| `internal/observability/inspect_global_test.go` (or `inspect_test.go`) | handler unit test: merged holders, value-from-peer, PARTIAL on unreachable |
| `cmd/wavespan-node/main.go` | construct resolver + peer inspector; register peer handler on the gRPC adapter; wire obs seams |
| `ui/src/views/DataBrowser.tsx` | Global mode sends `includePeerClusters: true`; holders column shows `peer_cluster_id`; (value modal already in branch) |
| `tests/integration/global_inspect_test.go` (new) | docker two-cluster cross-cluster test + single-cluster regression |

**Shared tuple** returned by both layers (keep identical so `inspect_global.go` merges uniformly):
`(holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string)`.

---

## Task 1: Proto — add the `InspectKey` RPC

**Files:**
- Modify: `proto/wavespan/v1/replication.proto`
- Regenerate: `proto/wavespan/v1/replication*.pb.go`, `_grpc.pb.go`, connect stubs

- [ ] **Step 1: Add the import + messages + RPC**

In `proto/wavespan/v1/replication.proto`, add near the top imports (it already imports `common.proto`):
```protobuf
import "wavespan/v1/observability.proto";  // for InspectHolder
```
Add messages (place just above `service GlobalReplication`):
```protobuf
// InspectKey asks a peer cluster "which of your nodes hold this key, at what version?" for the
// Global Data Browser (design/26). The peer runs its own within-cluster resolution and returns
// one holder per node that has the key, tagged with the peer's cluster_id. Single hop: the peer
// never recurses into its own peers.
message InspectKeyRequest {
  string namespace = 1;
  bytes key = 2;
  bool include_value = 3;
}
message InspectKeyResponse {
  repeated InspectHolder holders = 1;   // this cluster's holders, peer_cluster_id stamped
  StoredRecord best = 2;                // latest record this cluster holds (unset if none)
  bool complete = 3;                    // every alive member of this cluster answered
  repeated string warnings = 4;
}
```
Add the RPC inside `service GlobalReplication`:
```protobuf
  rpc InspectKey(InspectKeyRequest) returns (InspectKeyResponse);
```

- [ ] **Step 2: Regenerate and verify it compiles**

Run (from repo root): `make proto && go build ./...`
Expected: regenerates `replication.pb.go` + `replication_grpc.pb.go` (now with `InspectKey` on the client/server interfaces) and connect stubs; `go build ./...` succeeds (the `UnimplementedGlobalReplicationServer` embed in `grpcsrv/global.go` keeps it compiling without a hand-written method yet).

- [ ] **Step 3: Commit**
```bash
git add proto/ && git commit -m "proto: add GlobalReplication.InspectKey RPC for cross-cluster Data Browser"
```

---

## Task 2: `FetchReplica` client method

The replication client (`ConnectReplicator`) has `StoreReplica`/`ScanLocal` client methods but **no** `FetchReplica` client. Layer 1 and the peer-side handler need it.

**Files:**
- Modify: `internal/replication/local/connect.go` (after `StoreReplica`, ~line 52)
- Test: `internal/replication/local/connect_test.go` (add a case, or rely on resolver tests — see note)

- [ ] **Step 1: Add the client method**
```go
// FetchReplica asks a holder for its local winning record of a single key (design/05). Used by
// the Global Data Browser to resolve holders within a cluster.
func (r *ConnectReplicator) FetchReplica(ctx context.Context, target membership.Member, namespace string, key []byte) (*wavespanv1.FetchReplicaResponse, error) {
	c, err := r.client(target.DataAddr)
	if err != nil {
		return nil, err
	}
	return c.FetchReplica(ctx, &wavespanv1.FetchReplicaRequest{Namespace: namespace, Key: key})
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./internal/replication/local/...`
Expected: PASS. (This thin pass-through is exercised end-to-end by the integration test; the resolver's unit tests use a fake fetcher, so no dedicated unit test is needed here — adding one would just re-test grpc-generated code.)

- [ ] **Step 3: Commit**
```bash
git add internal/replication/local/connect.go
git commit -m "replication: add FetchReplica client method for global inspect"
```

---

## Task 3: Layer 1 — `holderinspect.ClusterResolver` (TDD)

A focused unit: resolve a single exact key across this cluster's alive members. Lives in its own package so both observability and the peer-side gRPC handler can use it without an import cycle.

**Files:**
- Create: `internal/holderinspect/resolver.go`
- Test: `internal/holderinspect/resolver_test.go`

- [ ] **Step 1: Write the failing test**

`internal/holderinspect/resolver_test.go`:
```go
package holderinspect

import (
	"context"
	"errors"
	"testing"

	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

type fakeFetcher struct {
	// memberID(DataAddr) -> record or error
	recs map[string]*wavespanv1.FetchReplicaResponse
	errs map[string]error
}

func (f *fakeFetcher) FetchReplica(_ context.Context, t membership.Member, _ string, _ []byte) (*wavespanv1.FetchReplicaResponse, error) {
	if e := f.errs[t.DataAddr]; e != nil {
		return nil, e
	}
	if r := f.recs[t.DataAddr]; r != nil {
		return r, nil
	}
	return &wavespanv1.FetchReplicaResponse{Found: false}, nil
}

type fakeMembers struct{ views []membership.MemberView }

func (f *fakeMembers) Members() []membership.MemberView { return f.views }

func ver(ms int64) *wavespanv1.Version { return &wavespanv1.Version{HlcPhysicalMs: ms, HlcLogical: 0} }

func aliveView(id, addr string) membership.MemberView {
	return membership.MemberView{Member: membership.Member{MemberID: id, DataAddr: addr}, State: membership.StateAlive}
}

// self holds v=10; m2 holds v=20 (newer) and answers; m3 is unreachable.
func TestResolveKey_MergesHoldersLatestWinsPartialOnUnreachable(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{
		aliveView("self", "self:1"), aliveView("m2", "m2:1"), aliveView("m3", "m3:1"),
	}}
	fetch := &fakeFetcher{
		recs: map[string]*wavespanv1.FetchReplicaResponse{
			"m2:1": {Found: true, Record: &wavespanv1.StoredRecord{Version: ver(20), Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("newer")}}}},
		},
		errs: map[string]error{"m3:1": errors.New("dial timeout")},
	}
	selfRec := &wavespanv1.StoredRecord{Version: ver(10), Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("old")}}}
	r := New(self, members, fetch, func(ns string, key []byte) (*wavespanv1.StoredRecord, bool, error) { return selfRec, true, nil })

	holders, best, complete, warnings := r.ResolveKey(context.Background(), "ns", []byte("k"), true)

	if complete { t.Fatal("expected PARTIAL: m3 unreachable") }
	if len(warnings) != 1 { t.Fatalf("want 1 warning, got %v", warnings) }
	if len(holders) != 2 { t.Fatalf("want 2 holders (self,m2), got %d", len(holders)) } // m3 didn't answer
	if holders[0].MemberId != "m2" || holders[1].MemberId != "self" {
		// deterministic sort by (peer_cluster_id, member_id): "" peer for all; m2 < self
		t.Fatalf("holders not sorted deterministically: %v", []string{holders[0].MemberId, holders[1].MemberId})
	}
	if best.GetVersion().GetHlcPhysicalMs() != 20 { t.Fatalf("best should be v20, got %v", best.GetVersion()) }
}

func TestResolveKey_CompleteWhenAllAnswer(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{aliveView("self", "self:1"), aliveView("m2", "m2:1")}}
	fetch := &fakeFetcher{recs: map[string]*wavespanv1.FetchReplicaResponse{"m2:1": {Found: true, Record: &wavespanv1.StoredRecord{Version: ver(5)}}}}
	r := New(self, members, fetch, func(ns string, key []byte) (*wavespanv1.StoredRecord, bool, error) {
		return &wavespanv1.StoredRecord{Version: ver(7)}, true, nil
	})
	_, _, complete, warnings := r.ResolveKey(context.Background(), "ns", []byte("k"), false)
	if !complete || len(warnings) != 0 { t.Fatalf("want COMPLETE no warnings, got complete=%v warns=%v", complete, warnings) }
}

// reveal=false must not surface inline values on the holders' best record path.
func TestResolveKey_RedactsWhenNotRevealed(t *testing.T) {
	self := membership.Member{MemberID: "self", DataAddr: "self:1"}
	members := &fakeMembers{views: []membership.MemberView{aliveView("self", "self:1")}}
	r := New(self, members, &fakeFetcher{}, func(ns string, key []byte) (*wavespanv1.StoredRecord, bool, error) {
		return &wavespanv1.StoredRecord{Version: ver(3), Value: &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: []byte("secret")}}}, true, nil
	})
	_, best, _, _ := r.ResolveKey(context.Background(), "ns", []byte("k"), false)
	if v := best.GetValue().GetInline(); len(v) != 0 { t.Fatalf("value must be redacted when reveal=false, got %q", v) }
}
```

- [ ] **Step 2: Run it to confirm it fails to compile (no `New`/`ResolveKey` yet)**

Run: `go test ./internal/holderinspect/...`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Implement `resolver.go`**

`internal/holderinspect/resolver.go`:
```go
// Package holderinspect resolves a single key's holders across the alive members of one cluster.
// It is the Layer 1 building block of the Global Data Browser (design/26): a point fan-out that
// mirrors the cluster-wide InspectLocal merge, specialized to an exact key. It owns the serving
// node's own holder (read from the local record store), so callers must NOT add self separately.
package holderinspect

import (
	"context"
	"fmt"
	"sort"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// MemberSource yields the current membership roster (satisfied by *membership.Service).
type MemberSource interface{ Members() []membership.MemberView }

// ReplicaFetcher fetches one key's local winning record from a member (satisfied by
// *local.ConnectReplicator via its FetchReplica client method).
type ReplicaFetcher interface {
	FetchReplica(ctx context.Context, target membership.Member, namespace string, key []byte) (*wavespanv1.FetchReplicaResponse, error)
}

// LocalRecordFn reads the serving node's own winning record for a key (satisfied by
// recordstore.Store.GetRecord).
type LocalRecordFn func(namespace string, key []byte) (*wavespanv1.StoredRecord, bool, error)

// ClusterResolver resolves a key across this cluster's alive members.
type ClusterResolver struct {
	self    membership.Member
	members MemberSource
	fetch   ReplicaFetcher
	local   LocalRecordFn
}

// New builds a ClusterResolver.
func New(self membership.Member, members MemberSource, fetch ReplicaFetcher, local LocalRecordFn) *ClusterResolver {
	return &ClusterResolver{self: self, members: members, fetch: fetch, local: local}
}

// ResolveKey returns the holders within this cluster, the latest record observed (nil if none),
// whether every alive member answered (complete), and warnings for unreachable members. reveal
// gates whether the returned record carries its inline value. Best-effort: an unreachable member
// flips complete=false and adds a warning, never an error.
func (r *ClusterResolver) ResolveKey(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) {
	complete = true
	consider := func(memberID string, rec *wavespanv1.StoredRecord) {
		holders = append(holders, &wavespanv1.InspectHolder{
			MemberId:    memberID,
			HolderClass: wavespanv1.HolderClass_HOLDER_DURABLE,
			Version:     rec.GetVersion(),
		})
		if best == nil || version.FromProto(rec.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0 {
			best = rec
		}
	}

	// Self via the local store (Layer 1 owns the self-holder).
	if rec, found, err := r.local(ns, key); err == nil && found {
		consider(r.self.MemberID, rec)
	}

	// Other alive members via a point FetchReplica.
	for _, mv := range r.members.Members() {
		if mv.Member.MemberID == r.self.MemberID || mv.State != membership.StateAlive {
			continue
		}
		resp, err := r.fetch.FetchReplica(ctx, mv.Member, ns, key)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("member %s unreachable: %v", mv.Member.MemberID, err))
			complete = false
			continue
		}
		if resp.GetFound() {
			consider(mv.Member.MemberID, resp.GetRecord())
		}
	}

	if best != nil {
		best = redact(best, reveal)
	}
	sortHolders(holders)
	return holders, best, complete, warnings
}

// redact returns rec with its inline value stripped unless reveal is set (and it is not a
// tombstone). It shallow-copies so the caller's store record is never mutated.
func redact(rec *wavespanv1.StoredRecord, reveal bool) *wavespanv1.StoredRecord {
	if reveal && !rec.GetTombstone() {
		return rec
	}
	clone := *rec
	clone.Value = nil
	return &clone
}

// sortHolders orders by (peer_cluster_id, member_id) so identical requests yield identical lists.
func sortHolders(hs []*wavespanv1.InspectHolder) {
	sort.Slice(hs, func(i, j int) bool {
		if hs[i].GetPeerClusterId() != hs[j].GetPeerClusterId() {
			return hs[i].GetPeerClusterId() < hs[j].GetPeerClusterId()
		}
		return hs[i].GetMemberId() < hs[j].GetMemberId()
	})
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `go test ./internal/holderinspect/...`
Expected: PASS (3 tests). If `ValueBody` field access differs, check `proto/wavespan/v1/common.pb.go` for the exact oneof accessor.

- [ ] **Step 5: Commit**
```bash
git add internal/holderinspect/
git commit -m "holderinspect: Layer 1 within-cluster single-key resolver (owns self, deterministic, redacting)"
```

---

## Task 4: Layer 2 — `global.PeerInspector` + peer-side builder (TDD)

The client side fans out to peers' `InspectKey`; the peer-side builder turns a Layer 1 result into an `InspectKeyResponse` tagged with this cluster's id.

**Files:**
- Create: `internal/replication/global/inspect.go`
- Test: `internal/replication/global/inspect_test.go`

- [ ] **Step 1: Write the failing test**

`internal/replication/global/inspect_test.go`:
```go
package global

import (
	"context"
	"errors"
	"testing"

	"github.com/yannick/wavespan/internal/config"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
)

type fakeGRClient struct {
	resp *wavespanv1.InspectKeyResponse
	err  error
}

func (f *fakeGRClient) InspectKey(_ context.Context, _ *wavespanv1.InspectKeyRequest, _ ...grpc.CallOption) (*wavespanv1.InspectKeyResponse, error) {
	return f.resp, f.err
}

func TestPeerInspector_TagsAndAggregates(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-b", ReplEndpoint: "b1:7800"}}
	// the peer already tagged its holders; PeerInspector trusts the peer's cluster id.
	dial := func(endpoint string) (inspectKeyClient, error) {
		return &fakeGRClient{resp: &wavespanv1.InspectKeyResponse{
			Holders: []*wavespanv1.InspectHolder{{MemberId: "b1", PeerClusterId: "test-b", Version: &wavespanv1.Version{HlcPhysicalMs: 9}}},
			Best:    &wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 9}},
			Complete: true,
		}}, nil
	}
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	holders, best, complete, warnings := pi.InspectPeers(context.Background(), "ns", []byte("k"), true)
	if !complete || len(warnings) != 0 { t.Fatalf("want complete no warnings, got %v %v", complete, warnings) }
	if len(holders) != 1 || holders[0].PeerClusterId != "test-b" { t.Fatalf("peer holder not surfaced/tagged: %v", holders) }
	if best.GetVersion().GetHlcPhysicalMs() != 9 { t.Fatalf("best not surfaced") }
}

func TestPeerInspector_UnreachablePeerPartial(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-b", ReplEndpoint: "b1:7800"}}
	dial := func(endpoint string) (inspectKeyClient, error) { return &fakeGRClient{err: errors.New("down")}, nil }
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	_, _, complete, warnings := pi.InspectPeers(context.Background(), "ns", []byte("k"), false)
	if complete || len(warnings) != 1 { t.Fatalf("want PARTIAL + warning, got complete=%v warns=%v", complete, warnings) }
}

func TestPeerInspector_SkipsSelfClusterAndEmptyEndpoint(t *testing.T) {
	peers := []config.ClusterPeer{{ClusterID: "test-a", ReplEndpoint: "a2:7800"}, {ClusterID: "test-b", ReplEndpoint: ""}}
	called := false
	dial := func(endpoint string) (inspectKeyClient, error) { called = true; return &fakeGRClient{resp: &wavespanv1.InspectKeyResponse{Complete: true}}, nil }
	pi := newPeerInspectorWithDial("test-a", peers, dial)
	_, _, complete, _ := pi.InspectPeers(context.Background(), "ns", []byte("k"), false)
	if called { t.Fatal("must not dial self-cluster or empty-endpoint peers") }
	if !complete { t.Fatal("no reachable peers => complete (nothing to be incomplete about)") }
}

// peer-side builder: tags this cluster's holders and copies through completeness/warnings.
func TestBuildPeerResponse_TagsSelfCluster(t *testing.T) {
	holders := []*wavespanv1.InspectHolder{{MemberId: "b1"}, {MemberId: "b2"}}
	best := &wavespanv1.StoredRecord{Version: &wavespanv1.Version{HlcPhysicalMs: 1}}
	resp := BuildPeerResponse("test-b", holders, best, true, nil)
	for _, h := range resp.GetHolders() {
		if h.GetPeerClusterId() != "test-b" { t.Fatalf("holder not tagged: %v", h) }
	}
	if !resp.GetComplete() || resp.GetBest() == nil { t.Fatal("completeness/best not carried") }
}
```

- [ ] **Step 2: Run it to confirm failure**

Run: `go test ./internal/replication/global/ -run 'PeerInspector|BuildPeerResponse'`
Expected: FAIL — undefined symbols.

- [ ] **Step 3: Implement `inspect.go`**

`internal/replication/global/inspect.go`:
```go
package global

import (
	"context"
	"fmt"
	"sort"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc"
)

// inspectKeyClient is the slice of GlobalReplicationClient that PeerInspector uses (one method),
// so tests can fake it without a real connection.
type inspectKeyClient interface {
	InspectKey(ctx context.Context, in *wavespanv1.InspectKeyRequest, opts ...grpc.CallOption) (*wavespanv1.InspectKeyResponse, error)
}

// PeerInspector resolves a key across configured peer clusters (Layer 2, design/06+26). For each
// peer it calls GlobalReplication.InspectKey; the peer runs its own within-cluster resolution and
// returns holders tagged with its cluster_id. Best-effort: an unreachable peer yields a warning
// and PARTIAL, never an error.
type PeerInspector struct {
	selfCluster string
	peers       []config.ClusterPeer
	dial        func(endpoint string) (inspectKeyClient, error)
}

// NewPeerInspector builds the inspector dialling peers over the pooled gRPC connections (mirrors
// Sender.client).
func NewPeerInspector(selfCluster string, peers []config.ClusterPeer) *PeerInspector {
	return newPeerInspectorWithDial(selfCluster, peers, func(endpoint string) (inspectKeyClient, error) {
		conn, err := rpcopts.GRPCConn(endpoint)
		if err != nil {
			return nil, err
		}
		return wavespanv1.NewGlobalReplicationClient(conn), nil
	})
}

func newPeerInspectorWithDial(selfCluster string, peers []config.ClusterPeer, dial func(string) (inspectKeyClient, error)) *PeerInspector {
	return &PeerInspector{selfCluster: selfCluster, peers: peers, dial: dial}
}

// InspectPeers returns aggregated peer holders, the latest peer record, completeness, and warnings.
func (p *PeerInspector) InspectPeers(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) {
	complete = true
	for _, peer := range p.peers {
		if peer.ReplEndpoint == "" || peer.ClusterID == p.selfCluster {
			continue
		}
		cl, err := p.dial(peer.ReplEndpoint)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("peer %s (%s) dial failed: %v", peer.ClusterID, peer.ReplEndpoint, err))
			complete = false
			continue
		}
		resp, err := cl.InspectKey(ctx, &wavespanv1.InspectKeyRequest{Namespace: ns, Key: key, IncludeValue: reveal})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("peer %s (%s) unreachable: %v", peer.ClusterID, peer.ReplEndpoint, err))
			complete = false
			continue
		}
		holders = append(holders, resp.GetHolders()...)
		warnings = append(warnings, resp.GetWarnings()...)
		if !resp.GetComplete() {
			complete = false
		}
		if rec := resp.GetBest(); rec != nil {
			if best == nil || version.FromProto(rec.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0 {
				best = rec
			}
		}
	}
	sort.Slice(holders, func(i, j int) bool {
		if holders[i].GetPeerClusterId() != holders[j].GetPeerClusterId() {
			return holders[i].GetPeerClusterId() < holders[j].GetPeerClusterId()
		}
		return holders[i].GetMemberId() < holders[j].GetMemberId()
	})
	return holders, best, complete, warnings
}

// BuildPeerResponse stamps each holder with this cluster's id and wraps a Layer 1 result as the
// InspectKey RPC response. Used by the gRPC GlobalReplication adapter's InspectKey handler.
func BuildPeerResponse(selfCluster string, holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string) *wavespanv1.InspectKeyResponse {
	for _, h := range holders {
		h.PeerClusterId = selfCluster
	}
	return &wavespanv1.InspectKeyResponse{Holders: holders, Best: best, Complete: complete, Warnings: warnings}
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `go test ./internal/replication/global/ -run 'PeerInspector|BuildPeerResponse'`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**
```bash
git add internal/replication/global/inspect.go internal/replication/global/inspect_test.go
git commit -m "global: Layer 2 PeerInspector (peer-cluster InspectKey fan-out) + peer-side response builder"
```

---

## Task 5: Peer-side `InspectKey` handler on the gRPC adapter

Host the served `InspectKey` on `grpcsrv.GlobalReplication`, delegating to an injected Layer 1 resolver and `BuildPeerResponse`.

**Files:**
- Modify: `internal/grpcsrv/global.go`
- Test: covered by the integration test (Task 9); a unit test here would need a full resolver — skip per YAGNI, the merge/build logic is already unit-tested in Tasks 3–4.

- [ ] **Step 1: Add an injected peer-inspector seam**

In `internal/grpcsrv/global.go`, define a minimal interface the adapter calls, and extend the constructor:
```go
// PeerKeyInspector runs this cluster's within-cluster resolution for one key and returns a
// peer-facing InspectKey response (holders tagged with this cluster's id). Satisfied by a small
// adapter over holderinspect.ClusterResolver wired in main.go. nil disables InspectKey.
type PeerKeyInspector interface {
	InspectKeyLocal(ctx context.Context, namespace string, key []byte, includeValue bool) (*wavespanv1.InspectKeyResponse, error)
}
```
Change the struct + constructor:
```go
type GlobalReplication struct {
	wavespanv1.UnimplementedGlobalReplicationServer
	applier *global.Applier
	ae      *global.AntiEntropy
	peer    PeerKeyInspector
}

func NewGlobalReplication(applier *global.Applier, ae *global.AntiEntropy, peer PeerKeyInspector) *GlobalReplication {
	return &GlobalReplication{applier: applier, ae: ae, peer: peer}
}
```
Add the handler:
```go
// InspectKey answers a peer cluster's "who holds this key?" by running this cluster's within-
// cluster resolution (Layer 1) and tagging holders with this cluster's id.
func (s *GlobalReplication) InspectKey(ctx context.Context, m *wavespanv1.InspectKeyRequest) (*wavespanv1.InspectKeyResponse, error) {
	if s.peer == nil {
		return nil, status.Error(codes.Unimplemented, "peer inspect not configured")
	}
	return s.peer.InspectKeyLocal(ctx, m.GetNamespace(), m.GetKey(), m.GetIncludeValue())
}
```

- [ ] **Step 2: Build (the new `NewGlobalReplication` arg breaks main.go — fixed in Task 7)**

Run: `go build ./internal/grpcsrv/...`
Expected: PASS for the package in isolation.

- [ ] **Step 3: Commit**
```bash
git add internal/grpcsrv/global.go
git commit -m "grpcsrv: GlobalReplication.InspectKey handler (delegates to injected peer inspector)"
```

---

## Task 6: Orchestrate in `InspectGlobal` + obs seams (TDD)

Replace the stub: drop the "not configured" branch and the pre-inspector self-append; call Layer 1 then Layer 2; merge into one `InspectKey` row.

**Files:**
- Modify: `internal/observability/obsservice.go` (seams), `internal/observability/inspect_global.go`
- Test: `internal/observability/inspect_global_test.go`

- [ ] **Step 1: Replace the `globalInspector` seam in `obsservice.go`**

Remove the `GlobalInspector` interface + `globalInspector` field + `WithGlobalInspector`. Add:
```go
// ClusterKeyResolver resolves a key's holders within THIS cluster (Layer 1). nil disables Global.
type ClusterKeyResolver interface {
	ResolveKey(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string)
}

// PeerKeyInspector resolves a key across peer clusters (Layer 2). nil => local-cluster only.
type PeerKeyInspector interface {
	InspectPeers(ctx context.Context, ns string, key []byte, reveal bool) (holders []*wavespanv1.InspectHolder, best *wavespanv1.StoredRecord, complete bool, warnings []string)
}
```
Add fields `clusterResolver ClusterKeyResolver` and `peerInspector PeerKeyInspector`, plus builders `WithClusterResolver(...)` and `WithPeerInspector(...)` (mirroring the existing `With*` style).

- [ ] **Step 2: Write the failing handler test**

`internal/observability/inspect_global_test.go` — use a fake `ServerStream` capturing sent rows (mirror the pattern already in `inspect_test.go`), inject fake resolver + peer inspector, and assert:
- holders from both layers appear, sorted; `best` value renders when serving node lacks the key;
- `PARTIAL` + warnings when either layer is incomplete;
- with `peerInspector == nil` (or `include_peer_clusters=false`), only Layer 1 holders appear and a Layer-1-complete result is `COMPLETE` (the old stub always returned PARTIAL — regression guard).

(Reuse the existing test's stream fake; assert on the emitted `InspectKey` + trailer.)

- [ ] **Step 3: Run it — FAIL**

Run: `go test ./internal/observability/ -run InspectGlobal`
Expected: FAIL (handler still the stub / seams unused).

- [ ] **Step 4: Rewrite `inspect_global.go` orchestration**

```go
func (s *ObsService) InspectGlobal(ctx context.Context, req *connect.Request[wavespanv1.InspectGlobalRequest], stream *connect.ServerStream[wavespanv1.InspectRow]) error {
	m := req.Msg
	ns, key := m.GetNamespace(), m.GetKey()
	role := security.RoleFrom(ctx)
	reveal := m.GetIncludeValue() && role == security.RoleAdmin

	if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Header{Header: &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID, ServedByMemberId: s.self.MemberID, Source: wavespanv1.ReadSource_FETCHED_CLOSEST_HOLDER,
	}}}); err != nil {
		return err
	}

	ik := &wavespanv1.InspectKey{LogicalPath: ns + "/" + string(key), KeyHash: security.KeyHash(ns, key), LogicalKey: key}
	complete := true
	var warnings []string
	var best *wavespanv1.StoredRecord

	if s.clusterResolver == nil {
		// Global resolution not wired at all: degrade honestly.
		complete = false
		warnings = append(warnings, "global holder resolution not configured on this node")
	} else {
		holders, b, c, w := s.clusterResolver.ResolveKey(ctx, ns, key, reveal)
		ik.Holders = append(ik.Holders, holders...)
		best, complete, warnings = b, c, w

		if m.GetIncludePeerClusters() && s.peerInspector != nil {
			ph, pb, pc, pw := s.peerInspector.InspectPeers(ctx, ns, key, reveal)
			ik.Holders = append(ik.Holders, ph...)
			warnings = append(warnings, pw...)
			complete = complete && pc
			if pb != nil && (best == nil || version.FromProto(pb.GetVersion()).Compare(version.FromProto(best.GetVersion())) > 0) {
				best = pb
			}
		}
	}

	// Surface the winning record's version/tombstone/expiry/value (value already redacted by the resolver).
	if best != nil {
		ik.Version = best.GetVersion()
		ik.Tombstone = best.GetTombstone()
		if best.ExpiresAtUnixMs != nil {
			ik.ExpiresAtUnixMs = best.ExpiresAtUnixMs
		}
		if v := best.GetValue().GetInline(); len(v) > 0 {
			ik.Value = v
		}
	}
	sortInspectHolders(ik.Holders) // (peer_cluster_id, member_id)

	if err := stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Key{Key: ik}}); err != nil {
		return err
	}
	completeness := wavespanv1.Completeness_COMPLETE
	if !complete {
		completeness = wavespanv1.Completeness_PARTIAL
	}
	return stream.Send(&wavespanv1.InspectRow{Row: &wavespanv1.InspectRow_Trailer{Trailer: &wavespanv1.InspectTrailer{
		RowsReturned: 1, FinalCompleteness: completeness, Warnings: warnings,
	}}})
}
```
Add a small `sortInspectHolders` helper (same composite key as `holderinspect.sortHolders`). Keep the `Handler()` method as-is. Add the `version` import.

- [ ] **Step 5: Run tests — PASS**

Run: `go test ./internal/observability/...`
Expected: PASS (new test + existing `inspect_test.go` still green).

- [ ] **Step 6: Commit**
```bash
git add internal/observability/
git commit -m "observability: InspectGlobal resolves holders via Layer 1 + Layer 2 (drop stub, drop self double-count)"
```

---

## Task 7: Wire it all in `main.go`

**Files:**
- Modify: `cmd/wavespan-node/main.go`

- [ ] **Step 1: Construct the Layer 1 resolver (always)**

After `replicator := local.NewConnectReplicator(httpClient)` (line ~221) and once `rstore`/`svc`/`self` exist, build:
```go
clusterResolver := holderinspect.New(self, svc, replicator,
	func(ns string, key []byte) (*wavespanv1.StoredRecord, bool, error) { return rstore.GetRecord(ns, key) })
```
(`svc` is the membership service — confirm it satisfies `Members() []membership.MemberView`; it already feeds `WithClusterScan`/`ClusterSource`.)

- [ ] **Step 2: Construct the peer inspector + peer-side handler when global is enabled**

Inside the `if cfg.GlobalReplication.Enabled()` block (near line 404), build a peer-side adapter over the resolver and a `PeerInspector`:
```go
peerInspector = global.NewPeerInspector(cfg.ClusterID, cfg.GlobalReplication.Peers)
peerHandler = grpcsrvPeerAdapter{selfCluster: cfg.ClusterID, resolver: clusterResolver} // implements grpcsrv.PeerKeyInspector
```
Define the tiny adapter (in main.go or a small file) that turns a Layer 1 result into an `InspectKeyResponse` via `global.BuildPeerResponse`:
```go
type grpcsrvPeerAdapter struct {
	selfCluster string
	resolver    *holderinspect.ClusterResolver
}
func (a grpcsrvPeerAdapter) InspectKeyLocal(ctx context.Context, ns string, key []byte, includeValue bool) (*wavespanv1.InspectKeyResponse, error) {
	h, best, complete, warns := a.resolver.ResolveKey(ctx, ns, key, includeValue)
	return global.BuildPeerResponse(a.selfCluster, h, best, complete, warns), nil
}
```

- [ ] **Step 3: Pass the peer handler into the gRPC adapter registration**

Update line ~529:
```go
wavespanv1.RegisterGlobalReplicationServer(grpcDataSrv, grpcsrv.NewGlobalReplication(globalApplier, globalAE, peerHandler))
```
(`peerHandler` is nil-typed when global is disabled — but this registration is already inside the global-enabled branch, so it's set. Confirm the surrounding `if`.)

- [ ] **Step 4: Wire the obs seams**

In the `obsSvc := observability.NewObsService(...)` builder chain (line ~692), add:
```go
		WithClusterResolver(clusterResolver).
```
and, only when global is enabled, `obsSvc = obsSvc.WithPeerInspector(peerInspector)` after the chain.

- [ ] **Step 5: Build the whole binary**

Run: `go build ./...`
Expected: PASS. Fix any import/declaration ordering (the peer adapter type, the `peerInspector`/`peerHandler` vars declared `nil` above the `if` like the existing `globalApplier`).

- [ ] **Step 6: Commit**
```bash
git add cmd/wavespan-node/main.go
git commit -m "node: wire global inspect — Layer 1 resolver always, Layer 2 peer inspector + handler when global enabled"
```

---

## Task 8: UI — request peers + show cluster in holders

**Files:**
- Modify: `ui/src/views/DataBrowser.tsx`
- (Already in this branch, commit alongside: `ui/src/components/Modal.tsx`, `ui/src/lib/clipboard.ts`, `ui/src/components/index.ts`, `ui/src/theme/components.css`, and the value-modal edits to `DataBrowser.tsx`.)

- [ ] **Step 1: Send `includePeerClusters` for Global**

In `run()`, the `obs.inspectGlobal({...})` call already passes `includeValue`; add `includePeerClusters: true`.

- [ ] **Step 2: Render `peer_cluster_id` in the holders column**

Change the holders cell so a holder with a peer cluster shows `memberId · clusterId`:
```tsx
<td className="ws-mono">{k.holders.map((h) => h.peerClusterId ? `${h.memberId}·${h.peerClusterId}` : h.memberId).join(", ")}</td>
```

- [ ] **Step 3: Typecheck + build**

Run (in `ui/`): `npx tsc --noEmit -p tsconfig.json && npm run build`
Expected: PASS (regenerate proto TS if needed — `obs.inspectGlobal` already accepts `includePeerClusters` since the proto field exists; if the generated client lacks it, run the UI proto-gen per `ui/package.json`).

- [ ] **Step 4: Commit**
```bash
git add ui/
git commit -m "ui(data): Global mode requests peer clusters and shows holder cluster; value modal + copy"
```

---

## Task 9: Integration test — cross-cluster + single-cluster regression

**Files:**
- Create: `tests/integration/global_inspect_test.go` (`//go:build integration`)

- [ ] **Step 1: Write the cross-cluster test**

Reuse the existing `composeGlobal`, `kvClient`, `membership` helpers. An `obsClient(port)` connect client for `ObservabilityService` may need adding (mirror `kvClient`). Outline:
```go
//go:build integration

package integration

// TestGlobalInspectAcrossClusters:
//  1. composeGlobal(t, "up", "-d"); cleanup down -v.
//  2. wait until membership("7951")==2 && membership("7953")==2.
//  3. Put a key into a globally-replicated namespace on test-a (admin 7951).
//  4. eventually: Get on test-b (7953) returns it (replication landed).
//  5. InspectGlobal(include_peer_clusters=true, include_value=true) on test-b (7953):
//     assert holders include at least one member with peer_cluster_id=="test-a" AND one local
//     (test-b) holder; final completeness == COMPLETE; value present.
//  6. composeGlobal(t, "stop", "a1","a2"); InspectGlobal again => PARTIAL + a warning naming a peer.
```
Use the `siblings`/default namespace already configured; if a namespace must be `replicationFactor: global` to cross clusters, write to one the compose marks global (the compose ships origin KV writes to peers when `GLOBAL_MODE=active-active-async`, so a normal Put on test-a replicates to test-b — verify via the existing `TestGlobalReplicationAcrossClusters` which already asserts cross-cluster Get).

- [ ] **Step 2: Add the single-cluster regression**

Either as a sub-test using the standard `docker/docker-compose.yaml` 3-node cluster (helpers exist) or as a focused assertion: `InspectGlobal(include_peer_clusters=false)` after writing a key replicated to 2 nodes lists **both** holders and reports `COMPLETE` (the old stub listed only the serving node and always `PARTIAL`).

- [ ] **Step 3: Run the integration tests**

Run: `make test-integration` (or `docker compose -f docker/docker-compose.global.yaml build` then `go test -tags integration -timeout 600s ./tests/integration/ -run GlobalInspect`)
Expected: PASS. Docker + the sibling `../wavesdb` module are required.

- [ ] **Step 4: Commit**
```bash
git add tests/integration/global_inspect_test.go
git commit -m "test(integration): cross-cluster InspectGlobal resolves peer holders; single-cluster lists all holders"
```

---

## Task 10: Full verification

- [ ] **Step 1: Unit + build + lint**

Run: `go build ./... && go test ./... && make lint`
Expected: all PASS (unit suite excludes the `integration`-tagged tests).

- [ ] **Step 2: Manual smoke (optional, mirrors the original CDP repro)**

Bring up `docker/docker-compose.global.yaml`, open test-b's console (admin 7953), Data Browser → Global, search a test-a-origin key: holders from both clusters appear with cluster tags; completeness `COMPLETE`; value renders; long values open the copy modal.

- [ ] **Step 3: Final commit / branch ready**

Branch `feat/global-inspect-rpc` ready for `superpowers:finishing-a-development-branch`.

---

## Notes / risks

- **`svc` as `MemberSource`:** confirm the membership service exposes `Members() []membership.MemberView` (it already satisfies `observability.ClusterSource`, which declares exactly that). If the concrete type differs, pass the same value used for `WithClusterScan`.
- **Proto cross-file import:** `replication.proto` importing `observability.proto` for `InspectHolder` must not create a proto import cycle — `observability.proto` does not import `replication.proto`, so it's safe. If buf flags layering, define a local `PeerHolder` message instead and map it (avoid unless forced).
- **Determinism:** the composite `(peer_cluster_id, member_id)` sort is applied in three places (Layer 1, Layer 2, and the final merge in `inspect_global.go`) — factor a shared comparator if the duplication bothers review, but correctness only needs the final sort.
- **Redaction:** values are stripped in the resolver unless `reveal`; peers are asked with `include_value=reveal`, so a non-admin never receives peer values. Keep this invariant if refactoring.
