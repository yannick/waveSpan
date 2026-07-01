package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb"
)

// Roster reports cluster membership by member id. Live is the set the coordinator fans prepare/export
// out to; Members is the full expected set (non-forgotten) used for the F1 member-completeness coverage
// check — an expected member that did not export makes the backup PARTIAL (see coverage.go).
type Roster interface {
	Live() []string
	Members() []string
}

// ExportRequest is the coordinator's instruction to one node to export its assignment. Each node resolves
// its OWN parent checkpoint (from ParentBackupID) and exports to the destination it resolves — so real
// incrementals and alt destinations work across nodes (Phase 3c Task 0).
type ExportRequest struct {
	BackupID       string
	MemberID       string
	Assignment     Selector
	Planes         []Plane
	FrontierT      int64
	ParentBackupID string // non-empty → physical incremental; the node resolves its own parent checkpoint
	// ObjStore is the coordinator's already-resolved destination store — an in-process handle used by the
	// coordinator's own node and the test harness (avoids a self-dial). The gRPC client ignores it.
	ObjStore ObjectStore
	// Destination is the original request destination, forwarded for the gRPC node to RE-RESOLVE against
	// its own config/env (named/default) or the inline/secret-ref creds in the request. nil = the node's
	// default destination. The in-process path ignores it (it has ObjStore).
	Destination *wavespanv1.Destination
}

// resolveParentCheckpoint reads a node's parent checkpoint from the parent backup's per-node physical
// sub-manifest in store (matched by member id). It returns nil (→ full physical export) when there is no
// parent backup, or when this member has no entry in it (a node added since the parent).
func resolveParentCheckpoint(store ObjectStore, parentBackupID, memberID string) (*wavesdb.CheckpointManifest, error) {
	if parentBackupID == "" {
		return nil, nil
	}
	key := PhysicalManifestKey(parentBackupID, memberID)
	ok, err := store.Exists(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // new node since the parent → full physical
	}
	pm, err := ReadPhysicalManifest(store, key)
	if err != nil {
		return nil, err
	}
	return pm.ToCheckpointManifest(), nil
}

// NodeClient is the coordinator's handle to one node's BackupService (prepare/export). The production
// implementation dials the node over the gRPC data port; tests substitute in-process fakes. The local
// node's client may delegate straight to the coordinator's own Agent.
type NodeClient interface {
	Prepare(ctx context.Context, backupID string, frontierT int64) (PrepareResult, error)
	Export(ctx context.Context, req ExportRequest) (ExportResult, error)
}

// ClientFactory returns a NodeClient for a member id.
type ClientFactory func(memberID string) (NodeClient, error)

// Assigner computes, given the live members, each member's export assignment and any uncovered range
// gaps (ranges with no live holder). A gap makes the resulting backup PARTIAL.
type Assigner interface {
	Assign(live []string) (assignments map[string]Selector, gaps []string)
}

// Clock supplies wall-clock milliseconds; injectable so tests are deterministic.
type Clock interface {
	NowMs() int64
}

type realClock struct{}

func (realClock) NowMs() int64 { return time.Now().UnixMilli() }

// Config wires a Coordinator's dependencies.
type Config struct {
	Self        string             // this node's member id
	Meta        MetaStore          // durable intent catalog (the meta shard)
	ObjStore    ObjectStore        // backup destination (also used by the local agent)
	Roster      Roster             // live membership
	ClientFor   ClientFactory      // dial a node's BackupService
	Assigner    Assigner           // ownership/assignment plan
	Clock       Clock              // nil = real time
	LocalStore  storage.LocalStore // this node's store, for the node-internal Prepare/Export RPCs
	Registry    *Registry          // export contributor registry (nil = DefaultRegistry)
	LeaseMs      int64        // intent lease window (0 = default)
	RetainMs     int64        // terminal-intent retention window (0 = default; Phase 3d)
	IsMetaLeader func() bool  // reports whether this node leads the meta shard (gates the sweep; nil = always)
	Logger       *slog.Logger // sweep logging (nil = slog.Default())

	// Phase 3e destination resolution. BackupCfg holds the default + named destinations; Getenv resolves
	// credential env-var refs (nil = os.Getenv); OpenStore turns a resolved destination into an
	// ObjectStore (nil = reuse ObjStore for the FS default; S3 destinations then require an opener).
	BackupCfg config.BackupConfig
	Getenv    func(string) string
	OpenStore func(ResolvedDestination) (ObjectStore, error)

	// F1 held-range coverage. HostedDataShards reports the collection data-shard ids this node hosts
	// (nil = none; wired from the live collections Manager); PrepareLocal echoes them as HeldRanges.
	// ExpectedDataShards is the cluster's configured data-shard count for the commit-time coverage
	// cross-check (0 = unknown → the collections check is skipped; see coverage.go).
	HostedDataShards   func() []uint64
	ExpectedDataShards uint64

	// MemberTimeout bounds each per-member Prepare/Export RPC (F5) so a dead/hung peer does not stall the
	// whole backup; on timeout (or any RPC error) a non-self member is skipped → PARTIAL. 0 = default.
	MemberTimeout time.Duration
}

// Coordinator drives a consistent cluster backup: it records a durable Intent in the meta shard,
// picks a cluster frontier, assigns owners, drives PrepareBackup/ExportBackup to the live nodes over the
// BackupService client (sequentially in 3a), and commits a cluster.manifest with explicit PARTIAL
// detection (design/backup phase 3a). It also serves the node-internal Prepare/Export for its own node
// via Agent.
type Coordinator struct {
	self       string
	meta       MetaStore
	objStore   ObjectStore
	roster     Roster
	clientFor  ClientFactory
	assigner   Assigner
	clock      Clock
	localStore   storage.LocalStore
	agent        *Agent
	leaseMs      int64
	retainMs     int64
	isMetaLeader func() bool
	logger       *slog.Logger
	backupCfg    config.BackupConfig
	getenv       func(string) string
	openStore    func(ResolvedDestination) (ObjectStore, error)

	hostedShards       func() []uint64
	expectedDataShards uint64
	memberTimeout      time.Duration

	// failAfterPhase, when set (test hook), makes run() persist the named phase's completion and then
	// return early — simulating coordinator loss for the resumability test.
	failAfterPhase Phase
}

const defaultLeaseMs = 10 * 60 * 1000        // 10 minutes
const defaultRetainMs = 30 * 24 * 60 * 60 * 1000 // 30 days terminal-intent retention (Phase 3d)

// frontierLeaseMs is the small lease added to now when picking the backup frontier T (spec §2 Begin:
// T = HLC.now()+lease). In 3a T is recorded for provenance only and not yet used as a Version<=T cut
// (the cluster-wide HLC sealing is Phase 3a.1); the now+lease value is set here so the contract is right
// when the cut lands.
const frontierLeaseMs = 5 * 1000 // 5 seconds

// defaultMemberTimeout bounds each per-member prepare/export RPC (F5). Generous enough for a healthy
// export yet short enough that a dead/hung peer is skipped promptly rather than stalling every backup.
const defaultMemberTimeout = 30 * time.Second

// NewCoordinator builds a Coordinator from cfg.
func NewCoordinator(cfg Config) *Coordinator {
	clk := cfg.Clock
	if clk == nil {
		clk = realClock{}
	}
	lease := cfg.LeaseMs
	if lease == 0 {
		lease = defaultLeaseMs
	}
	retain := cfg.RetainMs
	if retain == 0 {
		retain = defaultRetainMs
	}
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}
	getenv := cfg.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	memberTimeout := cfg.MemberTimeout
	if memberTimeout <= 0 {
		memberTimeout = defaultMemberTimeout
	}
	return &Coordinator{
		self:         cfg.Self,
		meta:         cfg.Meta,
		objStore:     cfg.ObjStore,
		roster:       cfg.Roster,
		clientFor:    cfg.ClientFor,
		assigner:     cfg.Assigner,
		clock:        clk,
		localStore:   cfg.LocalStore,
		agent:        NewAgent(cfg.Registry),
		leaseMs:      lease,
		retainMs:     retain,
		isMetaLeader: cfg.IsMetaLeader,
		logger:       lg,
		backupCfg:    cfg.BackupCfg,
		getenv:       getenv,
		openStore:    cfg.OpenStore,

		hostedShards:       cfg.HostedDataShards,
		expectedDataShards: cfg.ExpectedDataShards,
		memberTimeout:      memberTimeout,
	}
}

// resolveStore resolves a destination spec to its object store + the non-secret descriptor to persist.
// With no OpenStore configured it reuses the default ObjStore for the FS fallback; S3 destinations then
// require an opener.
func (c *Coordinator) resolveStore(spec DestinationSpec) (ObjectStore, Descriptor, error) {
	rd, desc, err := ResolveDestination(c.backupCfg, spec, c.getenv)
	if err != nil {
		return nil, Descriptor{}, err
	}
	if c.openStore != nil {
		store, err := c.openStore(rd)
		if err != nil {
			return nil, Descriptor{}, err
		}
		return store, desc, nil
	}
	if rd.UseFS {
		return c.objStore, desc, nil
	}
	return nil, Descriptor{}, fmt.Errorf("backup: S3 destination %q requires an object-store opener", desc.Bucket)
}

// BeginBackup records a durable intent, picks a frontier, and drives the phased backup to completion
// (synchronous in 3a). It returns the allocated backup id; the intent is the source of truth a resuming
// coordinator reads if this one is lost mid-flight.
func (c *Coordinator) BeginBackup(ctx context.Context, spec *wavespanv1.BackupSpec) (string, error) {
	now := c.clock.NowMs()
	id := newBackupID(now)

	// Resolve the destination at Begin: build the object store for this run (transient creds live only
	// here) and capture the NON-SECRET descriptor to persist in the intent/manifest. Each node also
	// re-resolves the destination from the forwarded request (Phase 3c Task 0), so alt destinations work
	// across a multi-node cluster — there is no single-node restriction.
	store, desc, err := c.resolveStore(destinationSpecFromProto(spec.GetDestination()))
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, err)
	}

	in := &Intent{
		SchemaVersion:      intentSchemaVersion,
		BackupID:           id,
		FrontierT:          now + frontierLeaseMs,
		CaptureWallClockMs: now,
		Selection:          selectorFromProto(spec.GetSelection()),
		Planes:             planesFromProto(spec.GetPlanes()),
		Parent:             spec.GetParent(),
		Destination:        desc, // descriptor only — never the resolved credentials
		Status:             StatusRunning,
		Phase:              PhaseAssign,
		LeaseDeadlineMs:    now + c.leaseMs,
		StartedMs:          now,
	}
	if len(in.Planes) == 0 {
		in.Planes = []Plane{PlaneLogical}
	}
	if in.Parent != "" {
		if err := c.validateParent(store, in.Planes, in.Parent); err != nil {
			return "", err
		}
	}
	if err := c.persist(ctx, in); err != nil {
		return "", err
	}
	if err := c.run(ctx, in, store, spec.GetDestination()); err != nil {
		return id, err
	}
	return id, nil
}

// run advances a backup from its current phase to commit, persisting the intent at every phase boundary
// so a resuming coordinator can pick up exactly where this one left off. store is the coordinator's
// resolved destination (commit + in-process export write there); protoDest is forwarded to gRPC nodes so
// they re-resolve the same target.
func (c *Coordinator) run(ctx context.Context, in *Intent, store ObjectStore, protoDest *wavespanv1.Destination) error {
	if in.Phase == PhaseAssign {
		assignments, gaps := c.assigner.Assign(c.roster.Live())
		in.Assignments = assignments
		in.Gaps = gaps
		in.Phase = PhasePrepare
		if err := c.persist(ctx, in); err != nil {
			return err
		}
		if c.failAfterPhase == PhaseAssign {
			return nil
		}
	}
	if in.Phase == PhasePrepare {
		if err := c.prepareAll(ctx, in); err != nil {
			return err
		}
		in.Phase = PhaseExport
		if err := c.persist(ctx, in); err != nil {
			return err
		}
		if c.failAfterPhase == PhasePrepare {
			return nil
		}
	}
	if in.Phase == PhaseExport {
		if err := c.exportAll(ctx, in, store, protoDest); err != nil {
			return err
		}
		in.Phase = PhaseCommit
		if err := c.persist(ctx, in); err != nil {
			return err
		}
		if c.failAfterPhase == PhaseExport {
			return nil
		}
	}
	if in.Phase == PhaseCommit {
		return c.commit(ctx, in, store)
	}
	return nil
}

// prepareAll calls PrepareBackup on every assigned member (sequentially in 3a). A member that is
// unreachable/errors (a stale-but-alive roster entry on a churned cluster) is SKIPPED, not fatal: it is
// logged and simply not recorded (so it contributes no coverage → commit() marks it a member gap →
// PARTIAL). The coordinator's OWN node (self) failing IS fatal — a backup missing self can't be produced.
// Each call is bounded by memberTimeout so a dead peer can't stall the whole backup (F5).
func (c *Coordinator) prepareAll(ctx context.Context, in *Intent) error {
	for _, mid := range sortedMembers(in.Assignments) {
		cl, err := c.clientFor(mid)
		if err == nil {
			var pr PrepareResult
			pr, err = callWithTimeout(ctx, c.memberTimeout, func(cctx context.Context) (PrepareResult, error) {
				return cl.Prepare(cctx, in.BackupID, in.FrontierT)
			})
			if err == nil {
				upsertNode(in, NodeRecord{MemberID: mid, Phase: PhasePrepare, HeldRanges: pr.HeldRanges})
				continue
			}
		}
		if mid == c.self {
			return fmt.Errorf("backup: preparing self %q: %w", mid, err)
		}
		c.logger.Warn("backup: member unreachable at prepare — skipping (will be a coverage gap)", "member", mid, "err", err)
	}
	return nil
}

// callWithTimeout runs fn under a child context bounded by memberTimeout (F5).
func callWithTimeout[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(cctx)
}

// exportAll calls ExportBackup on every assigned member (sequentially in 3a) and records each node's
// export result. store is the coordinator's resolved destination (used by the in-process path); protoDest
// is the original request destination, forwarded so a gRPC node re-resolves the same target. Each node
// resolves its own parent checkpoint from ParentBackupID.
func (c *Coordinator) exportAll(ctx context.Context, in *Intent, store ObjectStore, protoDest *wavespanv1.Destination) error {
	// Only export members that successfully prepared (proven reachable moments ago); a member skipped at
	// prepare is not retried here. A member that dies BETWEEN prepare and export is still tolerated below.
	prepared := map[string]bool{}
	for _, n := range in.PerNode {
		prepared[n.MemberID] = true
	}
	exported := 0
	for _, mid := range sortedMembers(in.Assignments) {
		if !prepared[mid] {
			continue // skipped at prepare → no coverage (becomes a member gap at commit)
		}
		cl, err := c.clientFor(mid)
		if err == nil {
			var res ExportResult
			res, err = callWithTimeout(ctx, c.memberTimeout, func(cctx context.Context) (ExportResult, error) {
				return cl.Export(cctx, ExportRequest{
					BackupID:       in.BackupID,
					MemberID:       mid,
					Assignment:     in.Assignments[mid],
					Planes:         in.Planes,
					FrontierT:      in.FrontierT,
					ParentBackupID: in.Parent,
					ObjStore:       store,
					Destination:    protoDest,
				})
			})
			if err == nil {
				upsertNode(in, NodeRecord{
					MemberID:            mid,
					Phase:               PhaseExport,
					Objects:             res.Objects,
					Bytes:               res.Bytes,
					SubManifestKey:      res.SubManifestKey,
					StorageUUID:         res.StorageUUID,
					PhysicalManifestKey: res.PhysicalManifestKey,
					PhysicalGlobalSeq:   res.PhysicalGlobalSeq,
					Done:                true,
				})
				exported++
				continue
			}
		}
		if mid == c.self {
			return fmt.Errorf("backup: exporting self %q: %w", mid, err)
		}
		c.logger.Warn("backup: member unreachable at export — skipping (will be a coverage gap)", "member", mid, "err", err)
	}
	if exported == 0 {
		return fmt.Errorf("backup: no member exported successfully (all unreachable) — cannot produce a backup")
	}
	return nil
}

// validateParent rejects an incremental whose parent cannot anchor it: incrementals are physical-only
// (logical is full-only), and the parent backup must itself carry a physical plane. Both are clear,
// up-front errors rather than a silently-degraded backup.
func (c *Coordinator) validateParent(store ObjectStore, planes []Plane, parentID string) error {
	if !hasPlane(planes, PlanePhysical) {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("backup: incremental from parent %q requires the physical plane; logical backups are full-only", parentID))
	}
	parent, err := ReadClusterManifest(store, parentID)
	if err != nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("backup: parent %q not found: %w", parentID, err))
	}
	if !containsString(parent.Planes, "physical") {
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("backup: parent %q has no physical plane; cannot take a physical incremental from it", parentID))
	}
	return nil
}

func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// commit writes the cluster.manifest and finalizes the intent status. A backup with enumerated gaps is
// committed PARTIAL — never silently COMPLETE.
//
// F1: gaps are the UNION of two sources — (1) the assigner's gap list (ranges it found with no live
// holder) and (2) real coverage gaps computed from the nodes' reported HeldRanges vs the expected
// coverage: a collection data shard hosted by no exporting node ("collections-shard:<id>") and an
// expected member that did not export ("member:<id>"). See coverage.go for why the tiers are checked
// differently (collections is deterministically partitioned; KV/AP has no per-node ownership, so member
// completeness is the honest proxy).
func (c *Coordinator) commit(ctx context.Context, in *Intent, store ObjectStore) error {
	refs := make([]PerNodeRef, 0, len(in.PerNode))
	topo := make([]TopologyEntry, 0, len(in.PerNode))
	for _, n := range in.PerNode {
		if !n.Done {
			continue // a member skipped at prepare/export (F5) exported nothing — no manifest ref/topology
		}
		refs = append(refs, PerNodeRef{
			MemberID:          n.MemberID,
			Ref:               n.SubManifestKey,
			Objects:           n.Objects,
			Bytes:             n.Bytes,
			PhysicalManifest:  n.PhysicalManifestKey,
			PhysicalGlobalSeq: n.PhysicalGlobalSeq,
		})
		topo = append(topo, TopologyEntry{MemberID: n.MemberID, StorageUUID: n.StorageUUID})
	}
	// Union the assigner-reported gaps with the real held-range coverage gaps.
	var expectedMembers []string
	if c.roster != nil {
		expectedMembers = c.roster.Members()
	}
	gaps := append([]string(nil), in.Gaps...)
	gaps = append(gaps, coverageGaps(in.PerNode, c.expectedDataShards, expectedMembers)...)
	in.Gaps = gaps
	status := StatusComplete
	if len(gaps) > 0 {
		status = StatusPartial
	}
	cm := &ClusterManifest{
		FormatVersion:      clusterManifestFormatVersion,
		BackupID:           in.BackupID,
		FrontierT:          in.FrontierT,
		CaptureWallClockMs: in.CaptureWallClockMs,
		Planes:             planesToStrings(in.Planes),
		Parent:             in.Parent,
		SourceTopology:     topo,
		PerNode:            refs,
		Status:             statusString(status),
		Gaps:               in.Gaps,
	}
	if err := WriteClusterManifest(store, cm); err != nil {
		return err
	}
	now := c.clock.NowMs()
	in.Status = status
	in.Phase = PhaseCommit
	in.FinishedMs = now
	in.RetainUntilMs = now + c.retainMs // terminal: schedule retention deletion (Phase 3d)
	return c.persist(ctx, in)
}

// resume loads a persisted intent and continues driving it from its recorded phase (Task 5). It is safe
// to re-run because object keys are deterministic — a re-export overwrites identical content.
//
// NOTE (3e): resume re-resolves the destination to its descriptor when one is recorded; if that descriptor
// names an explicit/inline-credential destination whose creds were never persisted, re-resolution yields
// no credentials and the resumed export to that destination fails — resuming such a backup requires the
// creds re-supplied (or a named/default destination). Default/named destinations re-resolve cleanly.
func (c *Coordinator) resume(ctx context.Context, backupID string) error {
	in, found, err := GetIntent(ctx, c.meta, backupID)
	if err != nil {
		return err
	}
	if !found {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("backup: no intent for %q", backupID))
	}
	store, err := c.storeForDescriptor(in.Destination)
	if err != nil {
		return err
	}
	// Forward a proto destination reconstructed from the descriptor so a gRPC node re-resolves the same
	// target (named/default/secret-ref re-resolve cleanly; inline-cred destinations can't — see resume's
	// doc). The coordinator's own/in-process node uses store directly.
	return c.run(ctx, in, store, descriptorToProto(in.Destination))
}

// storeForDescriptor re-opens the object store for a persisted destination descriptor (resume/3e). A
// default-FS or empty descriptor uses the default store; a named descriptor re-resolves from config; an
// explicit descriptor re-resolves using its recorded credential reference (inline-cred destinations
// cannot be re-resolved — their creds were never persisted).
func (c *Coordinator) storeForDescriptor(d Descriptor) (ObjectStore, error) {
	var spec DestinationSpec
	switch {
	case d.DefaultFS:
		// empty spec → the FS default
	case d.Name != "":
		spec = DestinationSpec{Name: d.Name} // named: re-resolves creds from config
	case d.Bucket != "" && d.Bucket == c.backupCfg.DefaultDestination.Bucket:
		// the configured default S3 destination — empty spec re-resolves it with the right creds (the
		// common production case, e.g. OVH), rather than mis-treating it as an explicit secret-ref dest
	case d.Bucket != "":
		spec = DestinationSpec{
			Bucket: d.Bucket, Prefix: d.Prefix, Region: d.Region, Endpoint: d.Endpoint,
			UseSSL: d.UseSSL, UsePathStyle: d.UsePathStyle, SecretRef: d.SecretName,
		}
	}
	store, _, err := c.resolveStore(spec)
	return store, err
}

func (c *Coordinator) persist(ctx context.Context, in *Intent) error {
	return PutIntent(ctx, c.meta, in)
}

// BackupStatus reports a backup's current state from its durable intent.
func (c *Coordinator) BackupStatus(ctx context.Context, backupID string) (*wavespanv1.BackupState, error) {
	in, found, err := GetIntent(ctx, c.meta, backupID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("backup: no such backup %q", backupID))
	}
	return intentToState(in), nil
}

// ListBackups lists known backups from the catalog.
func (c *Coordinator) ListBackups(ctx context.Context) ([]*wavespanv1.BackupSummary, error) {
	intents, err := ListIntents(ctx, c.meta)
	if err != nil {
		return nil, err
	}
	out := make([]*wavespanv1.BackupSummary, 0, len(intents))
	for _, in := range intents {
		out = append(out, summaryFromIntent(in))
	}
	return out, nil
}

// DeleteBackup removes a backup's catalog intent AND its object-store objects, chain-aware (Phase 3d):
// it refuses to delete a backup that a live incremental child still depends on unless force cascades the
// children. It reports deleted=false for an unknown id rather than silently claiming success. The object
// store is re-resolved from the backup's persisted destination descriptor (Phase 3c Task 0), so an
// alt-destination backup is deleted in its OWN bucket.
//
// LIMITATION: an inline-credential destination cannot be re-resolved (its creds were never persisted), so
// its alt-bucket objects are not deletable here — deletion falls back to the default store (a no-op for
// those objects); the intent is still removed. Named/default/secret-ref destinations re-resolve cleanly.
func (c *Coordinator) DeleteBackup(ctx context.Context, backupID string, force bool) (bool, error) {
	store, err := c.gcStoreFor(ctx, backupID)
	if err != nil {
		return false, err
	}
	return DeleteBackup(ctx, c.meta, store, backupID, force)
}

// gcStoreFor returns the object store holding a backup's objects, re-resolved from its persisted
// destination descriptor (Phase 3c Task 0). An unknown backup falls back to the default store (the caller
// treats a missing backup as deleted=false).
func (c *Coordinator) gcStoreFor(ctx context.Context, backupID string) (ObjectStore, error) {
	in, found, err := GetIntent(ctx, c.meta, backupID)
	if err != nil {
		return nil, err
	}
	if !found {
		return c.objStore, nil
	}
	return c.storeForIntent(in)
}

// storeForIntent re-resolves the object store for a backup from its persisted destination descriptor. An
// inline-credential destination falls back to the default store (its creds were never persisted, so its
// alt bucket can't be re-resolved — documented at DeleteBackup / ReconcileOrphans).
func (c *Coordinator) storeForIntent(in *Intent) (ObjectStore, error) {
	if in.Destination.SecretName == "inline" {
		return c.objStore, nil
	}
	return c.storeForDescriptor(in.Destination)
}

// RunSweep runs the leader-gated lifecycle sweep loop until ctx is done (Phase 3d). On each tick, when
// this node leads the meta shard, it expires/retires intents (SweepIntents) and reconciles orphan
// objects (ReconcileOrphans). Both re-resolve each backup's OWN destination store from its descriptor
// (Phase 3c Task 0), so alt-destination backups are GC'd in their own bucket. Transient errors are
// logged; the next tick retries.
func (c *Coordinator) RunSweep(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	storeFor := func(in *Intent) (ObjectStore, error) { return c.storeForIntent(in) }
	// The default destination's descriptor key, so orphan reconciliation doesn't scan a default-S3 bucket
	// twice (once as the default store, once via a default-destination backup's descriptor).
	_, defDesc, _ := ResolveDestination(c.backupCfg, DestinationSpec{}, c.getenv)
	defaultKey := destinationKey(defDesc)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.isMetaLeader != nil && !c.isMetaLeader() {
				continue // only the meta-shard leader sweeps
			}
			now := c.clock.NowMs()
			stats, err := SweepIntents(ctx, c.meta, storeFor, now, c.retainMs)
			if err != nil {
				// A persistently failing sweep (e.g. objstore outage) must not be invisible.
				c.logger.Warn("backup: intent sweep failed", "err", err)
				continue
			}
			if stats.Failed > 0 || stats.Deleted > 0 {
				c.logger.Info("backup: intent sweep", "lease_expired", stats.Failed, "retention_deleted", stats.Deleted)
			}
			deleted, err := ReconcileOrphans(ctx, c.meta, ReconcileOptions{
				StoreFor:     storeFor,
				DefaultStore: c.objStore,
				DefaultKey:   defaultKey,
				NowMs:        now,
				GraceMs:      defaultReconcileGraceMs,
				Logger:       c.logger,
			})
			if err != nil {
				c.logger.Warn("backup: orphan reconciliation failed", "err", err)
				continue
			}
			if len(deleted) > 0 {
				c.logger.Info("backup: orphan reconciliation", "objects_deleted", len(deleted))
			}
		}
	}
}

// PrepareLocal serves the node-internal PrepareBackup RPC against this node's store. It derives this
// node's REAL held ranges from its HostedDataShards closure (the collection data shards it hosts, as
// "shard:<id>" tokens) so commit() can cross-check cluster coverage (F1). These held ranges travel back
// to the coordinator in PrepareBackupResult.held_ranges — the same field over the gRPC path — so a remote
// node reports its own hosted shards from its own live Manager.
func (c *Coordinator) PrepareLocal(ctx context.Context, req *wavespanv1.PrepareBackupRequest) (*wavespanv1.PrepareBackupResult, error) {
	if c.localStore == nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("backup: node holds no local store"))
	}
	var held []string
	if c.hostedShards != nil {
		held = formatHeldShards(c.hostedShards())
	}
	pr, err := c.agent.Prepare(ctx, c.localStore, req.GetBackupId(), req.GetFrontierT(), held)
	if err != nil {
		return nil, err
	}
	return &wavespanv1.PrepareBackupResult{GlobalSeq: pr.GlobalSeq, HeldRanges: pr.HeldRanges}, nil
}

// ExportLocal serves the node-internal ExportBackup RPC against this node's store (Phase 3c Task 0). The
// node resolves its OWN destination from the request (named/default from its config/env, secret-ref from
// its env, inline creds from the request — transient), resolves its OWN parent checkpoint from
// parent_backup_id in that store, exports, and reports its storage identity. So real incrementals and
// alt destinations work for every node, not just the coordinator's.
func (c *Coordinator) ExportLocal(ctx context.Context, req *wavespanv1.ExportBackupRequest) (*wavespanv1.ExportBackupResult, error) {
	if c.localStore == nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("backup: node holds no local store"))
	}
	store, _, err := c.resolveStore(destinationSpecFromProto(req.GetDestination()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	parentCkpt, err := resolveParentCheckpoint(store, req.GetParentBackupId(), c.self)
	if err != nil {
		return nil, err
	}
	res, err := c.agent.Export(ctx, c.localStore, store, req.GetBackupId(), c.self,
		selectorFromProto(req.GetAssignment()), planesFromProto(req.GetPlanes()), req.GetFrontierT(), parentCkpt)
	if err != nil {
		return nil, err
	}
	return &wavespanv1.ExportBackupResult{
		Objects:        res.Objects,
		Bytes:          res.Bytes,
		SubManifestKey: res.SubManifestKey,
		StorageUuid:    res.StorageUUID,
	}, nil
}

// upsertNode merges a node record into the intent's PerNode list by member id, preserving the held-range
// summary captured at prepare time when a later (export) record carries none.
func upsertNode(in *Intent, rec NodeRecord) {
	for i := range in.PerNode {
		if in.PerNode[i].MemberID == rec.MemberID {
			if len(rec.HeldRanges) == 0 {
				rec.HeldRanges = in.PerNode[i].HeldRanges
			}
			in.PerNode[i] = rec
			return
		}
	}
	in.PerNode = append(in.PerNode, rec)
}

func sortedMembers(m map[string]Selector) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func newBackupID(nowMs int64) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("bk-%013d-%s", nowMs, hex.EncodeToString(b[:]))
}
