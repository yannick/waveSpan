package backup

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/storage"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Roster reports the currently live cluster members (by member id).
type Roster interface {
	Live() []string
}

// ExportRequest is the coordinator's instruction to one node to export its assignment.
type ExportRequest struct {
	BackupID   string
	MemberID   string
	Assignment Selector
	Planes     []Plane
	FrontierT  int64
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
	Self       string            // this node's member id
	Meta       MetaStore         // durable intent catalog (the meta shard)
	ObjStore   ObjectStore       // backup destination (also used by the local agent)
	Roster     Roster            // live membership
	ClientFor  ClientFactory     // dial a node's BackupService
	Assigner   Assigner          // ownership/assignment plan
	Clock      Clock             // nil = real time
	LocalStore storage.LocalStore // this node's store, for the node-internal Prepare/Export RPCs
	Registry   *Registry         // export contributor registry (nil = DefaultRegistry)
	LeaseMs    int64             // intent lease window (0 = default)
}

// Coordinator drives a consistent cluster backup: it records a durable BackupIntent in the meta shard,
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
	localStore storage.LocalStore
	agent      *Agent
	leaseMs    int64

	// failAfterPhase, when set (test hook), makes run() persist the named phase's completion and then
	// return early — simulating coordinator loss for the resumability test.
	failAfterPhase Phase
}

const defaultLeaseMs = 10 * 60 * 1000 // 10 minutes

// frontierLeaseMs is the small lease added to now when picking the backup frontier T (spec §2 Begin:
// T = HLC.now()+lease). In 3a T is recorded for provenance only and not yet used as a Version<=T cut
// (the cluster-wide HLC sealing is Phase 3a.1); the now+lease value is set here so the contract is right
// when the cut lands.
const frontierLeaseMs = 5 * 1000 // 5 seconds

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
	return &Coordinator{
		self:       cfg.Self,
		meta:       cfg.Meta,
		objStore:   cfg.ObjStore,
		roster:     cfg.Roster,
		clientFor:  cfg.ClientFor,
		assigner:   cfg.Assigner,
		clock:      clk,
		localStore: cfg.LocalStore,
		agent:      NewAgent(cfg.Registry),
		leaseMs:    lease,
	}
}

// BeginBackup records a durable intent, picks a frontier, and drives the phased backup to completion
// (synchronous in 3a). It returns the allocated backup id; the intent is the source of truth a resuming
// coordinator reads if this one is lost mid-flight.
func (c *Coordinator) BeginBackup(ctx context.Context, spec *wavespanv1.BackupSpec) (string, error) {
	now := c.clock.NowMs()
	id := newBackupID(now)
	in := &BackupIntent{
		SchemaVersion:      intentSchemaVersion,
		BackupID:           id,
		FrontierT:          now + frontierLeaseMs,
		CaptureWallClockMs: now,
		Selection:          selectorFromProto(spec.GetSelection()),
		Planes:             planesFromProto(spec.GetPlanes()),
		Parent:             spec.GetParent(),
		Destination:        descriptorFromProto(spec.GetDestination()),
		Status:             StatusRunning,
		Phase:              PhaseAssign,
		LeaseDeadlineMs:    now + c.leaseMs,
		StartedMs:          now,
	}
	if len(in.Planes) == 0 {
		in.Planes = []Plane{PlaneLogical}
	}
	if err := c.persist(ctx, in); err != nil {
		return "", err
	}
	if err := c.run(ctx, in); err != nil {
		return id, err
	}
	return id, nil
}

// run advances a backup from its current phase to commit, persisting the intent at every phase boundary
// so a resuming coordinator can pick up exactly where this one left off.
func (c *Coordinator) run(ctx context.Context, in *BackupIntent) error {
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
		if err := c.exportAll(ctx, in); err != nil {
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
		return c.commit(ctx, in)
	}
	return nil
}

// prepareAll calls PrepareBackup on every assigned member (sequentially in 3a) and records the ranges
// they report holding.
func (c *Coordinator) prepareAll(ctx context.Context, in *BackupIntent) error {
	for _, mid := range sortedMembers(in.Assignments) {
		cl, err := c.clientFor(mid)
		if err != nil {
			return err
		}
		pr, err := cl.Prepare(ctx, in.BackupID, in.FrontierT)
		if err != nil {
			return err
		}
		upsertNode(in, NodeRecord{MemberID: mid, Phase: PhasePrepare, HeldRanges: pr.HeldRanges})
	}
	return nil
}

// exportAll calls ExportBackup on every assigned member (sequentially in 3a) and records each node's
// export result.
func (c *Coordinator) exportAll(ctx context.Context, in *BackupIntent) error {
	for _, mid := range sortedMembers(in.Assignments) {
		cl, err := c.clientFor(mid)
		if err != nil {
			return err
		}
		res, err := cl.Export(ctx, ExportRequest{
			BackupID:   in.BackupID,
			MemberID:   mid,
			Assignment: in.Assignments[mid],
			Planes:     in.Planes,
			FrontierT:  in.FrontierT,
		})
		if err != nil {
			return err
		}
		upsertNode(in, NodeRecord{
			MemberID:       mid,
			Phase:          PhaseExport,
			Objects:        res.Objects,
			Bytes:          res.Bytes,
			SubManifestKey: res.SubManifestKey,
			StorageUUID:    res.StorageUUID,
			Done:           true,
		})
	}
	return nil
}

// commit writes the cluster.manifest and finalizes the intent status. A backup with enumerated gaps is
// committed PARTIAL — never silently COMPLETE.
//
// 3a: PARTIAL is driven solely by the assigner's gap list (ranges it found with no live holder). The
// HeldRanges plumbed via Prepare→NodeRecord is NOT yet compared against the assignments here — that
// held-range-vs-assignment coverage cross-check (catching real cluster gaps a node failed to cover)
// lands in 3a.1. HeldRanges is recorded now so that cross-check has its input when it ships.
func (c *Coordinator) commit(ctx context.Context, in *BackupIntent) error {
	refs := make([]PerNodeRef, 0, len(in.PerNode))
	topo := make([]TopologyEntry, 0, len(in.PerNode))
	for _, n := range in.PerNode {
		refs = append(refs, PerNodeRef{MemberID: n.MemberID, Ref: n.SubManifestKey, Objects: n.Objects, Bytes: n.Bytes})
		topo = append(topo, TopologyEntry{MemberID: n.MemberID, StorageUUID: n.StorageUUID})
	}
	status := StatusComplete
	if len(in.Gaps) > 0 {
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
	if err := WriteClusterManifest(c.objStore, cm); err != nil {
		return err
	}
	in.Status = status
	in.Phase = PhaseCommit
	in.FinishedMs = c.clock.NowMs()
	return c.persist(ctx, in)
}

// resume loads a persisted intent and continues driving it from its recorded phase (Task 5). It is safe
// to re-run because object keys are deterministic — a re-export overwrites identical content.
func (c *Coordinator) resume(ctx context.Context, backupID string) error {
	in, found, err := GetIntent(ctx, c.meta, backupID)
	if err != nil {
		return err
	}
	if !found {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("backup: no intent for %q", backupID))
	}
	return c.run(ctx, in)
}

func (c *Coordinator) persist(ctx context.Context, in *BackupIntent) error {
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
		out = append(out, &wavespanv1.BackupSummary{
			BackupId:   in.BackupID,
			Status:     statusToProto(in.Status),
			StartedMs:  in.StartedMs,
			FinishedMs: in.FinishedMs,
			Parent:     in.Parent,
		})
	}
	return out, nil
}

// DeleteBackup removes a backup's catalog intent (object GC is Phase 3d). It reports deleted=false for an
// unknown id rather than silently claiming success — DeleteBlob on a missing key is a no-op, so the
// truthful answer requires an existence check first.
func (c *Coordinator) DeleteBackup(ctx context.Context, backupID string) (bool, error) {
	_, found, err := GetIntent(ctx, c.meta, backupID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if err := DeleteIntent(ctx, c.meta, backupID); err != nil {
		return false, err
	}
	return true, nil
}

// PrepareLocal serves the node-internal PrepareBackup RPC against this node's store.
func (c *Coordinator) PrepareLocal(ctx context.Context, req *wavespanv1.PrepareBackupRequest) (*wavespanv1.PrepareBackupResult, error) {
	if c.localStore == nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("backup: node holds no local store"))
	}
	pr, err := c.agent.Prepare(ctx, c.localStore, req.GetBackupId(), req.GetFrontierT(), nil)
	if err != nil {
		return nil, err
	}
	return &wavespanv1.PrepareBackupResult{GlobalSeq: pr.GlobalSeq, HeldRanges: pr.HeldRanges}, nil
}

// ExportLocal serves the node-internal ExportBackup RPC against this node's store.
func (c *Coordinator) ExportLocal(ctx context.Context, req *wavespanv1.ExportBackupRequest) (*wavespanv1.ExportBackupResult, error) {
	if c.localStore == nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("backup: node holds no local store"))
	}
	res, err := c.agent.Export(ctx, c.localStore, c.objStore, req.GetBackupId(), c.self,
		selectorFromProto(req.GetAssignment()), planesFromProto(req.GetPlanes()), req.GetFrontierT())
	if err != nil {
		return nil, err
	}
	return &wavespanv1.ExportBackupResult{Objects: res.Objects, Bytes: res.Bytes, SubManifestKey: res.SubManifestKey}, nil
}

// upsertNode merges a node record into the intent's PerNode list by member id, preserving the held-range
// summary captured at prepare time when a later (export) record carries none.
func upsertNode(in *BackupIntent, rec NodeRecord) {
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
