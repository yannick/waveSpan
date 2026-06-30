package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"time"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// learnerAdmitTarget is the subset of Manager the AdmitLearner RPC needs (admit a learner to a shard
// this node hosts).
type learnerAdmitTarget interface {
	AddLearner(ctx context.Context, shardID, replicaID uint64, addr string) error
}

// Service is the public CollectionService Connect handler over the typed Collections engine
// (design/30 §13). Writes go through the owning shard's leader; reads honour the request's
// linearizable flag. WRONGTYPE maps to FailedPrecondition.
type Service struct {
	cols   *Collections
	self   membership.Member
	admit  learnerAdmitTarget
	tier   *tierStatus       // optional: backs the TierInfo operator view
	backup backupCoordinator // optional: backs the BackupService (design/backup phase 3a)
}

// tierStatus is this node's static collections placement, paired with the Manager for live status.
type tierStatus struct {
	mgr           *Manager
	raftAddr      string
	selfReplicaID uint64
	voter         bool
}

// NewService wires the CollectionService Connect handler.
func NewService(cols *Collections, self membership.Member) *Service {
	return &Service{cols: cols, self: self}
}

// WithLearnerAdmit enables the AdmitLearner RPC: this node will admit learners to shards it hosts (the
// server side of demand-fill, design/30 §9).
func (s *Service) WithLearnerAdmit(a learnerAdmitTarget) *Service {
	s.admit = a
	return s
}

// WithTierStatus backs the TierInfo RPC with this node's placement and Manager (active tunables +
// per-shard leader status).
func (s *Service) WithTierStatus(mgr *Manager, raftAddr string, selfReplicaID uint64, voter bool) *Service {
	s.tier = &tierStatus{mgr: mgr, raftAddr: raftAddr, selfReplicaID: selfReplicaID, voter: voter}
	return s
}

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *Service) Handler() (string, http.Handler) {
	return wavespanv1connect.NewCollectionServiceHandler(s, rpcopts.Handler()...)
}

// BudgetHandler returns the mountable Connect handler (path, handler) for the LeasedBudget escrow API
// (design/35). The same *Service backs both CollectionService and BudgetService — one engine, one error
// mapper (collErr) — so budget RPCs reuse the leader-routing, WRONGTYPE, and ResponseMeta plumbing.
func (s *Service) BudgetHandler() (string, http.Handler) {
	return wavespanv1connect.NewBudgetServiceHandler(s, rpcopts.Handler()...)
}

func (s *Service) meta() *wavespanv1.ResponseMeta {
	return &wavespanv1.ResponseMeta{
		ServedByClusterId: s.self.ClusterID,
		ServedByMemberId:  s.self.MemberID,
		Source:            wavespanv1.ReadSource_LOCAL_DURABLE,
		ConflictState:     wavespanv1.ConflictState_CONFLICT_NONE,
		Completeness:      wavespanv1.Completeness_COMPLETE,
		ObservedAtUnixMs:  time.Now().UnixMilli(),
	}
}

func collErr(err error) error {
	switch {
	case errors.Is(err, ErrWrongType):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrNotNumber):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrBusy), errors.Is(err, ErrDiskPressure):
		// Load shed (queue full / dragonboat busy) or disk-pressure shed: both are transient backpressure —
		// the client should retry with backoff. Connect's ResourceExhausted maps to gRPC ResourceExhausted
		// (codes 8 in both), so the write is rejected cleanly without the node ever crashing or filling its
		// volume.
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, ErrNoCapacity):
		// LeasedBudget (design/35): a STRICT pool with nothing left to grant — the caller asked for more
		// than exists. ResourceExhausted mirrors the load-shed semantics: the resource (budget) is depleted.
		return connect.NewError(connect.CodeResourceExhausted, err)
	case errors.Is(err, ErrBudgetExists), errors.Is(err, ErrBudgetNotFound), errors.Is(err, ErrLeaseUnknown):
		// Precondition violations: defining an existing pool, or grant/report/return against a missing
		// pool/lease (B9). FailedPrecondition, like WRONGTYPE.
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, ErrUnsupportedMode), errors.Is(err, ErrBudgetBadParam):
		// B3/B4: a non-STRICT mode or an invalid cap at define time; or (2a.3) an out-of-bounds
		// pacing/timing param at define/grant time — a bad argument, not a transient fault.
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, ErrWrongHolder):
		// Stage-2.x: a Report/Return whose bound holder_id does not match the lease's grantee — first-party
		// tampering or a nodeID collision. PermissionDenied: the caller is not authorized for this lease.
		return connect.NewError(connect.CodePermissionDenied, err)
	case errors.Is(err, ErrLeaseSettled):
		// 2b.3/§3.7: the grant's lease_id was already settled (returned/expired) and is tombstoned —
		// it is never re-granted. AlreadyExists tells the node-side lease cache (§4.2) to mint a fresh
		// lease_id (rotate its nonce) rather than retry the same id, which would keep hitting the
		// tombstone. Distinct from the missing-pool/lease FailedPrecondition above.
		return connect.NewError(connect.CodeAlreadyExists, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func (s *Service) count(n uint64, err error) (*connect.Response[wavespanv1.CountResult], error) {
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.CountResult{Meta: s.meta(), Count: n}), nil
}

// write builds a mutation carrying the idempotency key and proposes it (forwarding to the leader if
// this node isn't it). idem == "" means no dedup.
func (s *Service) write(ctx context.Context, op opKind, ns, coll []byte, idem string, items []item) (*connect.Response[wavespanv1.CountResult], error) {
	return s.count(s.cols.proposeCount(ctx, command{Op: op, NS: ns, Coll: coll, Idem: []byte(idem), Items: items}))
}

// --- Set ---

// SAdd adds set members (optionally with a TTL).
func (s *Service) SAdd(ctx context.Context, req *connect.Request[wavespanv1.SAddRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	items := itemsFromKeys(m.GetMembers())
	if m.TtlMs != nil {
		exp := time.Now().UnixMilli() + m.GetTtlMs()
		for i := range items {
			items[i].ExpiryMs = exp
		}
	}
	return s.write(ctx, opSAdd, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// SRem removes set members.
func (s *Service) SRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opSRem, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
}

// SIsMember reports set membership.
func (s *Service) SIsMember(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.BoolResult], error) {
	m := req.Msg
	ok, err := s.cols.SIsMember(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.BoolResult{Meta: s.meta(), Value: ok}), nil
}

// SCard returns the set cardinality.
func (s *Service) SCard(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.SCard(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// SMembers enumerates set members.
func (s *Service) SMembers(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.MembersResult], error) {
	m := req.Msg
	out, err := s.cols.SMembers(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.MembersResult{Meta: s.meta(), Members: out}), nil
}

// --- Hash ---

// HSet sets hash fields.
func (s *Service) HSet(ctx context.Context, req *connect.Request[wavespanv1.HSetRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	items := make([]item, len(m.GetFields()))
	for i, f := range m.GetFields() {
		items[i] = item{Key: f.GetField(), Val: f.GetValue()}
	}
	return s.write(ctx, opHSet, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// HDel deletes hash fields.
func (s *Service) HDel(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opHDel, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
}

// HIncrBy atomically increments an integer hash field and returns the new value.
func (s *Service) HIncrBy(ctx context.Context, req *connect.Request[wavespanv1.HIncrByRequest]) (*connect.Response[wavespanv1.Int64Result], error) {
	m := req.Msg
	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, uint64(m.GetDelta()))
	_, data, err := s.cols.proposeCmd(ctx, command{Op: opHIncrBy, NS: []byte(m.GetNamespace()), Coll: m.GetCollection(),
		Idem: []byte(m.GetIdempotencyKey()), Items: []item{{Key: m.GetField(), Val: d}}})
	if err != nil {
		return nil, collErr(err)
	}
	if len(data) != 8 {
		return nil, collErr(ErrNotNumber)
	}
	return connect.NewResponse(&wavespanv1.Int64Result{Meta: s.meta(), Value: int64(binary.BigEndian.Uint64(data))}), nil
}

// HIncrByFloat atomically increments a float hash field and returns the new value.
func (s *Service) HIncrByFloat(ctx context.Context, req *connect.Request[wavespanv1.HIncrByFloatRequest]) (*connect.Response[wavespanv1.DoubleResult], error) {
	m := req.Msg
	_, data, err := s.cols.proposeCmd(ctx, command{Op: opHIncrByFloat, NS: []byte(m.GetNamespace()), Coll: m.GetCollection(),
		Idem: []byte(m.GetIdempotencyKey()), Items: []item{{Key: m.GetField(), Score: m.GetDelta()}}})
	if err != nil {
		return nil, collErr(err)
	}
	if len(data) != 8 {
		return nil, collErr(ErrNotNumber)
	}
	return connect.NewResponse(&wavespanv1.DoubleResult{Meta: s.meta(), Value: math.Float64frombits(binary.BigEndian.Uint64(data))}), nil
}

// HGet returns a hash field value.
func (s *Service) HGet(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.ValueResult], error) {
	m := req.Msg
	v, found, err := s.cols.HGet(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.ValueResult{Meta: s.meta(), Found: found, Value: v}), nil
}

// HLen returns the number of hash fields.
func (s *Service) HLen(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.HLen(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// HGetAll returns all hash field/value pairs.
func (s *Service) HGetAll(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.FieldsResult], error) {
	m := req.Msg
	out, err := s.cols.HGetAll(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	fields := make([]*wavespanv1.FieldValue, len(out))
	for i, f := range out {
		fields[i] = &wavespanv1.FieldValue{Field: f.Field, Value: f.Value}
	}
	return connect.NewResponse(&wavespanv1.FieldsResult{Meta: s.meta(), Fields: fields}), nil
}

// --- Sorted set ---

// ZAdd adds or updates sorted-set members.
func (s *Service) ZAdd(ctx context.Context, req *connect.Request[wavespanv1.ZAddRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	items := make([]item, len(m.GetMembers()))
	for i, sm := range m.GetMembers() {
		items[i] = item{Key: sm.GetMember(), Score: sm.GetScore()}
	}
	return s.write(ctx, opZAdd, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), items)
}

// ZRem removes sorted-set members.
func (s *Service) ZRem(ctx context.Context, req *connect.Request[wavespanv1.KeysRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.write(ctx, opZRem, []byte(m.GetNamespace()), m.GetCollection(), m.GetIdempotencyKey(), itemsFromKeys(m.GetKeys()))
}

// ZScore returns a member's score.
func (s *Service) ZScore(ctx context.Context, req *connect.Request[wavespanv1.MemberRequest]) (*connect.Response[wavespanv1.ScoreResult], error) {
	m := req.Msg
	sc, found, err := s.cols.ZScore(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetMember(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.ScoreResult{Meta: s.meta(), Found: found, Score: sc}), nil
}

// ZCard returns the sorted-set cardinality.
func (s *Service) ZCard(ctx context.Context, req *connect.Request[wavespanv1.CardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	return s.count(s.cols.ZCard(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetLinearizable()))
}

// ZRange returns members in ascending score order.
func (s *Service) ZRange(ctx context.Context, req *connect.Request[wavespanv1.RangeRequest]) (*connect.Response[wavespanv1.ScoredMembersResult], error) {
	m := req.Msg
	out, err := s.cols.ZRange(ctx, []byte(m.GetNamespace()), m.GetCollection(), int(m.GetLimit()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	members := make([]*wavespanv1.ScoredMember, len(out))
	for i, sm := range out {
		members[i] = &wavespanv1.ScoredMember{Member: sm.Member, Score: sm.Score}
	}
	return connect.NewResponse(&wavespanv1.ScoredMembersResult{Meta: s.meta(), Members: members}), nil
}

// BulkRemove removes a list of members from many collections at once (best-effort fan-out, design/30
// §13.7). collections empty = every collection in the namespace.
func (s *Service) BulkRemove(ctx context.Context, req *connect.Request[wavespanv1.BulkRemoveRequest]) (*connect.Response[wavespanv1.BulkRemoveResult], error) {
	m := req.Msg
	entries, err := s.cols.BulkRemove(ctx, []byte(m.GetNamespace()), m.GetCollections(), m.GetMembers())
	if err != nil {
		return nil, collErr(err)
	}
	out := make([]*wavespanv1.BulkRemoveEntry, len(entries))
	for i, e := range entries {
		msg := ""
		if e.Err != nil {
			msg = e.Err.Error()
		}
		out[i] = &wavespanv1.BulkRemoveEntry{Collection: e.Collection, Removed: e.Removed, Error: msg}
	}
	return connect.NewResponse(&wavespanv1.BulkRemoveResult{Meta: s.meta(), Results: out}), nil
}

// ListCollections enumerates every collection in a namespace with its datatype, gathered best-effort
// across data shards (design/30 §13.7).
func (s *Service) ListCollections(ctx context.Context, req *connect.Request[wavespanv1.ListCollectionsRequest]) (*connect.Response[wavespanv1.ListCollectionsResult], error) {
	m := req.Msg
	infos, err := s.cols.ListCollectionInfos(ctx, []byte(m.GetNamespace()), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	out := make([]*wavespanv1.CollectionInfo, len(infos))
	for i, ci := range infos {
		out[i] = &wavespanv1.CollectionInfo{Name: ci.Name, Type: ci.Type.String()}
	}
	return connect.NewResponse(&wavespanv1.ListCollectionsResult{Meta: s.meta(), Collections: out}), nil
}

// TierInfo reports this node's consensus-tier placement, active tunables, and per-shard leader status.
func (s *Service) TierInfo(_ context.Context, _ *connect.Request[wavespanv1.TierInfoRequest]) (*connect.Response[wavespanv1.TierInfoResult], error) {
	res := &wavespanv1.TierInfoResult{Meta: s.meta(), Enabled: true}
	if s.tier == nil || s.tier.mgr == nil {
		return connect.NewResponse(res), nil // tier up, but status not wired
	}
	t := s.tier
	tun := t.mgr.Tunables()
	res.NodeHostId = t.mgr.NodeHostID()
	res.RaftAddress = t.raftAddr
	res.SelfReplicaId = t.selfReplicaID
	res.Voter = t.voter
	res.RttMs = tun.RTTMillisecond
	res.ElectionRtt = tun.ElectionRTT
	res.HeartbeatRtt = tun.HeartbeatRTT
	res.SnapshotEntries = tun.SnapshotEntries
	res.CompactionOverhead = tun.CompactionOverhead
	res.SweepMs = uint64(tun.SweepEvery / time.Millisecond)
	for _, st := range t.mgr.ShardStatuses() {
		res.Shards = append(res.Shards, &wavespanv1.ShardStatus{
			ShardId: st.ShardID, ReplicaId: st.ReplicaID, LeaderReplicaId: st.LeaderReplicaID,
			HasLeader: st.HasLeader, IsLeader: st.IsLeader, IsData: st.IsData,
		})
	}
	return connect.NewResponse(res), nil
}

// --- Leased budget (design/35, Stage 1) ---

// budStatResult builds a BudgetStatResult from the pool accounting plus this node's ResponseMeta.
func (s *Service) budStatResult(st BudStat) *wavespanv1.BudgetStatResult {
	return &wavespanv1.BudgetStatResult{
		Meta:               s.meta(),
		Exists:             st.Exists,
		CapUnits:           st.Cap,
		AvailableUnits:     st.Available,
		LeasedOutUnits:     st.LeasedOut,
		SpentUnits:         st.Spent,
		SpentReportedUnits: st.SpentReported,
		Epoch:              st.Epoch,
		Mode:               wavespanv1.BudgetMode(st.Mode),
	}
}

// BudgetDefine creates a STRICT escrow pool and returns the resulting pool accounting.
func (s *Service) BudgetDefine(ctx context.Context, req *connect.Request[wavespanv1.BudgetDefineRequest]) (*connect.Response[wavespanv1.BudgetStatResult], error) {
	m := req.Msg
	ns, coll := []byte(m.GetNamespace()), m.GetBudget()
	cfg := BudgetConfig{
		Rate:               m.GetRateUnitsPerSec(),
		Burst:              m.GetBurstUnits(),
		SelfGuardMs:        m.GetSelfGuardMs(),
		MaxPauseMs:         m.GetMaxPauseMs(),
		DefaultTTLMs:       m.GetDefaultTtlMs(),
		DedupRetryWindowMs: m.GetDedupRetryWindowMs(),
	}
	if err := s.cols.BudgetDefine(ctx, ns, coll, m.GetCapUnits(), uint8(m.GetMode()), cfg); err != nil {
		return nil, collErr(err)
	}
	st, err := s.cols.BudgetStat(ctx, ns, coll, false)
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(s.budStatResult(st)), nil
}

// BudgetGrant atomically leases up to amount_units. A depleted STRICT pool is reported as a normal
// result with no_capacity set (not an error), so callers can distinguish "nothing right now" from a fault.
func (s *Service) BudgetGrant(ctx context.Context, req *connect.Request[wavespanv1.BudgetGrantRequest]) (*connect.Response[wavespanv1.BudgetGrantResult], error) {
	m := req.Msg
	// ttl_ms is the per-grant TTL override (0 ⇒ the budget's DefaultTTLMs); self_guard/max_pause come
	// from the budget config and ride back on the result echo below.
	gr, err := s.cols.BudgetGrant(ctx, []byte(m.GetNamespace()), m.GetBudget(), []byte(m.GetHolderId()), m.GetAmountUnits(), m.GetLeaseId(), m.GetTtlMs())
	if err != nil {
		if errors.Is(err, ErrNoCapacity) {
			return connect.NewResponse(&wavespanv1.BudgetGrantResult{Meta: s.meta(), NoCapacity: true}), nil
		}
		return nil, collErr(err)
	}
	// Echo the effective leader-stamped timing so the node lease cache can run its freshness gate, stamp
	// its suspend-aware deadline, and set the self-fence (§5 M-4). Zero for a non-timed grant.
	return connect.NewResponse(&wavespanv1.BudgetGrantResult{
		Meta: s.meta(), GrantedUnits: gr.Granted, Partial: gr.Granted < m.GetAmountUnits(),
		GrantedMs: gr.GrantedMs, TtlMs: gr.TTLMs, SelfGuardMs: gr.SelfGuardMs, MaxPauseBudgetMs: gr.MaxPauseMs,
	}), nil
}

// BudgetReport folds a cumulative-per-lease spent total and returns the pool accounting.
func (s *Service) BudgetReport(ctx context.Context, req *connect.Request[wavespanv1.BudgetReportRequest]) (*connect.Response[wavespanv1.BudgetStatResult], error) {
	m := req.Msg
	ns, coll := []byte(m.GetNamespace()), m.GetBudget()
	if err := s.cols.BudgetReport(ctx, ns, coll, m.GetLeaseId(), []byte(m.GetHolderId()), m.GetSpentCumulative()); err != nil {
		return nil, collErr(err)
	}
	st, err := s.cols.BudgetStat(ctx, ns, coll, false)
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(s.budStatResult(st)), nil
}

// BudgetReturn releases a lease's unspent remainder and returns the pool accounting.
func (s *Service) BudgetReturn(ctx context.Context, req *connect.Request[wavespanv1.BudgetReturnRequest]) (*connect.Response[wavespanv1.BudgetStatResult], error) {
	m := req.Msg
	ns, coll := []byte(m.GetNamespace()), m.GetBudget()
	if err := s.cols.BudgetReturn(ctx, ns, coll, m.GetLeaseId(), []byte(m.GetHolderId()), m.GetSpentCumulative()); err != nil {
		return nil, collErr(err)
	}
	st, err := s.cols.BudgetStat(ctx, ns, coll, false)
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(s.budStatResult(st)), nil
}

// BudgetReconcile re-credits a budget to its authoritative external Σ-acked spend (§3.8), recovering the
// headroom a forced lease expiry stranded as underspend — without overspend. Controller/admin surface; it
// is mounted on the gRPC data plane and the Connect admin port, NOT the read-only HTTP gateway. The result
// carries the recovered amount (old spent - new spent) alongside the post-reconcile pool accounting.
func (s *Service) BudgetReconcile(ctx context.Context, req *connect.Request[wavespanv1.BudgetReconcileRequest]) (*connect.Response[wavespanv1.BudgetStatResult], error) {
	m := req.Msg
	ns, coll := []byte(m.GetNamespace()), m.GetBudget()
	recovered, err := s.cols.BudgetReconcile(ctx, ns, coll, m.GetTrueAckedUnits())
	if err != nil {
		return nil, collErr(err)
	}
	st, err := s.cols.BudgetStat(ctx, ns, coll, false)
	if err != nil {
		return nil, collErr(err)
	}
	res := s.budStatResult(st)
	res.RecoveredUnits = recovered
	return connect.NewResponse(res), nil
}

// BudgetStat reads the pool accounting (bounded-stale unless linearizable).
func (s *Service) BudgetStat(ctx context.Context, req *connect.Request[wavespanv1.BudgetStatRequest]) (*connect.Response[wavespanv1.BudgetStatResult], error) {
	m := req.Msg
	st, err := s.cols.BudgetStat(ctx, []byte(m.GetNamespace()), m.GetBudget(), m.GetLinearizable())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(s.budStatResult(st)), nil
}

// ProposeForward applies a write forwarded by a peer that was not the shard's leader (node-side leader
// routing, design/30 §13.13). It commits locally only — if this node also isn't the leader, the error
// propagates and the forwarder tries the next peer.
func (s *Service) ProposeForward(ctx context.Context, req *connect.Request[wavespanv1.ProposeForwardRequest]) (*connect.Response[wavespanv1.CountResult], error) {
	m := req.Msg
	n, data, err := s.cols.ProposeRaw(ctx, []byte(m.GetNamespace()), m.GetCollection(), m.GetCommand())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.CountResult{Meta: s.meta(), Count: n, Data: data}), nil
}

// AdmitLearner admits a peer as a non-voting learner of a shard this node hosts (demand-fill server).
func (s *Service) AdmitLearner(ctx context.Context, req *connect.Request[wavespanv1.AdmitLearnerRequest]) (*connect.Response[wavespanv1.AdmitLearnerResponse], error) {
	if s.admit == nil {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("collections: this node hosts no shards"))
	}
	m := req.Msg
	if err := s.admit.AddLearner(ctx, m.GetShardId(), m.GetReplicaId(), m.GetTarget()); err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.AdmitLearnerResponse{Meta: s.meta()}), nil
}
