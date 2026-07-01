package membership

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// --- proto conversions ---

func memberToProto(m Member) *wavespanv1.Member {
	return &wavespanv1.Member{
		ClusterId: m.ClusterID, MemberId: m.MemberID, StorageUuid: m.StorageUUID,
		NodeName: m.NodeName, Zone: m.Zone, Region: m.Region, Geo: m.Geo,
		GossipAddr: m.GossipAddr, DataAddr: m.DataAddr, AdminAddr: m.AdminAddr,
	}
}

func memberFromProto(p *wavespanv1.Member) Member {
	if p == nil {
		return Member{}
	}
	return Member{
		ClusterID: p.GetClusterId(), MemberID: p.GetMemberId(), StorageUUID: p.GetStorageUuid(),
		NodeName: p.GetNodeName(), Zone: p.GetZone(), Region: p.GetRegion(), Geo: p.GetGeo(),
		GossipAddr: p.GetGossipAddr(), DataAddr: p.GetDataAddr(), AdminAddr: p.GetAdminAddr(),
	}
}

func stateToProto(s State) wavespanv1.MemberLiveness {
	switch s {
	case StateAlive:
		return wavespanv1.MemberLiveness_MEMBER_ALIVE
	case StateSuspect:
		return wavespanv1.MemberLiveness_MEMBER_SUSPECT
	case StateUnreachable:
		return wavespanv1.MemberLiveness_MEMBER_UNREACHABLE
	case StateDead:
		return wavespanv1.MemberLiveness_MEMBER_DEAD
	case StateForgotten:
		return wavespanv1.MemberLiveness_MEMBER_FORGOTTEN
	default:
		return wavespanv1.MemberLiveness_MEMBER_LIVENESS_UNSPECIFIED
	}
}

func stateFromProto(p wavespanv1.MemberLiveness) State {
	switch p {
	case wavespanv1.MemberLiveness_MEMBER_SUSPECT:
		return StateSuspect
	case wavespanv1.MemberLiveness_MEMBER_UNREACHABLE:
		return StateUnreachable
	case wavespanv1.MemberLiveness_MEMBER_DEAD:
		return StateDead
	case wavespanv1.MemberLiveness_MEMBER_FORGOTTEN:
		return StateForgotten
	default:
		return StateAlive
	}
}

// MemberStateToProto converts a roster view to its wire form (also used by the admin handler).
func MemberStateToProto(v MemberView) *wavespanv1.MemberState {
	return &wavespanv1.MemberState{
		Member: memberToProto(v.Member), State: stateToProto(v.State),
		Incarnation: v.Incarnation, LastSeenUnixMs: v.LastSeenMs,
	}
}

func memberStateFromProto(p *wavespanv1.MemberState) MemberView {
	return MemberView{
		Member: memberFromProto(p.GetMember()), State: stateFromProto(p.GetState()),
		Incarnation: p.GetIncarnation(), LastSeenMs: p.GetLastSeenUnixMs(),
	}
}

func summariesToProto(ss []HolderSummaryWire) []*wavespanv1.HolderSummary {
	if len(ss) == 0 {
		return nil
	}
	out := make([]*wavespanv1.HolderSummary, 0, len(ss))
	for _, s := range ss {
		out = append(out, &wavespanv1.HolderSummary{MemberId: s.MemberID, BloomFilter: s.Bloom, HllSketch: s.HLL, ApproximateKeyCount: s.ApproxKeys, Namespaces: s.Namespaces, GeneratedAtUnixMs: s.GeneratedAtUnixMs})
	}
	return out
}

func summariesFromProto(ps []*wavespanv1.HolderSummary) []HolderSummaryWire {
	if len(ps) == 0 {
		return nil
	}
	out := make([]HolderSummaryWire, 0, len(ps))
	for _, p := range ps {
		out = append(out, HolderSummaryWire{MemberID: p.GetMemberId(), Bloom: p.GetBloomFilter(), HLL: p.GetHllSketch(), ApproxKeys: p.GetApproximateKeyCount(), Namespaces: p.GetNamespaces(), GeneratedAtUnixMs: p.GetGeneratedAtUnixMs()})
	}
	return out
}

func configDeltasToProto(ds []ConfigDeltaWire) []*wavespanv1.ConfigDelta {
	if len(ds) == 0 {
		return nil
	}
	out := make([]*wavespanv1.ConfigDelta, 0, len(ds))
	for _, d := range ds {
		out = append(out, &wavespanv1.ConfigDelta{Key: d.Key, Value: d.Value, Version: d.Version, Origin: d.Origin})
	}
	return out
}

func configDeltasFromProto(ds []*wavespanv1.ConfigDelta) []ConfigDeltaWire {
	if len(ds) == 0 {
		return nil
	}
	out := make([]ConfigDeltaWire, 0, len(ds))
	for _, d := range ds {
		out = append(out, ConfigDeltaWire{Key: d.GetKey(), Value: d.GetValue(), Version: d.GetVersion(), Origin: d.GetOrigin()})
	}
	return out
}

func heldBucketsToProto(bs []HeldBucketWire) []*wavespanv1.HeldBuckets {
	if len(bs) == 0 {
		return nil
	}
	out := make([]*wavespanv1.HeldBuckets, 0, len(bs))
	for _, b := range bs {
		out = append(out, &wavespanv1.HeldBuckets{MemberId: b.MemberID, Collection: b.Collection, Qver: b.QVer, Buckets: b.Buckets, GeneratedAtUnixMs: b.GeneratedAtUnixMs})
	}
	return out
}

func heldBucketsFromProto(bs []*wavespanv1.HeldBuckets) []HeldBucketWire {
	if len(bs) == 0 {
		return nil
	}
	out := make([]HeldBucketWire, 0, len(bs))
	for _, b := range bs {
		out = append(out, HeldBucketWire{MemberID: b.GetMemberId(), Collection: b.GetCollection(), QVer: b.GetQver(), Buckets: b.GetBuckets(), GeneratedAtUnixMs: b.GetGeneratedAtUnixMs()})
	}
	return out
}

func msgToReq(m *GossipMessage) *wavespanv1.GossipExchangeRequest {
	req := &wavespanv1.GossipExchangeRequest{From: memberToProto(m.From), HolderSummaries: summariesToProto(m.Summaries), ConfigDeltas: configDeltasToProto(m.ConfigDeltas), HeldBuckets: heldBucketsToProto(m.HeldBuckets)}
	for _, mv := range m.Members {
		req.Members = append(req.Members, MemberStateToProto(mv))
	}
	return req
}

func reqToMsg(r *wavespanv1.GossipExchangeRequest) *GossipMessage {
	m := &GossipMessage{From: memberFromProto(r.GetFrom()), Summaries: summariesFromProto(r.GetHolderSummaries()), ConfigDeltas: configDeltasFromProto(r.GetConfigDeltas()), HeldBuckets: heldBucketsFromProto(r.GetHeldBuckets())}
	for _, ms := range r.GetMembers() {
		m.Members = append(m.Members, memberStateFromProto(ms))
	}
	return m
}

func msgToResp(m *GossipMessage) *wavespanv1.GossipExchangeResponse {
	resp := &wavespanv1.GossipExchangeResponse{From: memberToProto(m.From), HolderSummaries: summariesToProto(m.Summaries), ConfigDeltas: configDeltasToProto(m.ConfigDeltas), HeldBuckets: heldBucketsToProto(m.HeldBuckets)}
	for _, mv := range m.Members {
		resp.Members = append(resp.Members, MemberStateToProto(mv))
	}
	return resp
}

func respToMsg(r *wavespanv1.GossipExchangeResponse) *GossipMessage {
	m := &GossipMessage{From: memberFromProto(r.GetFrom()), Summaries: summariesFromProto(r.GetHolderSummaries()), ConfigDeltas: configDeltasFromProto(r.GetConfigDeltas()), HeldBuckets: heldBucketsFromProto(r.GetHeldBuckets())}
	for _, ms := range r.GetMembers() {
		m.Members = append(m.Members, memberStateFromProto(ms))
	}
	return m
}

// --- gRPC transport (real gossip wire) ---

// GRPCTransport carries gossip over gRPC (the same optimized HTTP/2 stack as the data port). Clients
// are cached per gossip address over the shared pooled gRPC connections (rpcopts.GRPCConn).
type GRPCTransport struct {
	mu      sync.Mutex
	clients map[string]wavespanv1.GossipServiceClient
}

// NewGRPCTransport builds a gossip transport backed by pooled gRPC client connections.
func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{clients: map[string]wavespanv1.GossipServiceClient{}}
}

func (t *GRPCTransport) client(addr string) (wavespanv1.GossipServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[addr]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewGossipServiceClient(conn)
	t.clients[addr] = c
	return c, nil
}

// Ping sends a direct gossip exchange (implements Transport).
func (t *GRPCTransport) Ping(ctx context.Context, addr string, msg *GossipMessage) (*GossipMessage, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	resp, err := c.Exchange(ctx, msgToReq(msg))
	if err != nil {
		return nil, err
	}
	return respToMsg(resp), nil
}

// IndirectPing asks relayAddr to relay an exchange to targetAddr (implements Transport).
func (t *GRPCTransport) IndirectPing(ctx context.Context, relayAddr, targetAddr string, msg *GossipMessage) (*GossipMessage, error) {
	c, err := t.client(relayAddr)
	if err != nil {
		return nil, err
	}
	resp, err := c.IndirectExchange(ctx, &wavespanv1.IndirectExchangeRequest{
		TargetGossipAddr: targetAddr,
		Payload:          msgToReq(msg),
	})
	if err != nil {
		return nil, err
	}
	return respToMsg(resp), nil
}

// --- gRPC server (gossip handler) ---

// GossipGRPCServer adapts a Gossip driver to the GossipService gRPC handler. IndirectExchange relays
// a direct exchange to the requested target.
type GossipGRPCServer struct {
	wavespanv1.UnimplementedGossipServiceServer
	g         *Gossip
	transport *GRPCTransport
}

// NewGossipGRPCServer builds the handler; transport is used to relay indirect probes.
func NewGossipGRPCServer(g *Gossip, transport *GRPCTransport) *GossipGRPCServer {
	return &GossipGRPCServer{g: g, transport: transport}
}

// Exchange handles a direct gossip exchange.
func (s *GossipGRPCServer) Exchange(_ context.Context, req *wavespanv1.GossipExchangeRequest) (*wavespanv1.GossipExchangeResponse, error) {
	reply := s.g.HandleGossip(reqToMsg(req))
	return msgToResp(reply), nil
}

// IndirectExchange relays a gossip exchange to the requested target on the caller's behalf.
func (s *GossipGRPCServer) IndirectExchange(ctx context.Context, req *wavespanv1.IndirectExchangeRequest) (*wavespanv1.GossipExchangeResponse, error) {
	reply, err := s.transport.Ping(ctx, req.GetTargetGossipAddr(), reqToMsg(req.GetPayload()))
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return msgToResp(reply), nil
}
