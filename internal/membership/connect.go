package membership

import (
	"context"
	"net/http"
	"sync"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
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
		out = append(out, &wavespanv1.HolderSummary{MemberId: s.MemberID, BloomFilter: s.Bloom, GeneratedAtUnixMs: s.GeneratedAtUnixMs})
	}
	return out
}

func summariesFromProto(ps []*wavespanv1.HolderSummary) []HolderSummaryWire {
	if len(ps) == 0 {
		return nil
	}
	out := make([]HolderSummaryWire, 0, len(ps))
	for _, p := range ps {
		out = append(out, HolderSummaryWire{MemberID: p.GetMemberId(), Bloom: p.GetBloomFilter(), GeneratedAtUnixMs: p.GetGeneratedAtUnixMs()})
	}
	return out
}

func msgToReq(m *GossipMessage) *wavespanv1.GossipExchangeRequest {
	req := &wavespanv1.GossipExchangeRequest{From: memberToProto(m.From), HolderSummaries: summariesToProto(m.Summaries)}
	for _, mv := range m.Members {
		req.Members = append(req.Members, MemberStateToProto(mv))
	}
	return req
}

func reqToMsg(r *wavespanv1.GossipExchangeRequest) *GossipMessage {
	m := &GossipMessage{From: memberFromProto(r.GetFrom()), Summaries: summariesFromProto(r.GetHolderSummaries())}
	for _, ms := range r.GetMembers() {
		m.Members = append(m.Members, memberStateFromProto(ms))
	}
	return m
}

func msgToResp(m *GossipMessage) *wavespanv1.GossipExchangeResponse {
	resp := &wavespanv1.GossipExchangeResponse{From: memberToProto(m.From), HolderSummaries: summariesToProto(m.Summaries)}
	for _, mv := range m.Members {
		resp.Members = append(resp.Members, MemberStateToProto(mv))
	}
	return resp
}

func respToMsg(r *wavespanv1.GossipExchangeResponse) *GossipMessage {
	m := &GossipMessage{From: memberFromProto(r.GetFrom()), Summaries: summariesFromProto(r.GetHolderSummaries())}
	for _, ms := range r.GetMembers() {
		m.Members = append(m.Members, memberStateFromProto(ms))
	}
	return m
}

// --- Connect transport (real gossip wire) ---

// ConnectTransport carries gossip over the Connect protocol (HTTP/1.1-compatible). Clients are
// cached per gossip address.
type ConnectTransport struct {
	httpClient connect.HTTPClient
	mu         sync.Mutex
	clients    map[string]wavespanv1connect.GossipServiceClient
}

// NewConnectTransport builds a transport over the given HTTP client (nil uses http.DefaultClient).
func NewConnectTransport(hc *http.Client) *ConnectTransport {
	var c connect.HTTPClient = http.DefaultClient
	if hc != nil {
		c = hc
	}
	return &ConnectTransport{httpClient: c, clients: map[string]wavespanv1connect.GossipServiceClient{}}
}

func (t *ConnectTransport) client(addr string) wavespanv1connect.GossipServiceClient {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[addr]; ok {
		return c
	}
	c := wavespanv1connect.NewGossipServiceClient(t.httpClient, "http://"+addr)
	t.clients[addr] = c
	return c
}

// Ping sends a direct gossip exchange (implements Transport).
func (t *ConnectTransport) Ping(ctx context.Context, addr string, msg *GossipMessage) (*GossipMessage, error) {
	resp, err := t.client(addr).Exchange(ctx, connect.NewRequest(msgToReq(msg)))
	if err != nil {
		return nil, err
	}
	return respToMsg(resp.Msg), nil
}

// IndirectPing asks relayAddr to relay an exchange to targetAddr (implements Transport).
func (t *ConnectTransport) IndirectPing(ctx context.Context, relayAddr, targetAddr string, msg *GossipMessage) (*GossipMessage, error) {
	resp, err := t.client(relayAddr).IndirectExchange(ctx, connect.NewRequest(&wavespanv1.IndirectExchangeRequest{
		TargetGossipAddr: targetAddr,
		Payload:          msgToReq(msg),
	}))
	if err != nil {
		return nil, err
	}
	return respToMsg(resp.Msg), nil
}

// --- Connect server (gossip handler) ---

// GossipConnectServer adapts a Gossip driver to the GossipService Connect handler. IndirectExchange
// relays a direct exchange to the requested target.
type GossipConnectServer struct {
	g         *Gossip
	transport *ConnectTransport
}

// NewGossipConnectServer builds the handler; transport is used to relay indirect probes.
func NewGossipConnectServer(g *Gossip, transport *ConnectTransport) *GossipConnectServer {
	return &GossipConnectServer{g: g, transport: transport}
}

// Handler returns the mountable Connect handler path and http.Handler.
func (s *GossipConnectServer) Handler() (string, http.Handler) {
	return wavespanv1connect.NewGossipServiceHandler(s)
}

// Exchange handles a direct gossip exchange.
func (s *GossipConnectServer) Exchange(_ context.Context, req *connect.Request[wavespanv1.GossipExchangeRequest]) (*connect.Response[wavespanv1.GossipExchangeResponse], error) {
	reply := s.g.HandleGossip(reqToMsg(req.Msg))
	return connect.NewResponse(msgToResp(reply)), nil
}

// IndirectExchange relays a gossip exchange to the requested target on the caller's behalf.
func (s *GossipConnectServer) IndirectExchange(ctx context.Context, req *connect.Request[wavespanv1.IndirectExchangeRequest]) (*connect.Response[wavespanv1.GossipExchangeResponse], error) {
	reply, err := s.transport.Ping(ctx, req.Msg.GetTargetGossipAddr(), reqToMsg(req.Msg.GetPayload()))
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewResponse(msgToResp(reply)), nil
}
