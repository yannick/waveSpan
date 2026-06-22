package local

import (
	"context"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/rpcopts"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// ConnectReplicator dials peers' ReplicationService over the Connect protocol (clients cached
// per data address).
type ConnectReplicator struct {
	httpClient connect.HTTPClient
	mu         sync.Mutex
	clients    map[string]wavespanv1connect.ReplicationServiceClient
}

// NewConnectReplicator builds a replicator over the given HTTP client (nil uses http.DefaultClient).
func NewConnectReplicator(hc *http.Client) *ConnectReplicator {
	var c connect.HTTPClient = rpcopts.H2CClient()
	if hc != nil {
		c = hc
	}
	return &ConnectReplicator{httpClient: c, clients: map[string]wavespanv1connect.ReplicationServiceClient{}}
}

func (r *ConnectReplicator) client(addr string) wavespanv1connect.ReplicationServiceClient {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[addr]; ok {
		return c
	}
	c := wavespanv1connect.NewReplicationServiceClient(r.httpClient, "http://"+addr)
	r.clients[addr] = c
	return c
}

// StoreReplica sends the request to the target's data address (implements Replicator).
func (r *ConnectReplicator) StoreReplica(ctx context.Context, target membership.Member, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	resp, err := r.client(target.DataAddr).StoreReplica(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ScanLocal asks a holder to scan its local store over a subrange (routed-eventual scan, M6).
func (r *ConnectReplicator) ScanLocal(ctx context.Context, target membership.Member, namespace string, start, end []byte, limit int) ([]*wavespanv1.ScanLocalRow, error) {
	resp, err := r.client(target.DataAddr).ScanLocal(ctx, connect.NewRequest(&wavespanv1.ScanLocalRequest{
		Namespace: namespace, StartKey: start, EndKey: end, Limit: uint32(limit),
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetRows(), nil
}

// SubscriptionSource pushes cache updates to a key subscriber (implemented by the cache package
// in M5.B). When nil, SubscribeKey sends only the current record and closes.
type SubscriptionSource interface {
	Subscribe(ctx context.Context, req *wavespanv1.SubscribeKeyRequest, send func(*wavespanv1.CacheUpdate) error) error
}

// ReplicaServer adapts a Receiver to the full ReplicationService Connect handler (StoreReplica,
// FetchReplica, SubscribeKey).
type ReplicaServer struct {
	recv   *Receiver
	reader RecordReader
	self   string
	dataAd string
	source SubscriptionSource
}

// NewReplicaServer builds the Connect handler. reader serves FetchReplica; source (optional)
// serves live SubscribeKey updates.
func NewReplicaServer(recv *Receiver, reader RecordReader, selfMemberID, selfDataAddr string, source SubscriptionSource) *ReplicaServer {
	return &ReplicaServer{recv: recv, reader: reader, self: selfMemberID, dataAd: selfDataAddr, source: source}
}

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *ReplicaServer) Handler() (string, http.Handler) {
	return wavespanv1connect.NewReplicationServiceHandler(s, rpcopts.Handler()...)
}

// StoreReplica handles an inbound replica write.
func (s *ReplicaServer) StoreReplica(_ context.Context, req *connect.Request[wavespanv1.StoreReplicaRequest]) (*connect.Response[wavespanv1.StoreReplicaResponse], error) {
	resp, err := s.recv.Apply(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

// FetchReplica returns the local winning record for a key (design/05 "FetchReplica protocol").
func (s *ReplicaServer) FetchReplica(_ context.Context, req *connect.Request[wavespanv1.FetchReplicaRequest]) (*connect.Response[wavespanv1.FetchReplicaResponse], error) {
	rec, found, err := s.reader.GetRecord(req.Msg.GetNamespace(), req.Msg.GetKey())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &wavespanv1.FetchReplicaResponse{Found: found, Record: rec}
	if found && req.Msg.GetWantSubscriptionOffer() {
		resp.SubscriptionOffer = &wavespanv1.SubscriptionOffer{SourceMemberId: s.self, SourceDataAddr: s.dataAd}
	}
	return connect.NewResponse(resp), nil
}

const backfillMaxPage = 1024

// Backfill streams this holder's full records for a namespace, paginated, so a joining node can
// bootstrap an "everywhere"-replicated namespace (design/05 node sync).
func (s *ReplicaServer) Backfill(_ context.Context, req *connect.Request[wavespanv1.BackfillRequest]) (*connect.Response[wavespanv1.BackfillResponse], error) {
	limit := int(req.Msg.GetLimit())
	if limit <= 0 || limit > backfillMaxPage {
		limit = backfillMaxPage
	}
	recs, next, err := s.reader.ScanRecordsFrom(req.Msg.GetNamespace(), req.Msg.GetCursor(), limit)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&wavespanv1.BackfillResponse{Records: recs, NextCursor: next}), nil
}

// ScanLocal scans this holder's local store over a subrange (routed-eventual scan, M6).
func (s *ReplicaServer) ScanLocal(_ context.Context, req *connect.Request[wavespanv1.ScanLocalRequest]) (*connect.Response[wavespanv1.ScanLocalResponse], error) {
	rows, err := s.reader.ScanRange(req.Msg.GetNamespace(), req.Msg.GetStartKey(), req.Msg.GetEndKey(), int(req.Msg.GetLimit()), time.Now().UnixMilli())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &wavespanv1.ScanLocalResponse{}
	for _, r := range rows {
		row := &wavespanv1.ScanLocalRow{Key: r.Key, Value: r.Value, Version: r.Version.ToProto()}
		if r.ExpiresAtMs != nil {
			row.ExpiresAtUnixMs = r.ExpiresAtMs
		}
		resp.Rows = append(resp.Rows, row)
	}
	return connect.NewResponse(resp), nil
}

// SubscribeKey streams cache updates for a key. Without a live source (M5.A) it sends the current
// record once and closes; the subscriber then refetches on demand.
func (s *ReplicaServer) SubscribeKey(ctx context.Context, req *connect.Request[wavespanv1.SubscribeKeyRequest], stream *connect.ServerStream[wavespanv1.CacheUpdate]) error {
	if s.source != nil {
		return s.source.Subscribe(ctx, req.Msg, stream.Send)
	}
	rec, found, err := s.reader.GetRecord(req.Msg.GetNamespace(), req.Msg.GetKey())
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	if found {
		return stream.Send(&wavespanv1.CacheUpdate{Namespace: req.Msg.GetNamespace(), Key: req.Msg.GetKey(), Record: rec, StreamSequence: 1})
	}
	return nil
}
