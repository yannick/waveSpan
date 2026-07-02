package local

import (
	"context"
	"sync"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// ConnectReplicator dials peers' ReplicationService over gRPC (clients cached per data address).
// The name is retained for call-site compatibility; the transport is now grpc-go over the pooled
// connections in rpcopts.
type ConnectReplicator struct {
	mu       sync.Mutex
	clients  map[string]wavespanv1.ReplicationServiceClient
	batchers map[string]*destBatcher
}

// NewConnectReplicator builds a replicator. Peers are dialled over gRPC via the rpcopts pooled
// connections.
func NewConnectReplicator() *ConnectReplicator {
	return &ConnectReplicator{
		clients:  map[string]wavespanv1.ReplicationServiceClient{},
		batchers: map[string]*destBatcher{},
	}
}

func (r *ConnectReplicator) client(addr string) (wavespanv1.ReplicationServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[addr]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewReplicationServiceClient(conn)
	r.clients[addr] = c
	return c, nil
}

// StoreReplica sends the request to the target's data address (implements Replicator).
//
// Transparent coalescing (design/37 P1.4): concurrent StoreReplica calls to the same peer —
// coordinator min-ack writes, fanout target-N fills, repair pushes all land here — are batched by
// a per-destination destBatcher into one StoreReplicaBatch RPC per ~batchWindow. Callers keep
// exactly the one-request/one-response semantics of the unary call.
func (r *ConnectReplicator) StoreReplica(ctx context.Context, target membership.Member, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
	c, err := r.client(target.DataAddr)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	b, ok := r.batchers[target.DataAddr]
	if !ok {
		b = &destBatcher{}
		r.batchers[target.DataAddr] = b
	}
	r.mu.Unlock()
	return b.enqueue(ctx, req,
		func(ctx context.Context, reqs []*wavespanv1.StoreReplicaRequest) ([]*wavespanv1.StoreReplicaResponse, error) {
			resp, err := c.StoreReplicaBatch(ctx, &wavespanv1.StoreReplicaBatchRequest{Requests: reqs})
			return resp.GetResponses(), err
		},
		func(ctx context.Context, req *wavespanv1.StoreReplicaRequest) (*wavespanv1.StoreReplicaResponse, error) {
			return c.StoreReplica(ctx, req)
		},
	)
}

// FetchReplica asks a holder for its local winning record of a single key (design/05). Used by
// the Global Data Browser to resolve holders within a cluster.
func (r *ConnectReplicator) FetchReplica(ctx context.Context, target membership.Member, namespace string, key []byte) (*wavespanv1.FetchReplicaResponse, error) {
	c, err := r.client(target.DataAddr)
	if err != nil {
		return nil, err
	}
	return c.FetchReplica(ctx, &wavespanv1.FetchReplicaRequest{Namespace: namespace, Key: key})
}

// PeerFetch returns a PeerFetch closure backed by this replicator's pooled gRPC clients, for the
// intra-cluster anti-entropy pull path. It dials the data port over gRPC (the transport the pure
// grpc-go data server actually speaks) rather than the Connect wire, and returns (nil,false) on any
// transport or RPC error to match the PeerFetch contract.
func (r *ConnectReplicator) PeerFetch() PeerFetch {
	return func(ctx context.Context, dataAddr, namespace string, key []byte) (*wavespanv1.StoredRecord, bool) {
		c, err := r.client(dataAddr)
		if err != nil {
			return nil, false
		}
		resp, err := c.FetchReplica(ctx, &wavespanv1.FetchReplicaRequest{Namespace: namespace, Key: key})
		if err != nil {
			return nil, false
		}
		return resp.GetRecord(), resp.GetFound()
	}
}

// BackfillFetch returns a BackfillFetch closure backed by this replicator's pooled gRPC clients, for
// the everywhere-namespace bootstrap path. Like PeerFetch it dials the data port over gRPC.
func (r *ConnectReplicator) BackfillFetch() BackfillFetch {
	return func(ctx context.Context, dataAddr, namespace string, cursor []byte, limit int) ([]*wavespanv1.StoredRecord, []byte, error) {
		c, err := r.client(dataAddr)
		if err != nil {
			return nil, nil, err
		}
		resp, err := c.Backfill(ctx, &wavespanv1.BackfillRequest{Namespace: namespace, Cursor: cursor, Limit: uint32(limit)})
		if err != nil {
			return nil, nil, err
		}
		return resp.GetRecords(), resp.GetNextCursor(), nil
	}
}

// ScanLocal asks a holder to scan its local store over a subrange (routed-eventual scan, M6).
func (r *ConnectReplicator) ScanLocal(ctx context.Context, target membership.Member, namespace string, start, end []byte, limit int) ([]*wavespanv1.ScanLocalRow, error) {
	c, err := r.client(target.DataAddr)
	if err != nil {
		return nil, err
	}
	resp, err := c.ScanLocal(ctx, &wavespanv1.ScanLocalRequest{
		Namespace: namespace, StartKey: start, EndKey: end, Limit: uint32(limit),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRows(), nil
}

// SubscriptionSource pushes cache updates to a key subscriber (implemented by the cache package
// in M5.B). When nil, SubscribeKey sends only the current record and closes.
type SubscriptionSource interface {
	Subscribe(ctx context.Context, req *wavespanv1.SubscribeKeyRequest, send func(*wavespanv1.CacheUpdate) error) error
}
