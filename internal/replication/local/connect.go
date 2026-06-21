package local

import (
	"context"
	"net/http"
	"sync"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
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
	var c connect.HTTPClient = http.DefaultClient
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

// ReplicaServer adapts a Receiver to the ReplicationService Connect handler.
type ReplicaServer struct {
	recv *Receiver
}

// NewReplicaServer builds the Connect handler around a receiver.
func NewReplicaServer(recv *Receiver) *ReplicaServer { return &ReplicaServer{recv: recv} }

// Handler returns the mountable Connect handler (path, handler) for the data port.
func (s *ReplicaServer) Handler() (string, http.Handler) {
	return wavespanv1connect.NewReplicationServiceHandler(s)
}

// StoreReplica handles an inbound replica write.
func (s *ReplicaServer) StoreReplica(_ context.Context, req *connect.Request[wavespanv1.StoreReplicaRequest]) (*connect.Response[wavespanv1.StoreReplicaResponse], error) {
	resp, err := s.recv.Apply(req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}
