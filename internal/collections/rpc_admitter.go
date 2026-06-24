package collections

import (
	"context"
	"errors"
	"sync"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// RPCAdmitter implements LearnerAdmitter by calling the AdmitLearner RPC on peer nodes until one — a
// member of the shard — accepts (design/30 §9). Peers returns candidate peer data-port addresses (the
// node's membership view, minus self). Peers are dialled over gRPC via the rpcopts pooled connections;
// per-address grpc clients are cached.
type RPCAdmitter struct {
	peers func() []string

	mu      sync.Mutex
	clients map[string]wavespanv1.CollectionServiceClient
}

var _ LearnerAdmitter = (*RPCAdmitter)(nil)

// NewRPCAdmitter builds an admitter that asks the given peers over gRPC.
func NewRPCAdmitter(peers func() []string) *RPCAdmitter {
	return &RPCAdmitter{peers: peers, clients: map[string]wavespanv1.CollectionServiceClient{}}
}

// client returns a cached CollectionService gRPC client for addr, dialling it on first use.
func (a *RPCAdmitter) client(addr string) (wavespanv1.CollectionServiceClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if c, ok := a.clients[addr]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewCollectionServiceClient(conn)
	a.clients[addr] = c
	return c, nil
}

// AdmitLearner asks each peer in turn to admit this node as a learner; the first success wins.
func (a *RPCAdmitter) AdmitLearner(ctx context.Context, shardID, replicaID uint64, target string) error {
	var lastErr error
	for _, addr := range a.peers() {
		c, err := a.client(addr)
		if err != nil {
			lastErr = err
			continue
		}
		_, err = c.AdmitLearner(ctx, &wavespanv1.AdmitLearnerRequest{
			ShardId: shardID, ReplicaId: replicaID, Target: target,
		})
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		return errors.New("collections: no peers available to admit learner")
	}
	return lastErr
}
