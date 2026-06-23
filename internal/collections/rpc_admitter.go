package collections

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// RPCAdmitter implements LearnerAdmitter by calling the AdmitLearner RPC on peer nodes until one — a
// member of the shard — accepts (design/30 §9). Peers returns candidate peer data-port addresses (the
// node's membership view, minus self); the client is the node's shared mTLS-capable HTTP client.
type RPCAdmitter struct {
	client connect.HTTPClient
	peers  func() []string
}

var _ LearnerAdmitter = (*RPCAdmitter)(nil)

// NewRPCAdmitter builds an admitter that asks the given peers over Connect.
func NewRPCAdmitter(client connect.HTTPClient, peers func() []string) *RPCAdmitter {
	return &RPCAdmitter{client: client, peers: peers}
}

// AdmitLearner asks each peer in turn to admit this node as a learner; the first success wins.
func (a *RPCAdmitter) AdmitLearner(ctx context.Context, shardID, replicaID uint64, target string) error {
	var lastErr error
	for _, addr := range a.peers() {
		c := wavespanv1connect.NewCollectionServiceClient(a.client, "http://"+addr)
		_, err := c.AdmitLearner(ctx, connect.NewRequest(&wavespanv1.AdmitLearnerRequest{
			ShardId: shardID, ReplicaId: replicaID, Target: target,
		}))
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
