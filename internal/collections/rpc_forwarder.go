package collections

import (
	"context"
	"errors"
	"sync"

	"connectrpc.com/connect"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// RPCForwarder implements Forwarder by calling ProposeForward on peer nodes until the leader accepts
// (design/30 §13.13). It caches the last peer that accepted (the likely leader) and tries it first, so
// steady state is a single hop. Peers returns candidate peer data-port addresses (membership minus
// self); client is the node's shared mTLS-capable HTTP client.
type RPCForwarder struct {
	client connect.HTTPClient
	peers  func() []string

	mu   sync.Mutex
	hint string // last peer that accepted (the likely leader)
}

var _ Forwarder = (*RPCForwarder)(nil)

// NewRPCForwarder builds a forwarder over the given peers and HTTP client.
func NewRPCForwarder(client connect.HTTPClient, peers func() []string) *RPCForwarder {
	return &RPCForwarder{client: client, peers: peers}
}

// Forward tries the cached leader first, then the other peers, until one commits the write. Returns the
// apply result (value + optional data, e.g. an HIncr new value).
func (f *RPCForwarder) Forward(ctx context.Context, ns, coll, cmd []byte) (uint64, []byte, error) {
	f.mu.Lock()
	hint := f.hint
	f.mu.Unlock()

	ordered := make([]string, 0, 1)
	if hint != "" {
		ordered = append(ordered, hint)
	}
	for _, a := range f.peers() {
		if a != hint {
			ordered = append(ordered, a)
		}
	}

	var lastErr error
	for _, addr := range ordered {
		c := wavespanv1connect.NewCollectionServiceClient(f.client, "http://"+addr)
		resp, err := c.ProposeForward(ctx, connect.NewRequest(&wavespanv1.ProposeForwardRequest{
			Namespace: string(ns), Collection: coll, Command: cmd,
		}))
		if err == nil {
			f.mu.Lock()
			f.hint = addr
			f.mu.Unlock()
			return resp.Msg.GetCount(), resp.Msg.GetData(), nil
		}
		switch connect.CodeOf(err) {
		case connect.CodeFailedPrecondition:
			return 0, nil, ErrWrongType // a datatype mismatch is definitive on any node
		case connect.CodeInvalidArgument:
			return 0, nil, ErrNotNumber // a non-numeric HIncr field is definitive on any node
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("collections: no peer accepted the forwarded write")
	}
	return 0, nil, lastErr
}
