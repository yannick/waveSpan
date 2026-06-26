package collections

import (
	"context"
	"errors"
	"sync"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCForwarder implements Forwarder by calling ProposeForward on peer nodes until the leader accepts
// (design/30 §13.13). It caches the last peer that accepted (the likely leader) and tries it first, so
// steady state is a single hop. Peers returns candidate peer data-port addresses (membership minus
// self). Peers are dialled over gRPC via the rpcopts pooled connections; per-address grpc clients are
// cached.
type RPCForwarder struct {
	peers func() []string

	mu      sync.Mutex
	hint    string // last peer that accepted (the likely leader)
	clients map[string]wavespanv1.CollectionServiceClient
}

var _ Forwarder = (*RPCForwarder)(nil)

// NewRPCForwarder builds a forwarder over the given peers. It dials peer data ports over gRPC.
func NewRPCForwarder(peers func() []string) *RPCForwarder {
	return &RPCForwarder{peers: peers, clients: map[string]wavespanv1.CollectionServiceClient{}}
}

// client returns a cached CollectionService gRPC client for addr, dialling it on first use.
func (f *RPCForwarder) client(addr string) (wavespanv1.CollectionServiceClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewCollectionServiceClient(conn)
	f.clients[addr] = c
	return c, nil
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
		c, err := f.client(addr)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := c.ProposeForward(ctx, &wavespanv1.ProposeForwardRequest{
			Namespace: string(ns), Collection: coll, Command: cmd,
		})
		if err == nil {
			f.mu.Lock()
			f.hint = addr
			f.mu.Unlock()
			return resp.GetCount(), resp.GetData(), nil
		}
		switch status.Code(err) {
		case codes.FailedPrecondition:
			return 0, nil, ErrWrongType // a datatype mismatch is definitive on any node
		case codes.InvalidArgument:
			return 0, nil, ErrNotNumber // a non-numeric HIncr field is definitive on any node
		case codes.ResourceExhausted:
			// The leader shed this write (disk pressure / load shed, design/36 + design/33). It is a
			// transient backpressure signal, NOT a "this node can't lead" signal — trying the next peer would
			// just spread the flood, and the leader is the right node. Surface it terminally as ErrDiskPressure
			// so the forwarding node's handler re-maps it to ResourceExhausted (collErr) instead of Internal.
			return 0, nil, ErrDiskPressure
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("collections: no peer accepted the forwarded write")
	}
	return 0, nil, lastErr
}
