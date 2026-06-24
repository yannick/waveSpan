package global

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Sender drains each peer's outbound log and ships batches via PushGlobal, advancing its cursor
// and checkpointing the out-log so disk can be reclaimed (design/06). It resumes from the last
// sent cursor after a disconnect — no gaps, idempotent on replay.
type Sender struct {
	outlog *OutLog
	peers  []config.ClusterPeer
	batch  int

	mu      sync.Mutex
	clients map[string]wavespanv1.GlobalReplicationClient
	cursor  map[string]uint64 // (peer,partition) -> last sent seq
}

// NewSender wires a sender over an out-log and the configured peers. The hc argument is retained for
// call-site compatibility but is unused: peer replication endpoints are dialled over gRPC via the
// rpcopts pooled connections.
func NewSender(outlog *OutLog, peers []config.ClusterPeer, _ *http.Client) *Sender {
	return &Sender{outlog: outlog, peers: peers, batch: 256, clients: map[string]wavespanv1.GlobalReplicationClient{}, cursor: map[string]uint64{}}
}

func (s *Sender) client(endpoint string) (wavespanv1.GlobalReplicationClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[endpoint]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(endpoint)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewGlobalReplicationClient(conn)
	s.clients[endpoint] = c
	return c, nil
}

func (s *Sender) getCursor(peer string, partition uint32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor[pk(peer, partition)]
}

func (s *Sender) setCursor(peer string, partition uint32, seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursor[pk(peer, partition)] = seq
}

// DrainOnce ships all pending out-log entries to every peer and returns how many were sent. A peer
// that is unreachable is skipped; its entries stay in the out-log for the next pass.
func (s *Sender) DrainOnce(ctx context.Context) int {
	sent := 0
	for _, peer := range s.peers {
		for part := uint32(0); part < numPartitions; part++ {
			cursor := s.getCursor(peer.ClusterID, part)
			entries, err := s.outlog.IterateFrom(peer.ClusterID, part, cursor, s.batch)
			if err != nil || len(entries) == 0 {
				continue
			}
			req := &wavespanv1.PushGlobalRequest{}
			for _, e := range entries {
				req.Mutations = append(req.Mutations, e.Mutation)
			}
			cl, err := s.client(peer.ReplEndpoint)
			if err != nil {
				continue // could not dial: retry next pass from the same cursor (no gaps)
			}
			if _, err := cl.PushGlobal(ctx, req); err != nil {
				continue // peer down: retry next pass from the same cursor (no gaps)
			}
			last := entries[len(entries)-1].Seq
			s.setCursor(peer.ClusterID, part, last)
			s.outlog.Checkpoint(peer.ClusterID, part, last)
			_, _ = s.outlog.CompactBelowCheckpoint(peer.ClusterID, part)
			sent += len(entries)
		}
	}
	return sent
}

// Run drains on the given interval until ctx is done.
func (s *Sender) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.DrainOnce(ctx)
		}
	}
}
