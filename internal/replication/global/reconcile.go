package global

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/yannick/wavespan/internal/config"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Reconciler runs anti-entropy against each peer: it compares per-range content hashes, fetches and
// re-applies records in divergent ranges, and advances the out-log checkpoint once a peer is
// confirmed caught up so compaction can reclaim disk (design/06 "Anti-entropy").
type Reconciler struct {
	ae         *AntiEntropy
	applier    *Applier
	outlog     *OutLog
	peers      []config.ClusterPeer
	namespaces []string
	metrics    *Metrics
	clients    map[string]wavespanv1.GlobalReplicationClient
}

// NewReconciler wires a reconciler. namespaces is the set of namespaces whose whole keyspace is
// compared each round (default ["default"]). The hc argument is retained for call-site compatibility
// but is unused: peer replication endpoints are dialled over gRPC via the rpcopts pooled connections.
func NewReconciler(ae *AntiEntropy, applier *Applier, outlog *OutLog, peers []config.ClusterPeer, namespaces []string, _ *http.Client, m *Metrics) *Reconciler {
	if len(namespaces) == 0 {
		namespaces = []string{"default"}
	}
	return &Reconciler{ae: ae, applier: applier, outlog: outlog, peers: peers, namespaces: namespaces, metrics: m, clients: map[string]wavespanv1.GlobalReplicationClient{}}
}

func (r *Reconciler) ranges() []*wavespanv1.KeyRange {
	out := make([]*wavespanv1.KeyRange, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		out = append(out, &wavespanv1.KeyRange{Namespace: ns})
	}
	return out
}

func (r *Reconciler) client(endpoint string) (wavespanv1.GlobalReplicationClient, error) {
	if c, ok := r.clients[endpoint]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(endpoint)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewGlobalReplicationClient(conn)
	r.clients[endpoint] = c
	return c, nil
}

// ReconcileOnce runs one anti-entropy round against every peer and returns the number of divergent
// ranges repaired.
func (r *Reconciler) ReconcileOnce(ctx context.Context) int {
	repaired := 0
	ranges := r.ranges()
	local := r.ae.Summarize(ranges)
	localByNS := map[string][]byte{}
	for _, h := range local {
		localByNS[h.GetRange().GetNamespace()] = h.GetHash()
	}

	for _, peer := range r.peers {
		cl, err := r.client(peer.ReplEndpoint)
		if err != nil {
			continue
		}
		resp, err := cl.RangeSummary(ctx, &wavespanv1.RangeSummaryRequest{Ranges: ranges})
		if err != nil {
			continue
		}
		if r.metrics != nil {
			r.metrics.AntiEntropyRuns.Inc()
		}
		for _, rh := range resp.GetHashes() {
			ns := rh.GetRange().GetNamespace()
			if bytes.Equal(rh.GetHash(), localByNS[ns]) {
				continue // converged
			}
			repaired++
			if r.metrics != nil {
				r.metrics.AntiEntropyDivergentRanges.Inc()
			}
			r.fetchAndApply(ctx, cl, rh.GetRange())
		}
		// peer confirmed caught up this round -> advance the out-log checkpoint so compaction can run
		for part := uint32(0); part < numPartitions; part++ {
			r.outlog.Checkpoint(peer.ClusterID, part, r.outlog.LastSeq(peer.ClusterID, part))
		}
	}
	return repaired
}

func (r *Reconciler) fetchAndApply(ctx context.Context, cl wavespanv1.GlobalReplicationClient, kr *wavespanv1.KeyRange) {
	stream, err := cl.FetchRange(ctx, &wavespanv1.FetchRangeRequest{Range: kr})
	if err != nil {
		return
	}
	for {
		m, err := stream.Recv()
		if err != nil {
			break // io.EOF on a clean close, or a transport error
		}
		if _, err := r.applier.Apply(m); err != nil && r.metrics != nil {
			r.metrics.ApplyErrors.Inc()
		}
	}
}

// Run reconciles on the given interval until ctx is done.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.ReconcileOnce(ctx)
		}
	}
}
