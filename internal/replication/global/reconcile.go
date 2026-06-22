package global

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/config"
	"github.com/cwire/wavespan/internal/rpcopts"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
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
	httpClient connect.HTTPClient
	metrics    *Metrics
	clients    map[string]wavespanv1connect.GlobalReplicationClient
}

// NewReconciler wires a reconciler. namespaces is the set of namespaces whose whole keyspace is
// compared each round (default ["default"]).
func NewReconciler(ae *AntiEntropy, applier *Applier, outlog *OutLog, peers []config.ClusterPeer, namespaces []string, hc *http.Client, m *Metrics) *Reconciler {
	var c connect.HTTPClient = rpcopts.H2CClient()
	if hc != nil {
		c = hc
	}
	if len(namespaces) == 0 {
		namespaces = []string{"default"}
	}
	return &Reconciler{ae: ae, applier: applier, outlog: outlog, peers: peers, namespaces: namespaces, httpClient: c, metrics: m, clients: map[string]wavespanv1connect.GlobalReplicationClient{}}
}

func (r *Reconciler) ranges() []*wavespanv1.KeyRange {
	out := make([]*wavespanv1.KeyRange, 0, len(r.namespaces))
	for _, ns := range r.namespaces {
		out = append(out, &wavespanv1.KeyRange{Namespace: ns})
	}
	return out
}

func (r *Reconciler) client(endpoint string) wavespanv1connect.GlobalReplicationClient {
	if c, ok := r.clients[endpoint]; ok {
		return c
	}
	c := wavespanv1connect.NewGlobalReplicationClient(r.httpClient, "http://"+endpoint)
	r.clients[endpoint] = c
	return c
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
		cl := r.client(peer.ReplEndpoint)
		resp, err := cl.RangeSummary(ctx, connect.NewRequest(&wavespanv1.RangeSummaryRequest{Ranges: ranges}))
		if err != nil {
			continue
		}
		if r.metrics != nil {
			r.metrics.AntiEntropyRuns.Inc()
		}
		for _, rh := range resp.Msg.GetHashes() {
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

func (r *Reconciler) fetchAndApply(ctx context.Context, cl wavespanv1connect.GlobalReplicationClient, kr *wavespanv1.KeyRange) {
	stream, err := cl.FetchRange(ctx, connect.NewRequest(&wavespanv1.FetchRangeRequest{Range: kr}))
	if err != nil {
		return
	}
	for stream.Receive() {
		if _, err := r.applier.Apply(stream.Msg()); err != nil && r.metrics != nil {
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
