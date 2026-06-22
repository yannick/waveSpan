package local

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// PeerFetch fetches a peer's record for a key (FetchReplica). found is false when the peer has no
// record.
type PeerFetch func(ctx context.Context, dataAddr, namespace string, key []byte) (*wavespanv1.StoredRecord, bool)

// AECluster exposes the live roster to the intra-cluster anti-entropy worker.
type AECluster interface {
	Members() []membership.MemberView
}

// IntraAntiEntropy reconciles divergent versions of a key BETWEEN nodes of the same cluster
// (design/13 "anti-entropy and repair reconcile divergent state"). Origin+1 fanout is best-effort:
// if a holder misses the winning write of a concurrently-written key, nothing else updates it — its
// local read never misses and target-N repair only restores MISSING holders, not STALE ones. This
// pull-based pass scans local keys, fetches each from alive peers, and adopts the highest version
// (LWW), so concurrent same-key writers converge across all replicas.
type IntraAntiEntropy struct {
	store      *recordstore.Store
	self       membership.Member
	cluster    AECluster
	fetch      PeerFetch
	namespaces []string
	batch      int
}

// NewIntraAntiEntropy wires the worker. namespaces is the set whose keys are reconciled.
func NewIntraAntiEntropy(store *recordstore.Store, self membership.Member, cluster AECluster, fetch PeerFetch, namespaces []string) *IntraAntiEntropy {
	if len(namespaces) == 0 {
		namespaces = []string{"default"}
	}
	return &IntraAntiEntropy{store: store, self: self, cluster: cluster, fetch: fetch, namespaces: namespaces, batch: 512}
}

// ReconcileOnce runs one pass and returns the number of keys updated to a newer peer version.
func (a *IntraAntiEntropy) ReconcileOnce(ctx context.Context) int {
	peers := a.alivePeerAddrs()
	if len(peers) == 0 {
		return 0
	}
	updated := 0
	for _, ns := range a.namespaces {
		recs, err := a.store.ScanRecords(ns, nil, nil)
		if err != nil {
			continue
		}
		for i, rec := range recs {
			if i >= a.batch {
				break
			}
			best := rec
			bestVer := version.FromProto(rec.GetVersion())
			for _, addr := range peers {
				pr, found := a.fetch(ctx, addr, ns, rec.GetLogicalKey())
				if !found || pr == nil {
					continue
				}
				if v := version.FromProto(pr.GetVersion()); v.Compare(bestVer) > 0 {
					best, bestVer = pr, v
				}
			}
			if best != rec { // a peer had a newer version -> adopt it (LWW)
				kind := wavespanv1.MutationKind_MUTATION_KIND_PUT
				if best.GetTombstone() {
					kind = wavespanv1.MutationKind_MUTATION_KIND_DELETE
				}
				if _, err := a.store.Apply(best, kind); err == nil {
					updated++
				}
			}
		}
	}
	return updated
}

// Run reconciles on the given interval until ctx is done.
func (a *IntraAntiEntropy) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.ReconcileOnce(ctx)
		}
	}
}

func (a *IntraAntiEntropy) alivePeerAddrs() []string {
	var out []string
	for _, mv := range a.cluster.Members() {
		if mv.State == membership.StateAlive && mv.Member.MemberID != a.self.MemberID {
			out = append(out, mv.Member.DataAddr)
		}
	}
	return out
}

// NewConnectPeerFetch returns a PeerFetch over the ReplicationService FetchReplica RPC.
func NewConnectPeerFetch(hc *http.Client) PeerFetch {
	clients := map[string]wavespanv1connect.ReplicationServiceClient{}
	return func(ctx context.Context, dataAddr, ns string, key []byte) (*wavespanv1.StoredRecord, bool) {
		c, ok := clients[dataAddr]
		if !ok {
			c = wavespanv1connect.NewReplicationServiceClient(hc, "http://"+dataAddr)
			clients[dataAddr] = c
		}
		resp, err := c.FetchReplica(ctx, connect.NewRequest(&wavespanv1.FetchReplicaRequest{Namespace: ns, Key: key}))
		if err != nil {
			return nil, false
		}
		return resp.Msg.GetRecord(), resp.Msg.GetFound()
	}
}
