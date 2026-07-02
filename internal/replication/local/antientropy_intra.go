package local

import (
	"bytes"
	"context"
	"time"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// PeerFetch fetches a peer's record for a key (FetchReplica). found is false when the peer has no
// record.
type PeerFetch func(ctx context.Context, dataAddr, namespace string, key []byte) (*wavespanv1.StoredRecord, bool)

// PeerDigest fetches a peer's content hash for a key range (RangeDigest, design/37 P2.11).
// ok=false means the peer can't answer (old version, transport error) — fall back to per-key
// fetches for that peer.
type PeerDigest func(ctx context.Context, dataAddr, namespace string, start, end []byte) (digest []byte, count uint64, ok bool)

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
	digest     PeerDigest // optional: digest-gate the per-key fetches (design/37 P2.11)
	namespaces []string
	batch      int
	cursor     map[string][]byte // per-namespace resume point for the incremental sweep
}

// NewIntraAntiEntropy wires the worker. namespaces is the set whose keys are reconciled.
func NewIntraAntiEntropy(store *recordstore.Store, self membership.Member, cluster AECluster, fetch PeerFetch, namespaces []string) *IntraAntiEntropy {
	if len(namespaces) == 0 {
		namespaces = []string{"default"}
	}
	return &IntraAntiEntropy{store: store, self: self, cluster: cluster, fetch: fetch, namespaces: namespaces, batch: 256, cursor: map[string][]byte{}}
}

// WithDigest enables the digest phase: before per-key fetching a batch from a peer, compare a
// range digest and skip the peer entirely when it matches (one RPC instead of batch-many).
func (a *IntraAntiEntropy) WithDigest(d PeerDigest) *IntraAntiEntropy {
	a.digest = d
	return a
}

// ReconcileOnce runs ONE bounded, incremental pass: it scans at most `batch` keys per namespace from
// a rolling cursor (not the whole keyspace), reconciles them against peers, and advances the cursor
// so successive passes sweep the namespace over time. This caps per-tick work + allocation at
// O(batch × peers) instead of O(all keys × peers) every tick. Returns the number of keys updated.
func (a *IntraAntiEntropy) ReconcileOnce(ctx context.Context) int {
	peers := a.alivePeerAddrs()
	if len(peers) == 0 {
		return 0
	}
	updated := 0
	for _, ns := range a.namespaces {
		start := a.cursor[ns]
		recs, next, err := a.store.ScanRecordsFrom(ns, start, a.batch)
		if err != nil {
			continue
		}
		a.cursor[ns] = next // nil => start over from the top of the namespace next sweep

		// Digest phase (design/37 P2.11): the local batch covers exactly [start, next), so a peer
		// whose RangeDigest over that range matches ours holds identical (key, version) content —
		// skip its per-key fetches entirely. A peer that can't answer (old node, transport error)
		// falls back to the fetch phase. Without a digest fn every peer is fetched (old behavior).
		fetchPeers := peers
		if a.digest != nil && len(recs) > 0 {
			localDigest := DigestRecords(recs)
			fetchPeers = fetchPeers[:0:0]
			for _, addr := range peers {
				if pd, _, ok := a.digest(ctx, addr, ns, start, next); ok && bytes.Equal(pd, localDigest) {
					continue // converged with this peer for this range
				}
				fetchPeers = append(fetchPeers, addr)
			}
		}
		if len(fetchPeers) == 0 {
			continue
		}

		for _, rec := range recs {
			best := rec
			bestVer := version.FromProto(rec.GetVersion())
			for _, addr := range fetchPeers {
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

// The production PeerFetch is ConnectReplicator.PeerFetch (see connect.go), which dials the data
// port over gRPC. An earlier Connect-wire PeerFetch here was a silent no-op: the data port is a pure
// grpc-go server that does not route the Connect wire, so every FetchReplica failed at the transport
// layer and anti-entropy never converged divergent replicas.
