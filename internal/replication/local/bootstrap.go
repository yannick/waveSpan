package local

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/recordstore"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// BackfillFetch pulls one page of a peer's records for a namespace (the Backfill RPC). It returns
// the records, the cursor to resume after (empty = end), and any error.
type BackfillFetch func(ctx context.Context, dataAddr, namespace string, cursor []byte, limit int) ([]*wavespanv1.StoredRecord, []byte, error)

const bootstrapPage = 512

// Bootstrapper streams the existing records of "everywhere"-replicated namespaces from a peer when a
// node joins, so a fresh node holds the full reference data instead of waiting for new writes
// (design/05 node sync). Applying is idempotent (LWW), so a restarted node re-syncing is harmless.
type Bootstrapper struct {
	store      *recordstore.Store
	self       membership.Member
	cluster    AECluster // Members()
	fetch      BackfillFetch
	namespaces []string
	applied    int
}

// NewBootstrapper wires the bootstrapper over the everywhere-namespace set.
func NewBootstrapper(store *recordstore.Store, self membership.Member, cluster AECluster, fetch BackfillFetch, everywhereNamespaces []string) *Bootstrapper {
	return &Bootstrapper{store: store, self: self, cluster: cluster, fetch: fetch, namespaces: everywhereNamespaces}
}

// BootstrapOnce streams every everywhere-namespace from the first reachable alive peer and applies
// the records locally. Returns the number of records applied.
func (b *Bootstrapper) BootstrapOnce(ctx context.Context) int {
	applied := 0
	for _, ns := range b.namespaces {
		var cursor []byte
		for ctx.Err() == nil {
			recs, next, ok := b.pageFromAnyPeer(ctx, ns, cursor)
			if !ok {
				break // no peer could serve this page; try again on the next Run tick
			}
			for _, rec := range recs {
				kind := wavespanv1.MutationKind_MUTATION_KIND_PUT
				if rec.GetTombstone() {
					kind = wavespanv1.MutationKind_MUTATION_KIND_DELETE
				}
				if _, err := b.store.Apply(rec, kind); err == nil {
					applied++
				}
			}
			if len(next) == 0 {
				break // end of namespace
			}
			cursor = next
		}
	}
	b.applied += applied
	return applied
}

// pageFromAnyPeer asks each alive peer in turn for one page until one answers.
func (b *Bootstrapper) pageFromAnyPeer(ctx context.Context, ns string, cursor []byte) ([]*wavespanv1.StoredRecord, []byte, bool) {
	for _, mv := range b.cluster.Members() {
		if mv.State != membership.StateAlive || mv.Member.MemberID == b.self.MemberID {
			continue
		}
		recs, next, err := b.fetch(ctx, mv.Member.DataAddr, ns, cursor, bootstrapPage)
		if err == nil {
			return recs, next, true
		}
	}
	return nil, nil, false
}

// Run waits for at least one peer, performs the initial bootstrap, and (since membership can form
// gradually) retries until at least one namespace was reachable, then exits.
func (b *Bootstrapper) Run(ctx context.Context, retry time.Duration) {
	if len(b.namespaces) == 0 {
		return
	}
	if retry <= 0 {
		retry = 2 * time.Second
	}
	t := time.NewTicker(retry)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if b.hasAlivePeer() {
				b.BootstrapOnce(ctx)
				return // one full pass is enough; ongoing fanout + anti-entropy keep it current
			}
		}
	}
}

func (b *Bootstrapper) hasAlivePeer() bool {
	for _, mv := range b.cluster.Members() {
		if mv.State == membership.StateAlive && mv.Member.MemberID != b.self.MemberID {
			return true
		}
	}
	return false
}

// NewConnectBackfill returns a BackfillFetch over the ReplicationService Backfill RPC.
func NewConnectBackfill(hc connect.HTTPClient) BackfillFetch {
	if hc == nil {
		hc = http.DefaultClient
	}
	clients := map[string]wavespanv1connect.ReplicationServiceClient{}
	return func(ctx context.Context, dataAddr, ns string, cursor []byte, limit int) ([]*wavespanv1.StoredRecord, []byte, error) {
		c, ok := clients[dataAddr]
		if !ok {
			c = wavespanv1connect.NewReplicationServiceClient(hc, "http://"+dataAddr)
			clients[dataAddr] = c
		}
		resp, err := c.Backfill(ctx, connect.NewRequest(&wavespanv1.BackfillRequest{Namespace: ns, Cursor: cursor, Limit: uint32(limit)}))
		if err != nil {
			return nil, nil, err
		}
		return resp.Msg.GetRecords(), resp.Msg.GetNextCursor(), nil
	}
}
