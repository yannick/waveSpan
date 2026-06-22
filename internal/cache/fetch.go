package cache

import (
	"context"
	"net/http"
	"sync"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/latencygraph"
	"github.com/cwire/wavespan/internal/membership"
	"github.com/cwire/wavespan/internal/rpcopts"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Cluster exposes the live roster (satisfied by membership.Service).
type Cluster interface {
	Members() []membership.MemberView
}

// FetchResult is the outcome of a closest-holder fetch.
type FetchResult struct {
	Found  bool
	Record *wavespanv1.StoredRecord
	Source string // member id that served the record
	Offer  *wavespanv1.SubscriptionOffer
}

// Fetcher resolves the closest holder of a key from the gossiped directory and the latency
// graph, then FetchReplicas from it (design/05 "Dynamic cache read path"). It never broadcasts.
type Fetcher struct {
	self       membership.Member
	dir        *Directory
	cluster    Cluster
	graph      *latencygraph.Graph
	httpClient connect.HTTPClient
	mu         sync.Mutex
	clients    map[string]wavespanv1connect.ReplicationServiceClient
}

// NewFetcher wires a fetcher.
func NewFetcher(self membership.Member, dir *Directory, cluster Cluster, graph *latencygraph.Graph, hc *http.Client) *Fetcher {
	var c connect.HTTPClient = rpcopts.H2CClient()
	if hc != nil {
		c = hc
	}
	return &Fetcher{self: self, dir: dir, cluster: cluster, graph: graph, httpClient: c, clients: map[string]wavespanv1connect.ReplicationServiceClient{}}
}

func (f *Fetcher) client(addr string) wavespanv1connect.ReplicationServiceClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.clients[addr]; ok {
		return c
	}
	c := wavespanv1connect.NewReplicationServiceClient(f.httpClient, "http://"+addr)
	f.clients[addr] = c
	return c
}

// memberByID returns the live member with the given id.
func (f *Fetcher) memberByID(id string) (membership.Member, bool) {
	for _, mv := range f.cluster.Members() {
		if mv.Member.MemberID == id && mv.State == membership.StateAlive {
			return mv.Member, true
		}
	}
	return membership.Member{}, false
}

// orderHoldersByLatency sorts candidate holder member ids closest-first.
func (f *Fetcher) orderHoldersByLatency(ids []string) []membership.Member {
	var members []membership.Member
	for _, id := range ids {
		if m, ok := f.memberByID(id); ok {
			members = append(members, m)
		}
	}
	// stable insertion sort by latency score (small N)
	for i := 1; i < len(members); i++ {
		for j := i; j > 0 && f.graph.Score(members[j].MemberID, 0, 0, 0) < f.graph.Score(members[j-1].MemberID, 0, 0, 0); j-- {
			members[j], members[j-1] = members[j-1], members[j]
		}
	}
	return members
}

// Fetch resolves holders for (namespace, key) and fetches the record from the closest reachable
// one, trying alternates on failure. Found is false when no holder serves the record.
func (f *Fetcher) Fetch(ctx context.Context, namespace string, key []byte) (FetchResult, error) {
	holderIDs := f.dir.ResolveHolders(namespace, key)
	candidates := f.orderHoldersByLatency(holderIDs)
	req := connect.NewRequest(&wavespanv1.FetchReplicaRequest{Namespace: namespace, Key: key, WantSubscriptionOffer: true})

	for _, m := range candidates {
		resp, err := f.client(m.DataAddr).FetchReplica(ctx, req)
		if err != nil {
			continue // try the next holder
		}
		if resp.Msg.GetFound() {
			return FetchResult{Found: true, Record: resp.Msg.GetRecord(), Source: m.MemberID, Offer: resp.Msg.GetSubscriptionOffer()}, nil
		}
		// holder did not have it: follow alternate holder hints if any
		for _, alt := range resp.Msg.GetAlternateHolderMemberIds() {
			if am, ok := f.memberByID(alt); ok {
				if r2, e2 := f.client(am.DataAddr).FetchReplica(ctx, req); e2 == nil && r2.Msg.GetFound() {
					return FetchResult{Found: true, Record: r2.Msg.GetRecord(), Source: am.MemberID}, nil
				}
			}
		}
	}
	return FetchResult{Found: false}, nil
}
