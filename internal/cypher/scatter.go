package cypher

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/cwire/wavespan/internal/vector"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// Peer is a cluster member a vector query can scatter to.
type Peer struct {
	Member   string
	DataAddr string
}

// ScatterFunc queries holder peers and returns their per-node search fragments plus the count of
// holders that were unreachable (for honest partial flagging).
type ScatterFunc func(ctx context.Context, indexName string, query []float32, k, efSearch int, exact, rerank bool) ([][]vector.Hit, int)

// NewVectorScatter builds the scatter closure used by the executor's distributed vector search
// (design/08): for each alive peer other than self it calls VectorService.SearchLocal and collects
// the returned fragment. A peer that errors is counted unreachable, not fatal. peers() is evaluated
// per query so it tracks live membership; clients are cached per address.
func NewVectorScatter(self string, peers func() []Peer, hc *http.Client) ScatterFunc {
	if hc == nil {
		hc = http.DefaultClient
	}
	clients := map[string]wavespanv1connect.VectorServiceClient{}
	return func(ctx context.Context, indexName string, query []float32, k, efSearch int, exact, rerank bool) ([][]vector.Hit, int) {
		var fragments [][]vector.Hit
		unreachable := 0
		for _, p := range peers() {
			if p.Member == self || p.DataAddr == "" {
				continue
			}
			c, ok := clients[p.DataAddr]
			if !ok {
				c = wavespanv1connect.NewVectorServiceClient(hc, "http://"+p.DataAddr)
				clients[p.DataAddr] = c
			}
			resp, err := c.SearchLocal(ctx, connect.NewRequest(&wavespanv1.SearchLocalRequest{
				IndexName: indexName, Query: query, K: int32(k), EfSearch: int32(efSearch), Exact: exact, Rerank: rerank,
			}))
			if err != nil {
				unreachable++
				continue
			}
			fragments = append(fragments, hitsFromProto(resp.Msg.GetHits()))
		}
		return fragments, unreachable
	}
}

func hitsFromProto(in []*wavespanv1.VectorHit) []vector.Hit {
	out := make([]vector.Hit, 0, len(in))
	for _, h := range in {
		out = append(out, vector.Hit{
			Collection: h.GetCollection(), VectorID: h.GetVectorId(), GraphNodeID: h.GetGraphNodeId(),
			Distance: h.GetDistance(), Score: h.GetScore(),
		})
	}
	return out
}
