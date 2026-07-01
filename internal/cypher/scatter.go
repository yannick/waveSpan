package cypher

import (
	"context"

	"github.com/yannick/wavespan/internal/rpcopts"
	"github.com/yannick/wavespan/internal/vector"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
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
// per query so it tracks live membership; clients are cached per address. Peer Vector clients are
// dialled over gRPC via the rpcopts pooled connections.
func NewVectorScatter(self string, peers func() []Peer) ScatterFunc {
	clients := map[string]wavespanv1.VectorServiceClient{}
	return func(ctx context.Context, indexName string, query []float32, k, efSearch int, exact, rerank bool) ([][]vector.Hit, int) {
		var fragments [][]vector.Hit
		unreachable := 0
		for _, p := range peers() {
			if p.Member == self || p.DataAddr == "" {
				continue
			}
			c, ok := clients[p.DataAddr]
			if !ok {
				conn, err := rpcopts.GRPCConn(p.DataAddr)
				if err != nil {
					unreachable++
					continue
				}
				c = wavespanv1.NewVectorServiceClient(conn)
				clients[p.DataAddr] = c
			}
			resp, err := c.SearchLocal(ctx, &wavespanv1.SearchLocalRequest{
				IndexName: indexName, Query: query, K: int32(k), EfSearch: int32(efSearch), Exact: exact, Rerank: rerank,
			})
			if err != nil {
				unreachable++
				continue
			}
			fragments = append(fragments, hitsFromProto(resp.GetHits()))
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
