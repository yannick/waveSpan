package bench

import (
	"context"
	"fmt"
	"io"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// OpKVRead does one Get (matches kv.go's read branch).
func OpKVRead(ctx context.Context, c wavespanv1.KvServiceClient, ns, key string) error {
	_, err := c.Get(ctx, &wavespanv1.GetRequest{Namespace: ns, Key: []byte(key)})
	return err
}

// OpKVWrite does one origin+1 Put (matches kv.go's write branch).
func OpKVWrite(ctx context.Context, c wavespanv1.KvServiceClient, ns, key string, value []byte) error {
	_, err := c.Put(ctx, &wavespanv1.PutRequest{
		Namespace: ns, Key: []byte(key), Value: value, RequireOriginPlusOne: true,
	})
	return err
}

// OpMultiGet fetches a batch (matches multiget.go).
func OpMultiGet(ctx context.Context, c wavespanv1.KvServiceClient, ns string, keys [][]byte) error {
	_, err := c.MultiGet(ctx, &wavespanv1.MultiGetRequest{Namespace: ns, Keys: keys})
	return err
}

// OpCypher runs one query and drains the stream (matches query.go / load.go execCypher).
func OpCypher(ctx context.Context, c wavespanv1.CypherClient, graph, query string) error {
	stream, err := c.Query(ctx, &wavespanv1.CypherRequest{GraphId: graph, Query: query})
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// OpCreateNode / OpCreateEdge wrap OpCypher for the loader's CREATE statements (load.go:41,56).
func OpCreateNode(ctx context.Context, c wavespanv1.CypherClient, graph string, i int, city string) error {
	return OpCypher(ctx, c, graph, fmt.Sprintf("CREATE (:User {id:'user-%d', name:'User %d', age:%d, city:'%s'})", i, i, 18+i%60, city))
}

// OpCreateEdge creates one FOLLOWS edge between two users (the loader's edge statement).
func OpCreateEdge(ctx context.Context, c wavespanv1.CypherClient, graph string, a, b int) error {
	return OpCypher(ctx, c, graph, fmt.Sprintf("MATCH (a:User {id:'user-%d'}), (b:User {id:'user-%d'}) CREATE (a)-[:FOLLOWS]->(b)", a, b))
}
