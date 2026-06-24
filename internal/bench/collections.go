package bench

import (
	"context"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// CollectionsClient builds a gRPC CollectionService client for addr (a data port), dialled over the
// rpcopts pooled connections.
func CollectionsClient(addr string) wavespanv1.CollectionServiceClient {
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		panic(err)
	}
	return wavespanv1.NewCollectionServiceClient(conn)
}

// CollectionsClientLong is the long-call variant for the whole-namespace BulkRemove fan-out (which
// proposes per collection and can run far longer than 30s). gRPC has no hard client request timeout —
// calls are bounded only by their context deadline — so it collapses onto the same pooled conn as
// CollectionsClient.
func CollectionsClientLong(addr string) wavespanv1.CollectionServiceClient {
	return CollectionsClient(addr)
}

// --- set ---

// OpSAdd adds members to a set.
func OpSAdd(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SAdd(ctx, &wavespanv1.SAddRequest{Namespace: ns, Collection: coll, Members: members})
	return err
}

// OpSRem removes members from a set.
func OpSRem(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SRem(ctx, &wavespanv1.KeysRequest{Namespace: ns, Collection: coll, Keys: members})
	return err
}

// OpSIsMember tests set membership.
func OpSIsMember(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.SIsMember(ctx, &wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member})
	return err
}

// OpSCard returns set cardinality.
func OpSCard(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll []byte) error {
	_, err := c.SCard(ctx, &wavespanv1.CardRequest{Namespace: ns, Collection: coll})
	return err
}

// --- hash (incl. HIncrBy atomic counter) ---

// OpHSet sets one hash field.
func OpHSet(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, field, value []byte) error {
	_, err := c.HSet(ctx, &wavespanv1.HSetRequest{Namespace: ns, Collection: coll,
		Fields: []*wavespanv1.FieldValue{{Field: field, Value: value}}})
	return err
}

// OpHGet reads one hash field.
func OpHGet(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, field []byte) error {
	_, err := c.HGet(ctx, &wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: field})
	return err
}

// OpHIncrBy atomically increments an integer counter field.
func OpHIncrBy(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, field []byte, delta int64) error {
	_, err := c.HIncrBy(ctx, &wavespanv1.HIncrByRequest{Namespace: ns, Collection: coll, Field: field, Delta: delta})
	return err
}

// --- sorted set ---

// OpZAdd adds a scored member.
func OpZAdd(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, member []byte, score float64) error {
	_, err := c.ZAdd(ctx, &wavespanv1.ZAddRequest{Namespace: ns, Collection: coll,
		Members: []*wavespanv1.ScoredMember{{Member: member, Score: score}}})
	return err
}

// OpZScore reads a member's score.
func OpZScore(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.ZScore(ctx, &wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member})
	return err
}

// OpBulkRemove removes members from the given collections (empty colls = every collection in the
// namespace). Returns the number of collections the fan-out touched (for sets/sec) and the total
// removed. NOTE: named return `count` avoids shadowing the `colls` parameter.
func OpBulkRemove(ctx context.Context, c wavespanv1.CollectionServiceClient, ns string, colls, members [][]byte) (count int, removed uint64, err error) {
	resp, e := c.BulkRemove(ctx, &wavespanv1.BulkRemoveRequest{Namespace: ns, Collections: colls, Members: members})
	if e != nil {
		return 0, 0, e
	}
	for _, r := range resp.GetResults() {
		removed += r.GetRemoved()
	}
	return len(resp.GetResults()), removed, nil
}
