package bench

import (
	"context"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// CollectionsClient builds an H2C CollectionService client for addr (a data port).
func CollectionsClient(addr string) wavespanv1connect.CollectionServiceClient {
	return wavespanv1connect.NewCollectionServiceClient(rpcopts.H2CClient(), "http://"+addr)
}

// CollectionsClientLong is like CollectionsClient but with no hard request timeout, for the
// whole-namespace BulkRemove fan-out (which proposes per collection and can run far longer than the
// shared 30s-capped client allows). The call is bounded by its context deadline instead.
func CollectionsClientLong(addr string) wavespanv1connect.CollectionServiceClient {
	return wavespanv1connect.NewCollectionServiceClient(rpcopts.H2CClientNoTimeout(), "http://"+addr)
}

// --- set ---

// OpSAdd adds members to a set.
func OpSAdd(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SAdd(ctx, connect.NewRequest(&wavespanv1.SAddRequest{Namespace: ns, Collection: coll, Members: members}))
	return err
}

// OpSRem removes members from a set.
func OpSRem(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte, members ...[]byte) error {
	_, err := c.SRem(ctx, connect.NewRequest(&wavespanv1.KeysRequest{Namespace: ns, Collection: coll, Keys: members}))
	return err
}

// OpSIsMember tests set membership.
func OpSIsMember(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.SIsMember(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member}))
	return err
}

// OpSCard returns set cardinality.
func OpSCard(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll []byte) error {
	_, err := c.SCard(ctx, connect.NewRequest(&wavespanv1.CardRequest{Namespace: ns, Collection: coll}))
	return err
}

// --- hash (incl. HIncrBy atomic counter) ---

// OpHSet sets one hash field.
func OpHSet(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field, value []byte) error {
	_, err := c.HSet(ctx, connect.NewRequest(&wavespanv1.HSetRequest{Namespace: ns, Collection: coll,
		Fields: []*wavespanv1.FieldValue{{Field: field, Value: value}}}))
	return err
}

// OpHGet reads one hash field.
func OpHGet(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field []byte) error {
	_, err := c.HGet(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: field}))
	return err
}

// OpHIncrBy atomically increments an integer counter field.
func OpHIncrBy(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, field []byte, delta int64) error {
	_, err := c.HIncrBy(ctx, connect.NewRequest(&wavespanv1.HIncrByRequest{Namespace: ns, Collection: coll, Field: field, Delta: delta}))
	return err
}

// --- sorted set ---

// OpZAdd adds a scored member.
func OpZAdd(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte, score float64) error {
	_, err := c.ZAdd(ctx, connect.NewRequest(&wavespanv1.ZAddRequest{Namespace: ns, Collection: coll,
		Members: []*wavespanv1.ScoredMember{{Member: member, Score: score}}}))
	return err
}

// OpZScore reads a member's score.
func OpZScore(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, coll, member []byte) error {
	_, err := c.ZScore(ctx, connect.NewRequest(&wavespanv1.MemberRequest{Namespace: ns, Collection: coll, Member: member}))
	return err
}

// OpBulkRemove removes members from the given collections (empty colls = every collection in the
// namespace). Returns the number of collections the fan-out touched (for sets/sec) and the total
// removed. NOTE: named return `count` avoids shadowing the `colls` parameter.
func OpBulkRemove(ctx context.Context, c wavespanv1connect.CollectionServiceClient, ns string, colls, members [][]byte) (count int, removed uint64, err error) {
	resp, e := c.BulkRemove(ctx, connect.NewRequest(&wavespanv1.BulkRemoveRequest{Namespace: ns, Collections: colls, Members: members}))
	if e != nil {
		return 0, 0, e
	}
	for _, r := range resp.Msg.GetResults() {
		removed += r.GetRemoved()
	}
	return len(resp.Msg.GetResults()), removed, nil
}
