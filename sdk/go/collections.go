package wavespan

import (
	"context"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/yannick/wavespan/sdk/go/internal/gen/wavespan/v1"
)

// CollectionsClient is the ergonomic client for the replicated-collections API (design/30): sets, hash
// tables, and sorted sets over the CP consensus tier. Writes are linearizable; reads default to
// bounded-stale local reads — pass linearizable=true for a quorum read. Obtain one via
// [Client.Collections].
type CollectionsClient struct{ c *Client }

// Collections returns the replicated-collections sub-client.
func (c *Client) Collections() *CollectionsClient { return &CollectionsClient{c: c} }

// FieldValue is a hash field/value pair.
type FieldValue struct {
	Field []byte
	Value []byte
}

// ScoredMember is a sorted-set member and its score.
type ScoredMember struct {
	Member []byte
	Score  float64
}

// --- Set ---

// SAdd adds members to the set, returning the number newly added.
func (cc *CollectionsClient) SAdd(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.SAdd(ctx, connect.NewRequest(&wavespanv1.SAddRequest{
		Namespace: namespace, Collection: collection, Members: members,
	}))
	if err != nil {
		return 0, wrapErr("SAdd", err)
	}
	return resp.Msg.GetCount(), nil
}

// SAddTTL adds members that expire after ttl, returning the number newly added.
func (cc *CollectionsClient) SAddTTL(ctx context.Context, namespace string, collection []byte, ttl time.Duration, members ...[]byte) (uint64, error) {
	ms := ttl.Milliseconds()
	resp, err := cc.c.collections.SAdd(ctx, connect.NewRequest(&wavespanv1.SAddRequest{
		Namespace: namespace, Collection: collection, Members: members, TtlMs: &ms,
	}))
	if err != nil {
		return 0, wrapErr("SAddTTL", err)
	}
	return resp.Msg.GetCount(), nil
}

// SRem removes members from the set, returning the number removed.
func (cc *CollectionsClient) SRem(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.SRem(ctx, connect.NewRequest(&wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: members,
	}))
	if err != nil {
		return 0, wrapErr("SRem", err)
	}
	return resp.Msg.GetCount(), nil
}

// SIsMember reports whether member is in the set.
func (cc *CollectionsClient) SIsMember(ctx context.Context, namespace string, collection, member []byte, linearizable bool) (bool, error) {
	resp, err := cc.c.collections.SIsMember(ctx, connect.NewRequest(&wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: member, Linearizable: linearizable,
	}))
	if err != nil {
		return false, wrapErr("SIsMember", err)
	}
	return resp.Msg.GetValue(), nil
}

// SCard returns the set cardinality.
func (cc *CollectionsClient) SCard(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.SCard(ctx, connect.NewRequest(&wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	}))
	if err != nil {
		return 0, wrapErr("SCard", err)
	}
	return resp.Msg.GetCount(), nil
}

// SMembers returns up to limit set members (0 = all).
func (cc *CollectionsClient) SMembers(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([][]byte, error) {
	resp, err := cc.c.collections.SMembers(ctx, connect.NewRequest(&wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	}))
	if err != nil {
		return nil, wrapErr("SMembers", err)
	}
	return resp.Msg.GetMembers(), nil
}

// --- Hash ---

// HSet sets hash fields, returning the number of new fields.
func (cc *CollectionsClient) HSet(ctx context.Context, namespace string, collection []byte, fields ...FieldValue) (uint64, error) {
	pb := make([]*wavespanv1.FieldValue, len(fields))
	for i, f := range fields {
		pb[i] = &wavespanv1.FieldValue{Field: f.Field, Value: f.Value}
	}
	resp, err := cc.c.collections.HSet(ctx, connect.NewRequest(&wavespanv1.HSetRequest{
		Namespace: namespace, Collection: collection, Fields: pb,
	}))
	if err != nil {
		return 0, wrapErr("HSet", err)
	}
	return resp.Msg.GetCount(), nil
}

// HDel deletes hash fields, returning the number removed.
func (cc *CollectionsClient) HDel(ctx context.Context, namespace string, collection []byte, fields ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.HDel(ctx, connect.NewRequest(&wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: fields,
	}))
	if err != nil {
		return 0, wrapErr("HDel", err)
	}
	return resp.Msg.GetCount(), nil
}

// HGet returns a hash field value and whether it was present.
func (cc *CollectionsClient) HGet(ctx context.Context, namespace string, collection, field []byte, linearizable bool) ([]byte, bool, error) {
	resp, err := cc.c.collections.HGet(ctx, connect.NewRequest(&wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: field, Linearizable: linearizable,
	}))
	if err != nil {
		return nil, false, wrapErr("HGet", err)
	}
	return resp.Msg.GetValue(), resp.Msg.GetFound(), nil
}

// HLen returns the number of hash fields.
func (cc *CollectionsClient) HLen(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.HLen(ctx, connect.NewRequest(&wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	}))
	if err != nil {
		return 0, wrapErr("HLen", err)
	}
	return resp.Msg.GetCount(), nil
}

// HGetAll returns up to limit hash field/value pairs (0 = all).
func (cc *CollectionsClient) HGetAll(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([]FieldValue, error) {
	resp, err := cc.c.collections.HGetAll(ctx, connect.NewRequest(&wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	}))
	if err != nil {
		return nil, wrapErr("HGetAll", err)
	}
	out := make([]FieldValue, len(resp.Msg.GetFields()))
	for i, f := range resp.Msg.GetFields() {
		out[i] = FieldValue{Field: f.GetField(), Value: f.GetValue()}
	}
	return out, nil
}

// --- Sorted set ---

// ZAdd adds or updates sorted-set members, returning the number newly added.
func (cc *CollectionsClient) ZAdd(ctx context.Context, namespace string, collection []byte, members ...ScoredMember) (uint64, error) {
	pb := make([]*wavespanv1.ScoredMember, len(members))
	for i, m := range members {
		pb[i] = &wavespanv1.ScoredMember{Member: m.Member, Score: m.Score}
	}
	resp, err := cc.c.collections.ZAdd(ctx, connect.NewRequest(&wavespanv1.ZAddRequest{
		Namespace: namespace, Collection: collection, Members: pb,
	}))
	if err != nil {
		return 0, wrapErr("ZAdd", err)
	}
	return resp.Msg.GetCount(), nil
}

// ZRem removes sorted-set members, returning the number removed.
func (cc *CollectionsClient) ZRem(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.ZRem(ctx, connect.NewRequest(&wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: members,
	}))
	if err != nil {
		return 0, wrapErr("ZRem", err)
	}
	return resp.Msg.GetCount(), nil
}

// ZScore returns a member's score and whether it was present.
func (cc *CollectionsClient) ZScore(ctx context.Context, namespace string, collection, member []byte, linearizable bool) (float64, bool, error) {
	resp, err := cc.c.collections.ZScore(ctx, connect.NewRequest(&wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: member, Linearizable: linearizable,
	}))
	if err != nil {
		return 0, false, wrapErr("ZScore", err)
	}
	return resp.Msg.GetScore(), resp.Msg.GetFound(), nil
}

// ZCard returns the sorted-set cardinality.
func (cc *CollectionsClient) ZCard(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.ZCard(ctx, connect.NewRequest(&wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	}))
	if err != nil {
		return 0, wrapErr("ZCard", err)
	}
	return resp.Msg.GetCount(), nil
}

// ZRange returns members in ascending score order (limit 0 = all).
func (cc *CollectionsClient) ZRange(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([]ScoredMember, error) {
	resp, err := cc.c.collections.ZRange(ctx, connect.NewRequest(&wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	}))
	if err != nil {
		return nil, wrapErr("ZRange", err)
	}
	out := make([]ScoredMember, len(resp.Msg.GetMembers()))
	for i, m := range resp.Msg.GetMembers() {
		out[i] = ScoredMember{Member: m.GetMember(), Score: m.GetScore()}
	}
	return out, nil
}
