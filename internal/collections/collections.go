package collections

import (
	"bytes"
	"context"
)

// Collections is the typed datatype API over the consensus tier (design/30 §13.4-13.6): sets, hash
// tables, and sorted sets. It routes each op to its owning shard via the Directory and drives the
// RaftShard engine. Mutations that target a collection of a different datatype return ErrWrongType.
type Collections struct {
	shard RaftShard
	dir   Directory
}

// New builds the datatype API over a RaftShard engine and a shard Directory.
func New(shard RaftShard, dir Directory) *Collections {
	return &Collections{shard: shard, dir: dir}
}

func (c *Collections) propose(ctx context.Context, ns, coll []byte, cmd command) (uint64, error) {
	res, err := c.shard.Propose(ctx, c.dir.ShardFor(ns, coll), encodeCommand(cmd))
	if err != nil {
		return 0, err
	}
	if bytes.Equal(res.Data, wrongType) {
		return 0, ErrWrongType
	}
	return res.Value, nil
}

func (c *Collections) read(ctx context.Context, ns, coll []byte, q interface{}, lin bool) (interface{}, error) {
	return c.shard.Read(ctx, c.dir.ShardFor(ns, coll), q, lin)
}

// --- Set ---

func (c *Collections) SAdd(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.propose(ctx, ns, coll, command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

func (c *Collections) SRem(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.propose(ctx, ns, coll, command{Op: opSRem, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

func (c *Collections) SIsMember(ctx context.Context, ns, coll, member []byte, linearizable bool) (bool, error) {
	v, err := c.read(ctx, ns, coll, isMemberQuery{NS: ns, Coll: coll, Member: member}, linearizable)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

func (c *Collections) SCard(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	return c.card(ctx, ns, coll, linearizable)
}

func (c *Collections) SMembers(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([][]byte, error) {
	v, err := c.read(ctx, ns, coll, membersQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([][]byte)
	return out, nil
}

// --- Hash ---

func (c *Collections) HSet(ctx context.Context, ns, coll []byte, fields ...FieldValue) (uint64, error) {
	items := make([]item, len(fields))
	for i, f := range fields {
		items[i] = item{Key: f.Field, Val: f.Value}
	}
	return c.propose(ctx, ns, coll, command{Op: opHSet, NS: ns, Coll: coll, Items: items})
}

func (c *Collections) HDel(ctx context.Context, ns, coll []byte, fields ...[]byte) (uint64, error) {
	return c.propose(ctx, ns, coll, command{Op: opHDel, NS: ns, Coll: coll, Items: itemsFromKeys(fields)})
}

func (c *Collections) HGet(ctx context.Context, ns, coll, field []byte, linearizable bool) ([]byte, bool, error) {
	v, err := c.read(ctx, ns, coll, hGetQuery{NS: ns, Coll: coll, Field: field}, linearizable)
	if err != nil || v == nil {
		return nil, false, err
	}
	return v.([]byte), true, nil
}

func (c *Collections) HLen(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	return c.card(ctx, ns, coll, linearizable)
}

func (c *Collections) HGetAll(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([]FieldValue, error) {
	v, err := c.read(ctx, ns, coll, hGetAllQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([]FieldValue)
	return out, nil
}

// --- Sorted set ---

func (c *Collections) ZAdd(ctx context.Context, ns, coll []byte, members ...ScoredMember) (uint64, error) {
	items := make([]item, len(members))
	for i, m := range members {
		items[i] = item{Key: m.Member, Score: m.Score}
	}
	return c.propose(ctx, ns, coll, command{Op: opZAdd, NS: ns, Coll: coll, Items: items})
}

func (c *Collections) ZRem(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.propose(ctx, ns, coll, command{Op: opZRem, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

func (c *Collections) ZScore(ctx context.Context, ns, coll, member []byte, linearizable bool) (float64, bool, error) {
	v, err := c.read(ctx, ns, coll, zScoreQuery{NS: ns, Coll: coll, Member: member}, linearizable)
	if err != nil || v == nil {
		return 0, false, err
	}
	return *v.(*float64), true, nil
}

func (c *Collections) ZCard(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	return c.card(ctx, ns, coll, linearizable)
}

// ZRange returns members in ascending score order (limit 0 = all). Score-range / lex / cursor
// variants (design/30 §13.6, §13.8) are later milestones.
func (c *Collections) ZRange(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([]ScoredMember, error) {
	v, err := c.read(ctx, ns, coll, zRangeQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([]ScoredMember)
	return out, nil
}

// --- shared ---

func (c *Collections) card(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	v, err := c.read(ctx, ns, coll, cardQuery{NS: ns, Coll: coll}, linearizable)
	if err != nil {
		return 0, err
	}
	n, _ := v.(uint64)
	return n, nil
}

func itemsFromKeys(keys [][]byte) []item {
	items := make([]item, len(keys))
	for i, k := range keys {
		items[i] = item{Key: k}
	}
	return items
}
