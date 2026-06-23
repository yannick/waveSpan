package collections

import (
	"bytes"
	"context"
	"errors"
	"time"
)

// Collections is the typed datatype API over the consensus tier (design/30 §13.4-13.6): sets, hash
// tables, and sorted sets. It routes each op to its owning shard via the Directory and drives the
// RaftShard engine. Mutations that target a collection of a different datatype return ErrWrongType.
type Collections struct {
	shard     RaftShard
	dir       Directory
	filler    *DemandFiller // optional: join shards as a learner on a not-hosted read (design/30 §9)
	forwarder Forwarder     // optional: forward a write to the leader node when not local leader (§13.13)
}

// Forwarder forwards an already-encoded write to a peer when this node is not the owning shard's
// leader (node-side leader routing, design/30 §13.13). It returns the peer's apply result, or
// ErrWrongType. Implemented over CollectionService.ProposeForward; nil = no forwarding.
type Forwarder interface {
	Forward(ctx context.Context, ns, coll, cmd []byte) (uint64, error)
}

// New builds the datatype API over a RaftShard engine and a shard Directory.
func New(shard RaftShard, dir Directory) *Collections {
	return &Collections{shard: shard, dir: dir}
}

// WithDemandFill enables auto demand-fill: a read for a collection whose shard this node does not host
// triggers a learner join (via f) and a local retry, so the node becomes a dynamically-filling cache.
func (c *Collections) WithDemandFill(f *DemandFiller) *Collections {
	c.filler = f
	return c
}

// WithForwarder enables node-side leader routing: a write this node can't commit (it isn't the owning
// shard's leader) is forwarded to a peer until the leader accepts, so clients can call any node.
func (c *Collections) WithForwarder(f Forwarder) *Collections {
	c.forwarder = f
	return c
}

// proposeCmd commits a write. With a forwarder, a node that isn't the owning shard's leader forwards
// the write to a peer instead of issuing a (blocking) local propose that would just fail; if we believe
// we're the leader but lose it mid-propose, we fall back to forwarding (design/30 §13.13).
func (c *Collections) proposeCmd(ctx context.Context, cmd command) (uint64, error) {
	enc := encodeCommand(cmd)
	if c.forwarder == nil {
		return c.proposeLocal(ctx, cmd.NS, cmd.Coll, enc)
	}
	if c.shard.IsLeader(c.dir.ShardFor(cmd.NS, cmd.Coll)) {
		n, err := c.proposeLocal(ctx, cmd.NS, cmd.Coll, enc)
		if err == nil || !forwardable(ctx, err) {
			return n, err
		}
		// raced (lost leadership) — fall through and forward.
	}
	return c.forwarder.Forward(ctx, cmd.NS, cmd.Coll, enc)
}

// ProposeRaw applies an already-encoded write locally only (never forwards) — the target a peer's
// ProposeForward calls. Returns ErrWrongType / ErrFrozen like the typed methods.
func (c *Collections) ProposeRaw(ctx context.Context, ns, coll, enc []byte) (uint64, error) {
	return c.proposeLocal(ctx, ns, coll, enc)
}

func (c *Collections) proposeLocal(ctx context.Context, ns, coll, enc []byte) (uint64, error) {
	for {
		res, err := c.shard.Propose(ctx, c.dir.ShardFor(ns, coll), enc)
		if err != nil {
			return 0, err
		}
		if bytes.Equal(res.Data, wrongType) {
			return 0, ErrWrongType
		}
		if bytes.Equal(res.Data, frozenMark) {
			// The owning subrange is migrating (split). Refresh routing and retry until the directory cuts
			// over to the new shard, so the write is never lost — only briefly delayed (design/30 §6.1).
			if r, ok := c.dir.(*RangeDirectory); ok {
				_ = r.Refresh(ctx)
			}
			select {
			case <-ctx.Done():
				return 0, ErrFrozen
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		return res.Value, nil
	}
}

// forwardable reports whether a local propose error should be retried on a peer (it indicates this node
// can't act as leader). Definitive results (WRONGTYPE, FROZEN) and a cancelled context are not.
func forwardable(ctx context.Context, err error) bool {
	if errors.Is(err, ErrWrongType) || errors.Is(err, ErrFrozen) {
		return false
	}
	return ctx.Err() == nil
}

func (c *Collections) read(ctx context.Context, ns, coll []byte, q interface{}, lin bool) (interface{}, error) {
	shard := c.dir.ShardFor(ns, coll)
	v, err := c.shard.Read(ctx, shard, q, lin)
	if err != nil && c.filler != nil && errors.Is(err, ErrNotHosted) {
		if ferr := c.filler.Fill(ctx, shard); ferr != nil {
			return nil, err // surface the original not-hosted error when the fill fails
		}
		return c.shard.Read(ctx, shard, q, lin) // retry locally now that we host the shard
	}
	return v, err
}

// --- Set ---

// SAdd adds members to the set, returning the number newly added.
func (c *Collections) SAdd(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.proposeCmd(ctx, command{Op: opSAdd, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

// SRem removes members from the set, returning the number removed.
func (c *Collections) SRem(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.proposeCmd(ctx, command{Op: opSRem, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

// SAddTTL adds members that expire after ttlMs. The absolute expiry is stamped here (before propose)
// so every replica applies the same deterministic time; the leader's sweeper deletes them when due
// (design/30 §10).
func (c *Collections) SAddTTL(ctx context.Context, ns, coll []byte, ttlMs int64, members ...[]byte) (uint64, error) {
	expiry := time.Now().UnixMilli() + ttlMs
	items := make([]item, len(members))
	for i, m := range members {
		items[i] = item{Key: m, ExpiryMs: expiry}
	}
	return c.proposeCmd(ctx, command{Op: opSAdd, NS: ns, Coll: coll, Items: items})
}

// SIsMember reports whether member is in the set.
func (c *Collections) SIsMember(ctx context.Context, ns, coll, member []byte, linearizable bool) (bool, error) {
	v, err := c.read(ctx, ns, coll, isMemberQuery{NS: ns, Coll: coll, Member: member}, linearizable)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

// SCard returns the set cardinality.
func (c *Collections) SCard(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	return c.card(ctx, ns, coll, linearizable)
}

// SMembers returns up to limit set members in byte order (0 = all).
func (c *Collections) SMembers(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([][]byte, error) {
	v, err := c.read(ctx, ns, coll, membersQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([][]byte)
	return out, nil
}

// --- Hash ---

// HSet sets hash fields, returning the number of new (not updated) fields.
func (c *Collections) HSet(ctx context.Context, ns, coll []byte, fields ...FieldValue) (uint64, error) {
	items := make([]item, len(fields))
	for i, f := range fields {
		items[i] = item{Key: f.Field, Val: f.Value}
	}
	return c.proposeCmd(ctx, command{Op: opHSet, NS: ns, Coll: coll, Items: items})
}

// HDel deletes hash fields, returning the number removed.
func (c *Collections) HDel(ctx context.Context, ns, coll []byte, fields ...[]byte) (uint64, error) {
	return c.proposeCmd(ctx, command{Op: opHDel, NS: ns, Coll: coll, Items: itemsFromKeys(fields)})
}

// HGet returns a hash field's value and whether it was present.
func (c *Collections) HGet(ctx context.Context, ns, coll, field []byte, linearizable bool) ([]byte, bool, error) {
	v, err := c.read(ctx, ns, coll, hGetQuery{NS: ns, Coll: coll, Field: field}, linearizable)
	if err != nil || v == nil {
		return nil, false, err
	}
	return v.([]byte), true, nil
}

// HLen returns the number of hash fields.
func (c *Collections) HLen(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	return c.card(ctx, ns, coll, linearizable)
}

// HGetAll returns up to limit hash field/value pairs (0 = all).
func (c *Collections) HGetAll(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([]FieldValue, error) {
	v, err := c.read(ctx, ns, coll, hGetAllQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([]FieldValue)
	return out, nil
}

// --- Sorted set ---

// ZAdd adds or updates sorted-set members, returning the number newly added.
func (c *Collections) ZAdd(ctx context.Context, ns, coll []byte, members ...ScoredMember) (uint64, error) {
	items := make([]item, len(members))
	for i, m := range members {
		items[i] = item{Key: m.Member, Score: m.Score}
	}
	return c.proposeCmd(ctx, command{Op: opZAdd, NS: ns, Coll: coll, Items: items})
}

// ZRem removes sorted-set members, returning the number removed.
func (c *Collections) ZRem(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.proposeCmd(ctx, command{Op: opZRem, NS: ns, Coll: coll, Items: itemsFromKeys(members)})
}

// ZScore returns a member's score and whether it was present.
func (c *Collections) ZScore(ctx context.Context, ns, coll, member []byte, linearizable bool) (float64, bool, error) {
	v, err := c.read(ctx, ns, coll, zScoreQuery{NS: ns, Coll: coll, Member: member}, linearizable)
	if err != nil || v == nil {
		return 0, false, err
	}
	return *v.(*float64), true, nil
}

// ZCard returns the sorted-set cardinality.
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

// CardCheck returns the stored cardinality counter and the actual element count from one consistent
// snapshot; they must always be equal (an internal invariant probe for tests/ops).
func (c *Collections) CardCheck(ctx context.Context, ns, coll []byte, linearizable bool) (CardCheck, error) {
	v, err := c.read(ctx, ns, coll, cardCheckQuery{NS: ns, Coll: coll}, linearizable)
	if err != nil {
		return CardCheck{}, err
	}
	cc, _ := v.(CardCheck)
	return cc, nil
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
