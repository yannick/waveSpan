package collections

import "context"

// Collections is the Set API over the consensus tier (design/30 §13.4): it routes each op to its
// owning shard via the Directory and drives the RaftShard engine. M-A covers sets; hash tables and
// sorted sets arrive in M-D.
type Collections struct {
	shard RaftShard
	dir   Directory
}

// New builds the Set API over a RaftShard engine and a shard Directory.
func New(shard RaftShard, dir Directory) *Collections {
	return &Collections{shard: shard, dir: dir}
}

// SAdd adds members to the set, returning the number newly added.
func (c *Collections) SAdd(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.shard.Propose(ctx, c.dir.ShardFor(ns, coll),
		encodeCommand(command{Op: opSAdd, NS: ns, Coll: coll, Members: members}))
}

// SRem removes members, returning the number removed.
func (c *Collections) SRem(ctx context.Context, ns, coll []byte, members ...[]byte) (uint64, error) {
	return c.shard.Propose(ctx, c.dir.ShardFor(ns, coll),
		encodeCommand(command{Op: opSRem, NS: ns, Coll: coll, Members: members}))
}

// SIsMember reports membership. linearizable=true forces a leader read-index; false is bounded-stale.
func (c *Collections) SIsMember(ctx context.Context, ns, coll, member []byte, linearizable bool) (bool, error) {
	v, err := c.shard.Read(ctx, c.dir.ShardFor(ns, coll), isMemberQuery{NS: ns, Coll: coll, Member: member}, linearizable)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

// SCard returns the exact set cardinality (a single-writer counter — lossless, unlike LWW).
func (c *Collections) SCard(ctx context.Context, ns, coll []byte, linearizable bool) (uint64, error) {
	v, err := c.shard.Read(ctx, c.dir.ShardFor(ns, coll), cardQuery{NS: ns, Coll: coll}, linearizable)
	if err != nil {
		return 0, err
	}
	n, _ := v.(uint64)
	return n, nil
}

// SMembers returns up to limit members in byte order (0 = all). Streaming / cursor enumeration
// (design/30 §13.8) is a later milestone.
func (c *Collections) SMembers(ctx context.Context, ns, coll []byte, limit int, linearizable bool) ([][]byte, error) {
	v, err := c.shard.Read(ctx, c.dir.ShardFor(ns, coll), membersQuery{NS: ns, Coll: coll, Limit: limit}, linearizable)
	if err != nil {
		return nil, err
	}
	out, _ := v.([][]byte)
	return out, nil
}
