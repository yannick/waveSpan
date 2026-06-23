package collections

import "context"

// CypherCollections adapts the typed Collections engine to the Cypher set.*/hash.*/zset.* surface
// (planner.CollectionsAccess), routing through the same engine the CollectionService RPC API uses.
// Reads are bounded-stale (linearizable=false) — cheap and appropriate for query-time lookups.
type CypherCollections struct{ cols *Collections }

// NewCypherCollections wraps a Collections engine for use by the Cypher built-ins.
func NewCypherCollections(cols *Collections) *CypherCollections {
	return &CypherCollections{cols: cols}
}

// SAdd adds a set member.
func (a *CypherCollections) SAdd(ctx context.Context, ns string, coll, member []byte) (uint64, error) {
	return a.cols.SAdd(ctx, []byte(ns), coll, member)
}

// SRem removes a set member.
func (a *CypherCollections) SRem(ctx context.Context, ns string, coll, member []byte) (uint64, error) {
	return a.cols.SRem(ctx, []byte(ns), coll, member)
}

// SIsMember reports set membership (bounded-stale).
func (a *CypherCollections) SIsMember(ctx context.Context, ns string, coll, member []byte) (bool, error) {
	return a.cols.SIsMember(ctx, []byte(ns), coll, member, false)
}

// SCard returns the set cardinality (bounded-stale).
func (a *CypherCollections) SCard(ctx context.Context, ns string, coll []byte) (uint64, error) {
	return a.cols.SCard(ctx, []byte(ns), coll, false)
}

// SMembers enumerates set members (bounded-stale).
func (a *CypherCollections) SMembers(ctx context.Context, ns string, coll []byte, limit int) ([][]byte, error) {
	return a.cols.SMembers(ctx, []byte(ns), coll, limit, false)
}

// HSet sets a hash field.
func (a *CypherCollections) HSet(ctx context.Context, ns string, coll, field, value []byte) (uint64, error) {
	return a.cols.HSet(ctx, []byte(ns), coll, FieldValue{Field: field, Value: value})
}

// HGet reads a hash field (bounded-stale).
func (a *CypherCollections) HGet(ctx context.Context, ns string, coll, field []byte) ([]byte, bool, error) {
	return a.cols.HGet(ctx, []byte(ns), coll, field, false)
}

// HGetAll returns hash fields and values as parallel slices (bounded-stale).
func (a *CypherCollections) HGetAll(ctx context.Context, ns string, coll []byte, limit int) ([][]byte, [][]byte, error) {
	rows, err := a.cols.HGetAll(ctx, []byte(ns), coll, limit, false)
	if err != nil {
		return nil, nil, err
	}
	fields := make([][]byte, len(rows))
	values := make([][]byte, len(rows))
	for i, r := range rows {
		fields[i], values[i] = r.Field, r.Value
	}
	return fields, values, nil
}

// ZAdd adds a scored sorted-set member.
func (a *CypherCollections) ZAdd(ctx context.Context, ns string, coll, member []byte, score float64) (uint64, error) {
	return a.cols.ZAdd(ctx, []byte(ns), coll, ScoredMember{Member: member, Score: score})
}

// ZScore reads a sorted-set member's score (bounded-stale).
func (a *CypherCollections) ZScore(ctx context.Context, ns string, coll, member []byte) (float64, bool, error) {
	return a.cols.ZScore(ctx, []byte(ns), coll, member, false)
}

// ZRange returns sorted-set members and scores in ascending score order (bounded-stale).
func (a *CypherCollections) ZRange(ctx context.Context, ns string, coll []byte, limit int) ([][]byte, []float64, error) {
	rows, err := a.cols.ZRange(ctx, []byte(ns), coll, limit, false)
	if err != nil {
		return nil, nil, err
	}
	members := make([][]byte, len(rows))
	scores := make([]float64, len(rows))
	for i, r := range rows {
		members[i], scores[i] = r.Member, r.Score
	}
	return members, scores, nil
}
