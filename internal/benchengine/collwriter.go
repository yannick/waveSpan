package benchengine

import (
	"context"

	"github.com/yannick/wavespan/internal/bench"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// collWriter is the collection op surface the set/hash/zset/bulkremove workloads drive. Both the
// default single-address client (via plainCollWriter) and the opt-in bench.ShardAwareClient satisfy
// it, so the workload closures are identical regardless of which is wired in.
type collWriter interface {
	SAdd(ctx context.Context, ns string, coll []byte, members ...[]byte) error
	SIsMember(ctx context.Context, ns string, coll, member []byte) error
	SCard(ctx context.Context, ns string, coll []byte) error
	HSet(ctx context.Context, ns string, coll, field, value []byte) error
	HGet(ctx context.Context, ns string, coll, field []byte) error
	HIncrBy(ctx context.Context, ns string, coll, field []byte, delta int64) error
	ZAdd(ctx context.Context, ns string, coll, member []byte, score float64) error
	ZScore(ctx context.Context, ns string, coll, member []byte) error
	BulkRemove(ctx context.Context, ns string, colls, members [][]byte) (count int, removed uint64, err error)
}

// plainCollWriter adapts the default single-address gRPC client to collWriter, delegating to the
// existing bench.Op* helpers. This is the unchanged default path (no shard awareness, server-side
// forward hop intact).
type plainCollWriter struct {
	c wavespanv1.CollectionServiceClient
}

func (p plainCollWriter) SAdd(ctx context.Context, ns string, coll []byte, members ...[]byte) error {
	return bench.OpSAdd(ctx, p.c, ns, coll, members...)
}
func (p plainCollWriter) SIsMember(ctx context.Context, ns string, coll, member []byte) error {
	return bench.OpSIsMember(ctx, p.c, ns, coll, member)
}
func (p plainCollWriter) SCard(ctx context.Context, ns string, coll []byte) error {
	return bench.OpSCard(ctx, p.c, ns, coll)
}
func (p plainCollWriter) HSet(ctx context.Context, ns string, coll, field, value []byte) error {
	return bench.OpHSet(ctx, p.c, ns, coll, field, value)
}
func (p plainCollWriter) HGet(ctx context.Context, ns string, coll, field []byte) error {
	return bench.OpHGet(ctx, p.c, ns, coll, field)
}
func (p plainCollWriter) HIncrBy(ctx context.Context, ns string, coll, field []byte, delta int64) error {
	return bench.OpHIncrBy(ctx, p.c, ns, coll, field, delta)
}
func (p plainCollWriter) ZAdd(ctx context.Context, ns string, coll, member []byte, score float64) error {
	return bench.OpZAdd(ctx, p.c, ns, coll, member, score)
}
func (p plainCollWriter) ZScore(ctx context.Context, ns string, coll, member []byte) error {
	return bench.OpZScore(ctx, p.c, ns, coll, member)
}
func (p plainCollWriter) BulkRemove(ctx context.Context, ns string, colls, members [][]byte) (int, uint64, error) {
	return bench.OpBulkRemove(ctx, p.c, ns, colls, members)
}

// collWriterFor returns the collection write surface for cfg: the opt-in shard-aware client when
// cfg.ShardAware is set (routing writes straight to each shard's leader), otherwise the default
// single-address client. An error building the shard-aware client (e.g. no cores) is surfaced so the
// run fails fast rather than silently falling back.
func collWriterFor(cfg Config) (collWriter, error) {
	if cfg.ShardAware {
		sac, err := bench.NewShardAwareClient(cfg.Cores, cfg.DataShards)
		if err != nil {
			return nil, err
		}
		return sac, nil
	}
	return plainCollWriter{c: bench.CollectionsClient(cfg.DataAddr)}, nil
}
