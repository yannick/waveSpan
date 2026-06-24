package benchengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync/atomic"

	"github.com/yannick/wavespan/internal/bench"
)

// intParam reads an int parameter, tolerating JSON float64, int, and string-free maps.
func intParam(m map[string]any, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return def
	}
}

func floatParam(m map[string]any, key string, def float64) float64 {
	v, ok := m[key]
	if !ok {
		return def
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return def
	}
}

func strParam(m map[string]any, key, def string) string {
	v, ok := m[key]
	if !ok {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return def
}

// opsFor builds the op closure and label for a workload spec. Uses math/rand/v2 (goroutine-safe).
func opsFor(spec WorkloadSpec, cfg Config) (op func(context.Context) error, label string, err error) {
	switch spec.Kind {
	case "kv":
		keys := intParam(spec.Params, "keys", 1000)
		if keys < 1 {
			keys = 1
		}
		readRatio := floatParam(spec.Params, "readRatio", 0.5)
		ns := strParam(spec.Params, "namespace", "default")
		valueSize := intParam(spec.Params, "valueSize", 256)
		if valueSize < 1 {
			valueSize = 1
		}
		val := make([]byte, valueSize)
		for i := range val {
			val[i] = 'v'
		}
		c := bench.KVClient(cfg.DataAddr)
		op = func(ctx context.Context) error {
			key := fmt.Sprintf("bench/%d", rand.IntN(keys))
			if rand.Float64() < readRatio {
				return bench.OpKVRead(ctx, c, ns, key)
			}
			return bench.OpKVWrite(ctx, c, ns, key, val)
		}
		return op, "kv", nil

	case "multiget":
		keys := intParam(spec.Params, "keys", 1000)
		if keys < 1 {
			keys = 1
		}
		batch := intParam(spec.Params, "batch", 16)
		if batch < 1 {
			batch = 1
		}
		ns := strParam(spec.Params, "namespace", "default")
		c := bench.KVClient(cfg.DataAddr)
		op = func(ctx context.Context) error {
			ks := make([][]byte, batch)
			for i := range ks {
				ks[i] = []byte(fmt.Sprintf("bench/%d", rand.IntN(keys)))
			}
			return bench.OpMultiGet(ctx, c, ns, ks)
		}
		return op, "multiget", nil

	case "set":
		collections := intParam(spec.Params, "collections", 1000)
		if collections < 1 {
			collections = 1
		}
		members := intParam(spec.Params, "members", 100)
		if members < 1 {
			members = 1
		}
		writeRatio := floatParam(spec.Params, "writeRatio", 0.5)
		ns := strParam(spec.Params, "namespace", "bench-collections")
		c, err := collWriterFor(cfg)
		if err != nil {
			return nil, "", err
		}
		op = func(ctx context.Context) error {
			ctx, cancel := withDeadline(ctx, collOpTimeout) // collection writes need a deadline (Raft propose)
			defer cancel()
			coll := []byte(fmt.Sprintf("col/%d", rand.IntN(collections)))
			member := []byte(fmt.Sprintf("m/%d", rand.IntN(members)))
			if rand.Float64() < writeRatio {
				return c.SAdd(ctx, ns, coll, member)
			}
			if rand.Float64() < 0.5 {
				return c.SIsMember(ctx, ns, coll, member)
			}
			return c.SCard(ctx, ns, coll)
		}
		return op, "set", nil

	case "hash":
		collections := intParam(spec.Params, "collections", 1000)
		if collections < 1 {
			collections = 1
		}
		fields := intParam(spec.Params, "fields", 100)
		if fields < 1 {
			fields = 1
		}
		writeRatio := floatParam(spec.Params, "writeRatio", 0.5)
		counterRatio := floatParam(spec.Params, "counterRatio", 0.2)
		ns := strParam(spec.Params, "namespace", "bench-collections")
		counter := []byte("counter")
		val := []byte("v")
		c, err := collWriterFor(cfg)
		if err != nil {
			return nil, "", err
		}
		op = func(ctx context.Context) error {
			ctx, cancel := withDeadline(ctx, collOpTimeout) // collection writes need a deadline (Raft propose)
			defer cancel()
			coll := []byte(fmt.Sprintf("col/%d", rand.IntN(collections)))
			field := []byte(fmt.Sprintf("m/%d", rand.IntN(fields)))
			if rand.Float64() < writeRatio {
				if rand.Float64() < counterRatio {
					return c.HIncrBy(ctx, ns, coll, counter, 1)
				}
				return c.HSet(ctx, ns, coll, field, val)
			}
			return c.HGet(ctx, ns, coll, field)
		}
		return op, "hash", nil

	case "zset":
		collections := intParam(spec.Params, "collections", 1000)
		if collections < 1 {
			collections = 1
		}
		members := intParam(spec.Params, "members", 100)
		if members < 1 {
			members = 1
		}
		writeRatio := floatParam(spec.Params, "writeRatio", 0.5)
		ns := strParam(spec.Params, "namespace", "bench-collections")
		c, err := collWriterFor(cfg)
		if err != nil {
			return nil, "", err
		}
		op = func(ctx context.Context) error {
			ctx, cancel := withDeadline(ctx, collOpTimeout) // collection writes need a deadline (Raft propose)
			defer cancel()
			coll := []byte(fmt.Sprintf("col/%d", rand.IntN(collections)))
			member := []byte(fmt.Sprintf("m/%d", rand.IntN(members)))
			if rand.Float64() < writeRatio {
				return c.ZAdd(ctx, ns, coll, member, rand.Float64())
			}
			return c.ZScore(ctx, ns, coll, member)
		}
		return op, "zset", nil

	case "bulkremove":
		collections := intParam(spec.Params, "collections", 1000)
		if collections < 1 {
			collections = 1
		}
		batch := intParam(spec.Params, "batch", 50)
		if batch < 1 {
			batch = 1
		}
		ns := strParam(spec.Params, "namespace", "bench-collections")
		member := []byte(strParam(spec.Params, "member", "doomed"))
		members := [][]byte{member}
		c, err := collWriterFor(cfg)
		if err != nil {
			return nil, "", err
		}
		op = func(ctx context.Context) error {
			ctx, cancel := withDeadline(ctx, collOpTimeout) // batch remove + re-add: all need a deadline (Raft propose)
			defer cancel()
			keys := make([][]byte, batch)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("col/%d", rand.IntN(collections)))
			}
			_, _, err := c.BulkRemove(ctx, ns, keys, members)
			if err != nil {
				return err
			}
			// Re-add the member so the next removal has work to do.
			for _, k := range keys {
				if e := c.SAdd(ctx, ns, k, member); e != nil {
					return e
				}
			}
			return nil
		}
		return op, "bulkremove", nil

	case "cypher":
		if len(cfg.CypherQueries) == 0 {
			return nil, "", fmt.Errorf("benchengine: cypher workload needs CypherQueries")
		}
		queries := cfg.CypherQueries
		c := bench.CypherClient(cfg.DataAddr)
		var ctr atomic.Uint64
		op = func(ctx context.Context) error {
			i := ctr.Add(1) - 1
			q := queries[int(i%uint64(len(queries)))]
			return bench.OpCypher(ctx, c, cfg.Graph, q.Body)
		}
		return op, "cypher", nil

	default:
		return nil, "", fmt.Errorf("benchengine: unknown workload kind %q", spec.Kind)
	}
}
