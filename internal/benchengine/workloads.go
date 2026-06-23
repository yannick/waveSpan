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
