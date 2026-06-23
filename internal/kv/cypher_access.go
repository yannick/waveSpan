package kv

import (
	"context"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// CypherKV satisfies planner.KVAccess (structural — this package does not import the planner).
type CypherKV struct {
	reader *Reader
	coord  *Coordinator
}

// NewCypherKV wires the adapter from the same Reader and Coordinator the gRPC KV Service uses.
func NewCypherKV(reader *Reader, coord *Coordinator) *CypherKV {
	return &CypherKV{reader: reader, coord: coord}
}

// Get routes through the Reader: local-first with a closest-holder cache fetch on a miss.
// hideExpired=true so expired/tombstoned records read as absent (→ Cypher null). partial is true
// when the read may be incomplete (e.g. a holder was unreachable), so a not-found could be a false
// negative — the caller surfaces this rather than presenting it as a definite absence.
func (k *CypherKV) Get(ctx context.Context, namespace string, key []byte) (value []byte, found bool, partial bool, err error) {
	res, err := k.reader.Get(ctx, namespace, key, true)
	if err != nil {
		return nil, false, false, err
	}
	partial = res.GetMeta().GetCompleteness() == wavespanv1.Completeness_PARTIAL
	if !res.GetFound() {
		return nil, false, partial, nil
	}
	return res.GetValue(), true, partial, nil
}

// Put routes through the Coordinator (origin+1 durable + replication fanout). The returned version
// is the committed HLC version's stable MutationID string.
func (k *CypherKV) Put(ctx context.Context, namespace string, key, value []byte, ttlMs *int64) (string, error) {
	out, err := k.coord.Put(ctx, namespace, key, value, ttlMs, "")
	if err != nil {
		return "", err
	}
	return out.Version.MutationID(), nil
}

// Delete tombstones the key through the Coordinator and returns the tombstone version's MutationID.
func (k *CypherKV) Delete(ctx context.Context, namespace string, key []byte) (string, error) {
	out, err := k.coord.Delete(ctx, namespace, key, "")
	if err != nil {
		return "", err
	}
	return out.Version.MutationID(), nil
}
