// Package kv: CypherKV adapts the KV Reader+Coordinator to the cypher planner.KVAccess interface,
// so Cypher kv.* built-ins read and write the exact same namespaced, replicated KV as the gRPC API.
package kv

import "context"

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
// hideExpired=true so expired/tombstoned records read as absent (→ Cypher null).
func (k *CypherKV) Get(ctx context.Context, namespace string, key []byte) ([]byte, bool, error) {
	res, err := k.reader.Get(ctx, namespace, key, true)
	if err != nil {
		return nil, false, err
	}
	if !res.GetFound() {
		return nil, false, nil
	}
	return res.GetValue(), true, nil
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
