package collections

import (
	"bytes"
	"encoding/binary"

	"github.com/yannick/wavespan/internal/storage"
)

// Range split with dragonboat is migrate-based, not in-place: dragonboat shards are independent Raft
// groups with no split primitive, so splitting a range means starting a new shard, copying the
// subrange's keys into it, cutting the directory over, and purging them from the old shard (design/30
// §6, ADR 0008). These primitives do the copy/purge; the orchestration is in control.go.
//
// v1 assumes the splitting subrange is quiescent (no concurrent writes during the migration window);
// a freeze/cutover for concurrent writes is a follow-up (design/30 §6.2).

// rawKV is a shard-prefix-relative key (Suffix = key after the 8-byte shard prefix) and its value.
type rawKV struct {
	Suffix []byte
	Value  []byte
}

// migrateScanQuery returns the rawKV pairs of every key whose routing key falls in [StartRoute,
// EndRoute) — used to read a subrange out of the source shard. Empty EndRoute = +inf.
type migrateScanQuery struct {
	StartRoute []byte
	EndRoute   []byte
	Limit      int
}

// scanner is satisfied by both storage.Snapshot and storage.LocalStore.
type scanner interface {
	Scan(cf storage.ColumnFamily, start, end []byte, limit int) (storage.Iterator, error)
}

// routeKeyOf extracts the collection routing key (chunk(ns)||chunk(coll)) from a shard-prefix-relative
// key suffix, for subData and subTTL keys. Shard-level keys (subMeta) return ok=false (not migrated).
func routeKeyOf(suffix []byte) ([]byte, bool) {
	if len(suffix) == 0 {
		return nil, false
	}
	var body []byte
	switch suffix[0] {
	case subData:
		body = suffix[1:]
	case subTTL:
		if len(suffix) < 9 {
			return nil, false
		}
		body = suffix[9:] // skip be(expiry)
	default:
		return nil, false
	}
	ns, rest, err := takeChunk(body)
	if err != nil {
		return nil, false
	}
	coll, _, err := takeChunk(rest)
	if err != nil {
		return nil, false
	}
	return appendChunk(appendChunk(nil, ns), coll), true
}

func inRoute(rk, start, end []byte) bool {
	return (len(start) == 0 || bytes.Compare(rk, start) >= 0) &&
		(len(end) == 0 || bytes.Compare(rk, end) < 0)
}

// scanRange returns the rawKV pairs under prefix whose routing key is in [start,end).
func scanRange(sc scanner, prefix, start, end []byte, limit int) ([]rawKV, error) {
	it, err := sc.Scan(storage.CFReplData, prefix, prefixEnd(prefix), 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []rawKV
	for it.Valid() {
		suffix := it.Key()[len(prefix):]
		if rk, ok := routeKeyOf(suffix); ok && inRoute(rk, start, end) {
			out = append(out, rawKV{
				Suffix: append([]byte(nil), suffix...),
				Value:  append([]byte(nil), it.Value()...),
			})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		it.Next()
	}
	return out, it.Err()
}

// --- migrate command codecs (op byte then payload) ---

func encodeIngest(kvs []rawKV) []byte {
	buf := []byte{byte(opIngest)}
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(kvs)))
	buf = append(buf, cnt[:]...)
	for _, kv := range kvs {
		buf = appendChunk(buf, kv.Suffix)
		buf = appendChunk(buf, kv.Value)
	}
	return buf
}

func decodeIngest(b []byte) ([]rawKV, error) {
	if len(b) < 5 {
		return nil, errShortCommand
	}
	n := binary.BigEndian.Uint32(b[1:5])
	rest := b[5:]
	out := make([]rawKV, 0, n)
	for i := uint32(0); i < n; i++ {
		var suffix, val []byte
		var err error
		if suffix, rest, err = takeChunk(rest); err != nil {
			return nil, err
		}
		if val, rest, err = takeChunk(rest); err != nil {
			return nil, err
		}
		out = append(out, rawKV{Suffix: suffix, Value: val})
	}
	return out, nil
}

func encodePurge(start, end []byte) []byte {
	buf := []byte{byte(opPurge)}
	buf = appendChunk(buf, start)
	return appendChunk(buf, end)
}

func decodePurge(b []byte) (start, end []byte, err error) {
	if len(b) < 1 {
		return nil, nil, errShortCommand
	}
	rest := b[1:]
	if start, rest, err = takeChunk(rest); err != nil {
		return nil, nil, err
	}
	if end, _, err = takeChunk(rest); err != nil {
		return nil, nil, err
	}
	return start, end, nil
}

// applyMigrate handles opIngest/opPurge in a data shard's Update (design/30 §6).
func (u *updateCtx) applyMigrate(cmd []byte) (int64, error) {
	switch opKind(cmd[0]) {
	case opIngest:
		kvs, err := decodeIngest(cmd)
		if err != nil {
			return 0, err
		}
		for _, kv := range kvs {
			key := append(append([]byte{}, u.s.prefix...), kv.Suffix...)
			u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: key, Value: kv.Value})
		}
		return int64(len(kvs)), nil
	case opPurge:
		start, end, err := decodePurge(cmd)
		if err != nil {
			return 0, err
		}
		kvs, err := scanRange(u.s.store, u.s.prefix, start, end, 0)
		if err != nil {
			return 0, err
		}
		for _, kv := range kvs {
			key := append(append([]byte{}, u.s.prefix...), kv.Suffix...)
			u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: key, Delete: true})
		}
		return int64(len(kvs)), nil
	}
	return 0, errUnknownOp
}
