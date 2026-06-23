package collections

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	sm "github.com/lni/dragonboat/v4/statemachine"

	"github.com/yannick/wavespan/internal/storage"
)

// The meta shard (design/30 §7) is a Raft group holding the authoritative range directory: an ordered
// set of key-ranges [start,end) -> data shardID. Every node caches it and routes a collection to the
// shard whose range covers its routing key. Ranges are stored keyed by start; empty start = -inf,
// empty end = +inf.

type metaOp byte

const (
	opMetaPut    metaOp = 1 // upsert a range [start,end) -> shardID (init / split)
	opMetaDelete metaOp = 2 // remove the range keyed by start (merge)
)

type metaCommand struct {
	Op      metaOp
	Start   []byte
	End     []byte
	ShardID uint64
}

func encodeMetaCommand(c metaCommand) []byte {
	buf := []byte{byte(c.Op)}
	buf = appendChunk(buf, c.Start)
	buf = appendChunk(buf, c.End)
	return append(buf, u64(c.ShardID)...)
}

func decodeMetaCommand(b []byte) (metaCommand, error) {
	if len(b) < 1 {
		return metaCommand{}, errShortCommand
	}
	c := metaCommand{Op: metaOp(b[0])}
	rest := b[1:]
	var err error
	if c.Start, rest, err = takeChunk(rest); err != nil {
		return metaCommand{}, err
	}
	if c.End, rest, err = takeChunk(rest); err != nil {
		return metaCommand{}, err
	}
	if len(rest) < 8 {
		return metaCommand{}, errShortCommand
	}
	c.ShardID = binary.BigEndian.Uint64(rest[:8])
	return c, nil
}

// rangeEntry is one directory range.
type rangeEntry struct {
	Start   []byte
	End     []byte
	ShardID uint64
}

// metaListQuery returns the full range directory.
type metaListQuery struct{}

// metaSM is the meta shard's on-disk state machine. Range entries live under subData keyed by start:
//
//	<prefix>|subData|<start> -> chunk(end) || be(shardID)
type metaSM struct {
	baseSM
}

func newMetaSM(store storage.LocalStore, shardID uint64) *metaSM {
	return &metaSM{baseSM: newBaseSM(store, shardID)}
}

func (m *metaSM) rangeSpace() []byte           { return append(append([]byte{}, m.prefix...), subData) }
func (m *metaSM) rangeKey(start []byte) []byte { return append(m.rangeSpace(), start...) }

func (m *metaSM) Update(entries []sm.Entry) ([]sm.Entry, error) {
	if len(entries) == 0 {
		return entries, nil
	}
	var ops []storage.StoreOp
	for i := range entries {
		c, err := decodeMetaCommand(entries[i].Cmd)
		if err != nil {
			return nil, err
		}
		switch c.Op {
		case opMetaPut:
			val := append(appendChunk(nil, c.End), u64(c.ShardID)...)
			ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: m.rangeKey(c.Start), Value: val})
			entries[i].Result = sm.Result{Value: 1}
		case opMetaDelete:
			ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: m.rangeKey(c.Start), Delete: true})
			entries[i].Result = sm.Result{Value: 1}
		default:
			return nil, errors.New("collections: unknown meta op")
		}
	}
	ops = append(ops, storage.StoreOp{CF: storage.CFReplData, Key: m.appliedKey(), Value: u64(entries[len(entries)-1].Index)})
	if err := m.store.Batch(ops); err != nil {
		return nil, err
	}
	return entries, nil
}

func (m *metaSM) Lookup(query interface{}) (interface{}, error) {
	switch query.(type) {
	case metaListQuery:
		snap, err := m.store.Snapshot()
		if err != nil {
			return nil, err
		}
		defer func() { _ = snap.Close() }()
		rs := m.rangeSpace()
		it, err := snap.Scan(storage.CFReplData, rs, prefixEnd(rs), 0)
		if err != nil {
			return nil, err
		}
		defer func() { _ = it.Close() }()
		var out []rangeEntry
		for it.Valid() {
			start := append([]byte(nil), it.Key()[len(rs):]...)
			end, rest, derr := takeChunk(it.Value())
			if derr == nil && len(rest) >= 8 {
				out = append(out, rangeEntry{Start: start, End: append([]byte(nil), end...), ShardID: binary.BigEndian.Uint64(rest[:8])})
			}
			it.Next()
		}
		return out, it.Err()
	default:
		return nil, errors.New("collections: unknown meta query")
	}
}

// routeKey is the position of a collection in the global ordered keyspace used for range routing
// (design/30 §2): length-prefixed namespace then collection.
func routeKey(ns, coll []byte) []byte {
	return appendChunk(appendChunk(nil, ns), coll)
}

// RouteKey is the exported form of routeKey, for computing split boundaries.
func RouteKey(ns, coll []byte) []byte { return routeKey(ns, coll) }

// RangeDirectory caches the meta shard's range directory and routes collections to data shards
// (design/30 §7.3). It refreshes from the meta shard; routing is an in-memory interval lookup.
type RangeDirectory struct {
	shard     RaftShard
	metaShard uint64

	mu     sync.RWMutex
	ranges []rangeEntry
}

var _ Directory = (*RangeDirectory)(nil)

// NewRangeDirectory builds a directory backed by the meta shard reachable through shard.
func NewRangeDirectory(shard RaftShard, metaShard uint64) *RangeDirectory {
	return &RangeDirectory{shard: shard, metaShard: metaShard}
}

// Refresh reloads the range directory from the meta shard (linearizable).
func (d *RangeDirectory) Refresh(ctx context.Context) error {
	v, err := d.shard.Read(ctx, d.metaShard, metaListQuery{}, true)
	if err != nil {
		return err
	}
	ranges, _ := v.([]rangeEntry)
	d.mu.Lock()
	d.ranges = ranges
	d.mu.Unlock()
	return nil
}

// ShardFor returns the data shard owning a collection, or 0 if the directory has no covering range.
func (d *RangeDirectory) ShardFor(ns, coll []byte) uint64 {
	key := routeKey(ns, coll)
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, r := range d.ranges {
		if inRoute(key, r.Start, r.End) {
			return r.ShardID
		}
	}
	return 0
}

// rangeContaining returns the range whose [start,end) covers a routing key.
func (d *RangeDirectory) rangeContaining(key []byte) (rangeEntry, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, r := range d.ranges {
		if inRoute(key, r.Start, r.End) {
			return r, true
		}
	}
	return rangeEntry{}, false
}

// all returns a copy of the current range set.
func (d *RangeDirectory) all() []rangeEntry {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]rangeEntry(nil), d.ranges...)
}

// maxShardID is the largest data shard id in the directory (for allocating the next one).
func (d *RangeDirectory) maxShardID() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var mx uint64
	for _, r := range d.ranges {
		if r.ShardID > mx {
			mx = r.ShardID
		}
	}
	return mx
}
