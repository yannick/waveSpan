// Package collections implements the replicated-collections consensus tier (design/30): sets, hash
// tables, and sorted sets over range-based multi-Raft (dragonboat, ADR 0008), coexisting with the AP
// cache KV without touching its hot path.
//
// Milestones (design/30 §18): M-0 stood up one dragonboat shard over wavesdb; M-A added the raftshard
// interface, the Set datatype, and the Set API; M-D (in progress) adds the Hash and Sorted-set
// datatypes with a per-collection type header. The meta Raft group + multi-range directory, the
// cheap-mTLS transport, the SWIM node registry, split/merge, and the placement driver are later
// milestones; for now dragonboat's built-in transport, default Pebble LogDB, and a single-range
// directory are used.
package collections

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
)

var (
	errShortCommand = errors.New("collections: short command")
	errUnknownOp    = errors.New("collections: unknown op")
	// ErrWrongType is returned when an op targets a collection of a different datatype.
	ErrWrongType = errors.New("collections: WRONGTYPE")
	// ErrFrozen is returned when an op targets a subrange currently frozen for a split migration; it is
	// transient — the client retries and the write lands on the new shard after the directory cuts over.
	ErrFrozen = errors.New("collections: subrange frozen (splitting)")
	// ErrNotNumber is returned when HIncrBy/HIncrByFloat targets a field whose value is not a number.
	ErrNotNumber = errors.New("collections: hash field value is not a number")

	wrongType  = []byte("WRONGTYPE") // Result.Data sentinel set by the state machine
	frozenMark = []byte("FROZEN")    // Result.Data sentinel: mutation rejected, subrange is migrating
	notNumber  = []byte("NOTNUM")    // Result.Data sentinel: HIncr* on a non-numeric field
)

// opKind is the log-command opcode.
type opKind byte

const (
	opSAdd         opKind = 1  // set add
	opSRem         opKind = 2  // set remove
	opHSet         opKind = 3  // hash set field(s)
	opHDel         opKind = 4  // hash delete field(s)
	opZAdd         opKind = 5  // sorted-set add (member+score)
	opZRem         opKind = 6  // sorted-set remove
	opExpire       opKind = 7  // leader-proposed TTL deletion (design/30 §10)
	opIngest       opKind = 8  // migrate: write raw kv pairs into this shard (design/30 §6)
	opPurge        opKind = 9  // migrate: delete a routeKey subrange from this shard
	opFreeze       opKind = 10 // split: reject mutations to a routeKey subrange (design/30 §6.1)
	opUnfreeze     opKind = 11 // split: lift a freeze
	opHIncrBy      opKind = 12 // hash: atomic integer increment of a field (HINCRBY)
	opHIncrByFloat opKind = 13 // hash: atomic float increment of a field (HINCRBYFLOAT)
	opRemove       opKind = 14 // type-agnostic element removal (bulk cross-collection delete, §13.7)
	opBatch        opKind = 15 // QW2: a coalesced batch of single commands carried in one Raft entry
)

// mutates reports whether an op changes element state (and so must respect a subrange freeze).
func mutates(op opKind) bool {
	return typeForOp(op) != 0 || op == opExpire || op == opRemove
}

// collType is the fixed datatype of a collection, recorded in its header.
type collType byte

const (
	typeSet  collType = 1
	typeHash collType = 2
	typeZSet collType = 3
)

// typeForOp maps a mutation op to the datatype it requires (0 = type-agnostic, e.g. opExpire).
func typeForOp(op opKind) collType {
	switch op {
	case opSAdd, opSRem:
		return typeSet
	case opHSet, opHDel, opHIncrBy, opHIncrByFloat:
		return typeHash
	case opZAdd, opZRem:
		return typeZSet
	}
	return 0
}

// item is one element of a command: a set member, a hash field(+value), or a zset member(+score).
// ExpiryMs, when > 0, is the absolute expiry time (unix ms) the leader stamped before proposing —
// deterministic across replicas because it is fixed in the committed entry (design/30 §10 / N4).
type item struct {
	Key      []byte
	Val      []byte
	Score    float64
	ExpiryMs int64
}

// command is one proposed mutation carried in a Raft log entry's Cmd.
type command struct {
	Op    opKind
	NS    []byte
	Coll  []byte
	Idem  []byte // optional idempotency key (design/30 §13.12); empty = no dedup
	Items []item
}

// encodeBufPool reuses command-encode scratch on the hot propose path (A1, design/32 profiling: the
// consensus path is ~63% GC-bound). dragonboat's Propose copies the cmd bytes before returning, so the
// caller may return the buffer to the pool immediately after Propose — see proposer.go / collections.go.
var encodeBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 128); return &b }}

// encBuf is a pooled encode buffer handle; release returns it to the pool. The returned []byte is only
// valid until release is called (or the bytes are copied / handed to Propose, which copies them).
type encBuf struct{ p *[]byte }

func (e encBuf) bytes() []byte { return *e.p }
func (e encBuf) release() {
	*e.p = (*e.p)[:0]
	encodeBufPool.Put(e.p)
}

// encodeCommandPooled encodes c into a pooled buffer. The caller MUST call release() once the bytes are
// no longer needed (i.e. after the Propose that copies them returns), reusing the scratch for the next op.
func encodeCommandPooled(c command) encBuf {
	p := encodeBufPool.Get().(*[]byte)
	*p = appendCommand((*p)[:0], c)
	return encBuf{p: p}
}

// encodeCommand encodes c into a fresh slice (the allocating path, kept for callers that retain the
// bytes beyond a single Propose — control-plane ops, the TTL sweeper, tests).
func encodeCommand(c command) []byte {
	return appendCommand(make([]byte, 0, 1+12+len(c.NS)+len(c.Coll)), c)
}

// appendCommand appends the wire encoding of c to dst and returns the grown slice.
func appendCommand(dst []byte, c command) []byte {
	dst = append(dst, byte(c.Op))
	dst = appendChunk(dst, c.NS)
	dst = appendChunk(dst, c.Coll)
	dst = appendChunk(dst, c.Idem)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(c.Items)))
	dst = append(dst, cnt[:]...)
	for _, it := range c.Items {
		dst = appendChunk(dst, it.Key)
		dst = appendChunk(dst, it.Val)
		var sc [16]byte
		binary.BigEndian.PutUint64(sc[0:8], math.Float64bits(it.Score))
		binary.BigEndian.PutUint64(sc[8:16], uint64(it.ExpiryMs))
		dst = append(dst, sc[:]...)
	}
	return dst
}

// encodeBatchInto encodes a coalesced batch of pre-encoded single commands (QW2) into dst:
//
//	opBatch || uint32(count) || (chunk(cmd))*
//
// Each cmd is a complete single-command encoding (appendCommand output). The SM decodes the wrapper and
// applies each sub-command in one Update, so N coalesced ops collapse to one Raft entry.
func encodeBatchInto(dst []byte, cmds [][]byte) []byte {
	dst = append(dst, byte(opBatch))
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(cmds)))
	dst = append(dst, cnt[:]...)
	for _, cmd := range cmds {
		dst = appendChunk(dst, cmd)
	}
	return dst
}

// decodeBatch splits an opBatch wrapper into its sub-command byte slices (each a single-command
// encoding). The returned slices alias b and are valid for the lifetime of the entry being applied.
func decodeBatch(b []byte) ([][]byte, error) {
	if len(b) < 1 || opKind(b[0]) != opBatch {
		return nil, errShortCommand
	}
	rest := b[1:]
	if len(rest) < 4 {
		return nil, errShortCommand
	}
	n := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]
	out := make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		var cmd []byte
		var err error
		if cmd, rest, err = takeChunk(rest); err != nil {
			return nil, err
		}
		out = append(out, cmd)
	}
	return out, nil
}

func decodeCommand(b []byte) (command, error) {
	return decodeCommandInto(b, nil)
}

// decodeCommandInto decodes b, reusing scratch for the Items slice to avoid a per-entry allocation in
// the apply loop (A1). The decoded NS/Coll/Idem/Key/Val sub-slices alias b (no copy) — safe because the
// command is fully consumed within one Update over the entry it came from. Pass nil scratch for a
// fresh slice (the allocating path used by tests / callers that retain the command).
func decodeCommandInto(b []byte, scratch []item) (command, error) {
	if len(b) < 1 {
		return command{}, errShortCommand
	}
	c := command{Op: opKind(b[0])}
	if typeForOp(c.Op) == 0 && c.Op != opExpire && c.Op != opRemove {
		return command{}, errUnknownOp
	}
	rest := b[1:]
	var err error
	if c.NS, rest, err = takeChunk(rest); err != nil {
		return command{}, err
	}
	if c.Coll, rest, err = takeChunk(rest); err != nil {
		return command{}, err
	}
	if c.Idem, rest, err = takeChunk(rest); err != nil {
		return command{}, err
	}
	if len(rest) < 4 {
		return command{}, errShortCommand
	}
	n := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]
	if cap(scratch) >= int(n) {
		c.Items = scratch[:0]
	} else {
		c.Items = make([]item, 0, n)
	}
	for i := uint32(0); i < n; i++ {
		var it item
		if it.Key, rest, err = takeChunk(rest); err != nil {
			return command{}, err
		}
		if it.Val, rest, err = takeChunk(rest); err != nil {
			return command{}, err
		}
		if len(rest) < 16 {
			return command{}, errShortCommand
		}
		it.Score = math.Float64frombits(binary.BigEndian.Uint64(rest[:8]))
		it.ExpiryMs = int64(binary.BigEndian.Uint64(rest[8:16]))
		rest = rest[16:]
		c.Items = append(c.Items, it)
	}
	return c, nil
}

// appendChunk appends uint32(len(b)) || b.
func appendChunk(dst, b []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	dst = append(dst, l[:]...)
	return append(dst, b...)
}

func takeChunk(b []byte) (val, rest []byte, err error) {
	if len(b) < 4 {
		return nil, nil, errShortCommand
	}
	n := binary.BigEndian.Uint32(b[:4])
	b = b[4:]
	if uint32(len(b)) < n {
		return nil, nil, errShortCommand
	}
	return b[:n], b[n:], nil
}

func writeChunk(w io.Writer, b []byte) error {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	if _, err := w.Write(l[:]); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readChunk(r io.Reader) ([]byte, error) {
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err // io.EOF at a clean frame boundary
	}
	n := binary.BigEndian.Uint32(l[:])
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return b, nil
}

// prefixEnd returns the smallest key strictly greater than every key with the given prefix.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// sortableScore encodes a float64 score so big-endian byte order matches numeric order (for ZRANGE):
// flip the sign bit for positives, flip all bits for negatives.
func sortableScore(f float64) []byte {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		bits = ^bits // negative: flip all
	} else {
		bits |= 1 << 63 // positive: flip sign
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return b[:]
}

func unsortableScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 {
		bits &^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}
