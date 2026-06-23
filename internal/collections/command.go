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
)

var (
	errShortCommand = errors.New("collections: short command")
	errUnknownOp    = errors.New("collections: unknown op")
	// ErrWrongType is returned when an op targets a collection of a different datatype.
	ErrWrongType = errors.New("collections: WRONGTYPE")

	wrongType = []byte("WRONGTYPE") // Result.Data sentinel set by the state machine
)

// opKind is the log-command opcode.
type opKind byte

const (
	opSAdd opKind = 1 // set add
	opSRem opKind = 2 // set remove
	opHSet opKind = 3 // hash set field(s)
	opHDel opKind = 4 // hash delete field(s)
	opZAdd opKind = 5 // sorted-set add (member+score)
	opZRem opKind = 6 // sorted-set remove
)

// collType is the fixed datatype of a collection, recorded in its header.
type collType byte

const (
	typeSet  collType = 1
	typeHash collType = 2
	typeZSet collType = 3
)

// typeForOp maps an op to the datatype it requires.
func typeForOp(op opKind) collType {
	switch op {
	case opSAdd, opSRem:
		return typeSet
	case opHSet, opHDel:
		return typeHash
	case opZAdd, opZRem:
		return typeZSet
	}
	return 0
}

// item is one element of a command: a set member, a hash field(+value), or a zset member(+score).
type item struct {
	Key   []byte
	Val   []byte
	Score float64
}

// command is one proposed mutation carried in a Raft log entry's Cmd.
type command struct {
	Op    opKind
	NS    []byte
	Coll  []byte
	Items []item
}

func encodeCommand(c command) []byte {
	buf := make([]byte, 0, 1+8+len(c.NS)+len(c.Coll))
	buf = append(buf, byte(c.Op))
	buf = appendChunk(buf, c.NS)
	buf = appendChunk(buf, c.Coll)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(c.Items)))
	buf = append(buf, cnt[:]...)
	for _, it := range c.Items {
		buf = appendChunk(buf, it.Key)
		buf = appendChunk(buf, it.Val)
		var sc [8]byte
		binary.BigEndian.PutUint64(sc[:], math.Float64bits(it.Score))
		buf = append(buf, sc[:]...)
	}
	return buf
}

func decodeCommand(b []byte) (command, error) {
	if len(b) < 1 {
		return command{}, errShortCommand
	}
	c := command{Op: opKind(b[0])}
	if typeForOp(c.Op) == 0 {
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
	if len(rest) < 4 {
		return command{}, errShortCommand
	}
	n := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]
	c.Items = make([]item, 0, n)
	for i := uint32(0); i < n; i++ {
		var it item
		if it.Key, rest, err = takeChunk(rest); err != nil {
			return command{}, err
		}
		if it.Val, rest, err = takeChunk(rest); err != nil {
			return command{}, err
		}
		if len(rest) < 8 {
			return command{}, errShortCommand
		}
		it.Score = math.Float64frombits(binary.BigEndian.Uint64(rest[:8]))
		rest = rest[8:]
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
