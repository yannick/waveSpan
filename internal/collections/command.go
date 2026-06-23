// Package collections implements the replicated-collections consensus tier (design/30): sets, hash
// tables, and sorted sets over range-based multi-Raft (dragonboat, ADR 0008), coexisting with the AP
// cache KV without touching its hot path.
//
// Milestones (design/30 §18): M-0 stood up one dragonboat shard end to end over wavesdb. M-A (here)
// adds the raftshard interface, the Set datatype state machine (members + an exact cardinality
// counter), and a CollectionService-style Set API routed through a directory. The meta Raft group +
// multi-range directory, the cheap-mTLS transport, the SWIM node registry, and the wavesdb-backed
// LogDB (Appendix B.4-B.6) are later milestones; M-A uses dragonboat's built-in transport, default
// Pebble LogDB, and a single-range directory.
package collections

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	errShortCommand = errors.New("collections: short command")
	errUnknownOp    = errors.New("collections: unknown op")
)

// opKind is the log-command opcode. M-A covers Set mutations; hash/sorted-set ops and conditional
// batches (design/30 §5.2, §13.9) arrive with later datatypes.
type opKind byte

const (
	opSAdd opKind = 1
	opSRem opKind = 2
)

// command is one proposed mutation carried in a Raft log entry's Cmd: a Set op over (namespace,
// collection) with one or more members.
type command struct {
	Op      opKind
	NS      []byte
	Coll    []byte
	Members [][]byte
}

func encodeCommand(c command) []byte {
	buf := make([]byte, 0, 1+8+len(c.NS)+len(c.Coll))
	buf = append(buf, byte(c.Op))
	buf = appendChunk(buf, c.NS)
	buf = appendChunk(buf, c.Coll)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(c.Members)))
	buf = append(buf, cnt[:]...)
	for _, m := range c.Members {
		buf = appendChunk(buf, m)
	}
	return buf
}

func decodeCommand(b []byte) (command, error) {
	if len(b) < 1 {
		return command{}, errShortCommand
	}
	c := command{Op: opKind(b[0])}
	if c.Op != opSAdd && c.Op != opSRem {
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
	c.Members = make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		var m []byte
		if m, rest, err = takeChunk(rest); err != nil {
			return command{}, err
		}
		c.Members = append(c.Members, m)
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

// writeChunk / readChunk frame a length-prefixed byte slice on a snapshot stream.
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

// prefixEnd returns the smallest key strictly greater than every key with the given prefix (exclusive
// upper bound). A nil result means "to the end of the keyspace".
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
