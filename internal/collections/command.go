// Package collections implements the replicated-collections consensus tier (design/30): sets, hash
// tables, and sorted sets over range-based multi-Raft (dragonboat, ADR 0008), coexisting with the AP
// cache KV without touching its hot path.
//
// This is the M-0 spike (design/30 §18): one dragonboat shard end to end, an on-disk state machine
// over wavesdb (CFReplData), propose/commit/apply and bounded-stale + linearizable reads, validated
// across restart and a CGO-free build. The cheap-mTLS transport, SWIM node registry, and the
// wavesdb-backed LogDB (Appendix B.4–B.6) are layered in later milestones; for now dragonboat's
// built-in transport and default Pebble LogDB are used.
package collections

import (
	"encoding/binary"
	"errors"
	"io"
)

var errShortCommand = errors.New("collections: short command")

// opKind is the M-0 command opcode. The full LogCommand set (design/30 §5.2) arrives with the typed
// datatypes; the spike only needs PUT/DELETE of a single key.
type opKind byte

const (
	opPut    opKind = 1
	opDelete opKind = 2
)

// command is one proposed mutation. It is the bytes carried in a Raft log entry's Cmd.
type command struct {
	Op  opKind
	Key []byte
	Val []byte
}

func encodeCommand(c command) []byte {
	buf := make([]byte, 0, 1+8+len(c.Key)+len(c.Val))
	buf = append(buf, byte(c.Op))
	buf = appendChunk(buf, c.Key)
	buf = appendChunk(buf, c.Val)
	return buf
}

func decodeCommand(b []byte) (command, error) {
	if len(b) < 1 {
		return command{}, errShortCommand
	}
	c := command{Op: opKind(b[0])}
	rest := b[1:]
	key, rest, err := takeChunk(rest)
	if err != nil {
		return command{}, err
	}
	val, _, err := takeChunk(rest)
	if err != nil {
		return command{}, err
	}
	c.Key, c.Val = key, val
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

// readChunk returns io.EOF cleanly when the stream is exhausted at a frame boundary.
func readChunk(r io.Reader) ([]byte, error) {
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err // io.EOF at a clean boundary
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

// prefixEnd returns the smallest key strictly greater than every key with the given prefix, for an
// exclusive scan upper bound. A nil result means "to the end of the keyspace".
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
