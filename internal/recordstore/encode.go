// Package recordstore is the local versioned-record primitive shared by the public KV
// coordinator and the StoreReplica receiver: key encoding, atomic apply (record + latest
// pointer + mutation log), and reads (design/02, design/05).
package recordstore

import (
	"encoding/binary"

	"github.com/cwire/wavespan/internal/version"
)

// Key layout (design/01_architecture.md "Internal keyspace"), with column families supplying
// the top-level type so keys omit it:
//
//	CFKVMeta  latestKey(ns, userKey)        = lenPrefix(ns) || userKey
//	CFKVData  dataKey(ns, userKey, version) = lenPrefix(ns) || lenPrefix(userKey) || versionEnc
//
// The latest-pointer key appends the raw user key after a length-prefixed namespace, so user
// keys sort in their natural byte order within a namespace — range scans (M6) are correct.

// latestKey builds the order-preserving latest-pointer key for CFKVMeta.
func latestKey(namespace string, userKey []byte) []byte {
	out := lenPrefix(nil, []byte(namespace))
	return append(out, userKey...)
}

// namespacePrefix is the scan prefix covering all latest-pointer keys in a namespace.
func namespacePrefix(namespace string) []byte {
	return lenPrefix(nil, []byte(namespace))
}

// dataKey builds the versioned-record key for CFKVData. Versions of one user key are grouped
// under lenPrefix(ns)||lenPrefix(userKey); the version suffix makes each unique.
func dataKey(namespace string, userKey []byte, v version.Version) []byte {
	out := lenPrefix(nil, []byte(namespace))
	out = lenPrefix(out, userKey)
	return append(out, encodeVersion(v)...)
}

// dataKeyPrefix is the scan prefix covering all versions of one user key.
func dataKeyPrefix(namespace string, userKey []byte) []byte {
	out := lenPrefix(nil, []byte(namespace))
	return lenPrefix(out, userKey)
}

// lenPrefix appends uvarint(len(b)) || b to dst.
func lenPrefix(dst, b []byte) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(b)))
	dst = append(dst, tmp[:n]...)
	return append(dst, b...)
}

// encodeVersion is a fixed-width, byte-comparable version suffix: physical(BE) | logical(BE) |
// lenPrefix(cluster) | lenPrefix(member) | sequence(BE).
func encodeVersion(v version.Version) []byte {
	var head [12]byte
	binary.BigEndian.PutUint64(head[0:8], v.HLCPhysicalMs)
	binary.BigEndian.PutUint32(head[8:12], v.HLCLogical)
	out := append([]byte(nil), head[:]...)
	out = lenPrefix(out, []byte(v.WriterClusterID))
	out = lenPrefix(out, []byte(v.WriterMemberID))
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], v.WriterSequence)
	return append(out, seq[:]...)
}
