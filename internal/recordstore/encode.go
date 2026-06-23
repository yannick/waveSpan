// Package recordstore is the local versioned-record primitive shared by the public KV
// coordinator and the StoreReplica receiver: key encoding, atomic apply (record + latest
// pointer + mutation log), and reads (design/02, design/05).
package recordstore

import (
	"encoding/binary"

	"github.com/yannick/wavespan/internal/version"
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

// TTL bucket index keys live in CFKVMeta behind a 0xff sentinel (latest-pointer keys begin with a
// uvarint namespace length < 0x80, so they never collide). The bucket-start prefix sorts the index
// by expiry so the sweeper scans only the due buckets.
//
//	ttlKey = 0xff | bucketStart(BE) | lenPrefix(ns) | userKey  -> value = encodeVersion(version)
const (
	ttlSentinel = 0xff
	ttlBucketMs = 60_000 // coarse 60s buckets bound write amplification (design/03)
)

func ttlBucketStart(expiresMs int64) int64 { return (expiresMs / ttlBucketMs) * ttlBucketMs }

func ttlKey(expiresMs int64, namespace string, userKey []byte) []byte {
	out := make([]byte, 0, 1+8+len(namespace)+len(userKey)+2)
	out = append(out, ttlSentinel)
	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], uint64(ttlBucketStart(expiresMs)))
	out = append(out, bs[:]...)
	out = lenPrefix(out, []byte(namespace))
	return append(out, userKey...)
}

// ttlLowBound / ttlScanBound bracket the ttl index entries due to expire by nowMs.
func ttlLowBound() []byte { return []byte{ttlSentinel} }

func ttlScanBound(nowMs int64) []byte {
	out := []byte{ttlSentinel}
	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], uint64(ttlBucketStart(nowMs)+ttlBucketMs))
	return append(out, bs[:]...)
}

// parseTTLKey extracts the namespace and user key from a ttl index key.
func parseTTLKey(k []byte) (namespace string, userKey []byte, ok bool) {
	if len(k) < 9 || k[0] != ttlSentinel {
		return "", nil, false
	}
	rest := k[9:] // skip sentinel + 8-byte bucket start
	nsLen, n := binary.Uvarint(rest)
	if n <= 0 || int(nsLen) > len(rest)-n {
		return "", nil, false
	}
	return string(rest[n : n+int(nsLen)]), rest[n+int(nsLen):], true
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
