package local

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// DigestRecords hashes the (key, version) tuples of a key-ordered record range (design/37 P2.11).
// Both sides of a RangeDigest exchange MUST use this exact scheme: the intra-cluster anti-entropy
// pass compares a local batch's digest against each peer's digest of the same [start, end) range
// and skips per-key fetches when they match. Unlike the cross-cluster rangeHash it includes the
// HLC logical component, so versions differing only in logical ticks still diverge the digest.
func DigestRecords(recs []*wavespanv1.StoredRecord) []byte {
	h := sha256.New()
	var b [8]byte
	for _, rec := range recs {
		v := version.FromProto(rec.GetVersion())
		binary.BigEndian.PutUint64(b[:], uint64(len(rec.GetLogicalKey())))
		h.Write(b[:])
		h.Write(rec.GetLogicalKey())
		binary.BigEndian.PutUint64(b[:], v.HLCPhysicalMs)
		h.Write(b[:])
		binary.BigEndian.PutUint32(b[:4], v.HLCLogical)
		h.Write(b[:4])
		binary.BigEndian.PutUint64(b[:], v.WriterSequence)
		h.Write(b[:])
		h.Write([]byte(v.WriterClusterID))
		h.Write([]byte(v.WriterMemberID))
	}
	return h.Sum(nil)
}
