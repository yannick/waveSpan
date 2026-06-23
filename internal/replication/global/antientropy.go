package global

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/version"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// AntiEntropy compares per-range content hashes with a peer and repairs divergent ranges by
// fetching and re-applying records (design/06 "Anti-entropy"). It is mandatory because partitions
// and VPN restarts cause missed replication.
type AntiEntropy struct {
	store *recordstore.Store
}

// NewAntiEntropy builds an anti-entropy helper over the local store.
func NewAntiEntropy(store *recordstore.Store) *AntiEntropy { return &AntiEntropy{store: store} }

// rangeHash hashes the (key, version) tuples visible locally in a range — order-independent within
// the range because records are scanned in sorted key order.
func (ae *AntiEntropy) rangeHash(r *wavespanv1.KeyRange) []byte {
	recs, err := ae.store.ScanRecords(r.GetNamespace(), r.GetStart(), r.GetEnd())
	if err != nil {
		return nil
	}
	h := sha256.New()
	for _, rec := range recs {
		v := version.FromProto(rec.GetVersion())
		h.Write(rec.GetLogicalKey())
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v.HLCPhysicalMs)
		h.Write(b[:])
		binary.BigEndian.PutUint64(b[:], v.WriterSequence)
		h.Write(b[:])
		h.Write([]byte(v.WriterClusterID))
		h.Write([]byte(v.WriterMemberID))
	}
	return h.Sum(nil)
}

// Summarize returns content hashes for the requested ranges.
func (ae *AntiEntropy) Summarize(ranges []*wavespanv1.KeyRange) []*wavespanv1.RangeHash {
	out := make([]*wavespanv1.RangeHash, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, &wavespanv1.RangeHash{Range: r, Hash: ae.rangeHash(r)})
	}
	return out
}

// FetchRange returns the local records in a range as GlobalMutations for a diverged peer to apply.
func (ae *AntiEntropy) FetchRange(r *wavespanv1.KeyRange) []*wavespanv1.GlobalMutation {
	recs, err := ae.store.ScanRecords(r.GetNamespace(), r.GetStart(), r.GetEnd())
	if err != nil {
		return nil
	}
	out := make([]*wavespanv1.GlobalMutation, 0, len(recs))
	for _, rec := range recs {
		v := version.FromProto(rec.GetVersion())
		out = append(out, &wavespanv1.GlobalMutation{
			Id:        &wavespanv1.GlobalMutationId{ClusterId: v.WriterClusterID, MemberId: v.WriterMemberID, WriterSequence: v.WriterSequence},
			Namespace: rec.GetNamespace(), Key: rec.GetLogicalKey(), Record: rec,
			Partition: Partition(rec.GetNamespace(), rec.GetLogicalKey()),
		})
	}
	return out
}
