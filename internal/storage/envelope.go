package storage

import (
	"encoding/binary"

	"github.com/cwire/wavespan/internal/version"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// Envelope encode/decode helpers. StoredRecord, LatestPointer, and MutationEnvelope are the
// authoritative wire/disk forms (design/02_storage_wavesdb.md). They are plain protobuf, so
// encode/decode is proto marshal/unmarshal; these wrappers centralise it and the key layout.

// marshalOpts is reused for the append-marshal helpers below.
var marshalOpts = proto.MarshalOptions{}

// AppendStoredRecord / AppendLatestPointer / AppendMutationEnvelope marshal onto an existing buffer
// (proto MarshalAppend) so the caller can reuse one pooled buffer per write instead of allocating a
// fresh slice per encode. The returned bytes are only valid until the buffer is next reused, so the
// caller must copy them (the storage write path does, via the engine's per-commit copy).
func AppendStoredRecord(buf []byte, r *wavespanv1.StoredRecord) ([]byte, error) {
	return marshalOpts.MarshalAppend(buf, r)
}

// AppendLatestPointer marshals a LatestPointer onto buf.
func AppendLatestPointer(buf []byte, p *wavespanv1.LatestPointer) ([]byte, error) {
	return marshalOpts.MarshalAppend(buf, p)
}

// AppendMutationEnvelope marshals a MutationEnvelope onto buf.
func AppendMutationEnvelope(buf []byte, m *wavespanv1.MutationEnvelope) ([]byte, error) {
	return marshalOpts.MarshalAppend(buf, m)
}

// EncodeStoredRecord marshals a StoredRecord.
func EncodeStoredRecord(r *wavespanv1.StoredRecord) ([]byte, error) { return proto.Marshal(r) }

// DecodeStoredRecord unmarshals a StoredRecord.
func DecodeStoredRecord(b []byte) (*wavespanv1.StoredRecord, error) {
	r := &wavespanv1.StoredRecord{}
	return r, proto.Unmarshal(b, r)
}

// EncodeLatestPointer marshals a LatestPointer.
func EncodeLatestPointer(p *wavespanv1.LatestPointer) ([]byte, error) { return proto.Marshal(p) }

// DecodeLatestPointer unmarshals a LatestPointer.
func DecodeLatestPointer(b []byte) (*wavespanv1.LatestPointer, error) {
	p := &wavespanv1.LatestPointer{}
	return p, proto.Unmarshal(b, p)
}

// EncodeMutationEnvelope marshals a MutationEnvelope.
func EncodeMutationEnvelope(m *wavespanv1.MutationEnvelope) ([]byte, error) { return proto.Marshal(m) }

// DecodeMutationEnvelope unmarshals a MutationEnvelope.
func DecodeMutationEnvelope(b []byte) (*wavespanv1.MutationEnvelope, error) {
	m := &wavespanv1.MutationEnvelope{}
	return m, proto.Unmarshal(b, m)
}

// --- Keyspace builders (design/01_architecture.md "Internal keyspace") ---
//
// Composite keys are length-prefixed so the components are unambiguous regardless of bytes
// in user keys, while a fixed component order keeps prefix scans (e.g. all versions of a
// user key) working. The owning column family supplies the top-level type, so these keys
// omit it.

// KVDataKey builds the key for a versioned record in CFKVData: ns | "data" | userKey | version.
func KVDataKey(namespace string, userKey []byte, v *wavespanv1.Version) []byte {
	return joinKey([]byte(namespace), []byte("data"), userKey, encodeVersion(v))
}

// KVLatestKey builds the latest-pointer key in CFKVMeta: ns | "latest" | userKey.
func KVLatestKey(namespace string, userKey []byte) []byte {
	return joinKey([]byte(namespace), []byte("latest"), userKey)
}

// ReplLogKey builds a local mutation-log key in CFReplLog: "local" | partition | seq(BE).
func ReplLogKey(partition string, seq uint64) []byte {
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], seq)
	return joinKey([]byte("local"), []byte(partition), s[:])
}

// joinKey concatenates components with a varint length prefix on each, making the encoding
// injective (no delimiter-collision ambiguity).
func joinKey(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += binary.MaxVarintLen64 + len(p)
	}
	out := make([]byte, 0, n)
	var tmp [binary.MaxVarintLen64]byte
	for _, p := range parts {
		m := binary.PutUvarint(tmp[:], uint64(len(p)))
		out = append(out, tmp[:m]...)
		out = append(out, p...)
	}
	return out
}

// encodeVersion produces a fixed-order, byte-comparable encoding of a Version suitable as a
// key suffix: physical(BE) | logical(BE) | clusterID | memberID | sequence(BE).
func encodeVersion(v *wavespanv1.Version) []byte {
	if v == nil {
		return nil
	}
	var head [12]byte
	binary.BigEndian.PutUint64(head[0:8], v.GetHlcPhysicalMs())
	binary.BigEndian.PutUint32(head[8:12], v.GetHlcLogical())
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], v.GetWriterSequence())
	return joinKey(head[:], []byte(v.GetWriterClusterId()), []byte(v.GetWriterMemberId()), seq[:])
}

// RebuildLatestPointer derives the latest pointer for a key from its versioned records by
// selecting the hlc-last-write-wins winner (design/02 "Required local invariants" 3,
// design/22 compare order). Tombstone and expiry are carried from the winner. Sibling
// tracking (keep-siblings policy) is added with global active-active in M7; under the LWW
// default the winner stands alone.
func RebuildLatestPointer(records []*wavespanv1.StoredRecord) *wavespanv1.LatestPointer {
	if len(records) == 0 {
		return nil
	}
	winner := records[0]
	for _, r := range records[1:] {
		if version.FromProto(r.GetVersion()).Compare(version.FromProto(winner.GetVersion())) > 0 {
			winner = r
		}
	}
	lp := &wavespanv1.LatestPointer{
		Winner:    winner.GetVersion(),
		Tombstone: winner.GetTombstone(),
	}
	if winner.ExpiresAtUnixMs != nil {
		lp.ExpiresAtUnixMs = proto.Int64(winner.GetExpiresAtUnixMs())
	}
	return lp
}
