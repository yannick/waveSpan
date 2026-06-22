package vector

import (
	"strings"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"google.golang.org/protobuf/proto"
)

// Raw vectors replicate globally through the same mutation stream as KV (design/06/08): only raw
// records cross the wire, never ANN segments. A vector is wrapped in a StoredRecord under a
// reserved namespace so the receiver can route it to the vector store + local index.
const vecNSPrefix = "\x00vec\x00"

// MutationNamespace is the reserved replication namespace for a vector collection.
func MutationNamespace(collection string) string { return vecNSPrefix + collection }

// IsMutationNamespace reports whether a namespace carries a wrapped vector.
func IsMutationNamespace(ns string) bool { return strings.HasPrefix(ns, vecNSPrefix) }

// CollectionFromNamespace extracts the collection from a vector replication namespace.
func CollectionFromNamespace(ns string) string { return strings.TrimPrefix(ns, vecNSPrefix) }

// Wrap encodes a vector record into a StoredRecord for replication.
func Wrap(rec *wavespanv1.VectorRecord) (*wavespanv1.StoredRecord, error) {
	b, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return &wavespanv1.StoredRecord{
		Namespace:  MutationNamespace(rec.GetCollection()),
		LogicalKey: []byte(rec.GetVectorId()),
		Version:    rec.GetVersion(),
		Tombstone:  rec.GetTombstone(),
		Value:      &wavespanv1.ValueBody{Body: &wavespanv1.ValueBody_Inline{Inline: b}},
	}, nil
}

// Unwrap decodes a replicated StoredRecord back into a vector record.
func Unwrap(rec *wavespanv1.StoredRecord) (*wavespanv1.VectorRecord, error) {
	v := &wavespanv1.VectorRecord{}
	if err := proto.Unmarshal(rec.GetValue().GetInline(), v); err != nil {
		return nil, err
	}
	return v, nil
}
