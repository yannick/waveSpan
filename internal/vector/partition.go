// Package vector implements raw vector storage and exact distributed top-k search (design/08).
// ANN/HNSW and the delta index are M10; this package ships exact search only.
package vector

// NumPartitions is the fixed vector partition count (design/08 "Partitioning").
const NumPartitions = 256

func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, b := range []byte(s) {
		h = (h ^ uint32(b)) * 16777619
	}
	return h
}

// Partition returns the partition of a graph-attached vector: hash(graph_id + node_id) (design/08).
func Partition(graphID, nodeID string) uint32 {
	return hash(graphID+"\x00"+nodeID) % NumPartitions
}

// PartitionBare returns the partition of a bare vector: hash(collection_id + vector_id).
func PartitionBare(collection, vectorID string) uint32 {
	return hash(collection+"\x00"+vectorID) % NumPartitions
}
