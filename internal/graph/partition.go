package graph

// NumPartitions is the fixed partition count for graph fragment routing (design/07
// "Fragment routing").
const NumPartitions = 256

// Partition maps a node to a partition by hash(graph_id+node_id) (design/07). Edges partition by
// their start node so a node and its outgoing edges co-locate.
func Partition(graphID, nodeID string) uint32 {
	h := uint32(2166136261)
	for _, b := range []byte(graphID + "\x00" + nodeID) {
		h = (h ^ uint32(b)) * 16777619
	}
	return h % NumPartitions
}
