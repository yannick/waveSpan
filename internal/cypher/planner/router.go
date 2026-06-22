package planner

// PartitionRouter maps a graph partition to the pod that owns it (design/07 "Fragment routing").
type PartitionRouter interface {
	PodFor(partition uint32) string
}

// LocalRouter maps every partition to the local pod (single-node execution).
type LocalRouter struct{ Self string }

// PodFor returns the local pod for any partition.
func (l LocalRouter) PodFor(uint32) string { return l.Self }

// Route is the outcome of routing a scan/expand across partitions.
type Route struct {
	Fragments            int
	Capped               bool
	PartialGraphPossible bool
}

// RouteFragments bounds the remote fragment fan-out at maxRemoteFragments (design/07): a plan that
// would touch more partitions than the cap is truncated and marked partial_graph_possible.
func RouteFragments(targetPartitions int, limits Limits) Route {
	frags := targetPartitions
	capped := false
	if frags > limits.MaxRemoteFragments {
		frags = limits.MaxRemoteFragments
		capped = true
	}
	return Route{Fragments: frags, Capped: capped, PartialGraphPossible: capped || targetPartitions > 1}
}
