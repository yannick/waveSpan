package backup

// Selector restricts a logical backup to a subset of the data. Each set is an
// independent, type-specific filter: Namespaces selects KV + collections data,
// Graphs selects graph data, VectorCollections selects vector data. A Selector
// with all sets empty (IsEmpty) means "everything" — a full backup.
type Selector struct {
	Namespaces        map[string]struct{}
	Graphs            map[string]struct{}
	VectorCollections map[string]struct{}
}

// Set builds a string set from the given members (a convenience for callers).
func Set(members ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(members))
	for _, s := range members {
		m[s] = struct{}{}
	}
	return m
}

// IsEmpty reports whether the selector imposes no filter (full backup).
func (s Selector) IsEmpty() bool {
	return len(s.Namespaces) == 0 && len(s.Graphs) == 0 && len(s.VectorCollections) == 0
}

// contains reports whether set holds v.
func contains(set map[string]struct{}, v string) bool {
	_, ok := set[v]
	return ok
}
