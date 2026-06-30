package graph

// OfKey extracts the graph name from any CFGraphData or CFGraphIndex key, for
// partial-backup selection. The graph name is the first lp chunk after the key
// prefix. The 2-byte adjacency prefixes ("ao"/"ai") are detected before the 1-byte
// prefixes ("n"/"e"/"l"/"p"). Returns ("", false) for an unknown leading byte or a
// short/malformed key — never panics.
func OfKey(key []byte) (string, bool) {
	var rest []byte
	switch {
	case len(key) >= 2 && (string(key[:2]) == pfxOutAdj || string(key[:2]) == pfxInAdj):
		rest = key[2:]
	case len(key) >= 1 && (string(key[:1]) == pfxNode || string(key[:1]) == pfxEdge ||
		string(key[:1]) == pfxLabel || string(key[:1]) == pfxProp):
		rest = key[1:]
	default:
		return "", false
	}
	g, tail := decodeLP(rest)
	if tail == nil {
		return "", false
	}
	return g, true
}
