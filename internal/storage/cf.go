package storage

// cfNames maps each logical ColumnFamily to its wavesdb column-family name. The order of
// allColumnFamilies mirrors the ColumnFamily iota in store.go.
var cfNames = map[ColumnFamily]string{
	CFSys:         "sys",
	CFKVData:      "kv_data",
	CFKVMeta:      "kv_meta",
	CFGraphData:   "graph_data",
	CFGraphIndex:  "graph_index",
	CFVectorRaw:   "vector_raw",
	CFVectorIndex: "vector_index",
	CFReplLog:     "repl_log",
	CFCacheMeta:   "cache_meta",
	CFReplData:    "repl_data",
}

// allColumnFamilies is the set ensured present on open, in declaration order.
var allColumnFamilies = []ColumnFamily{
	CFSys, CFKVData, CFKVMeta, CFGraphData, CFGraphIndex,
	CFVectorRaw, CFVectorIndex, CFReplLog, CFCacheMeta, CFReplData,
}

// Name returns the wavesdb column-family name for a logical family.
func (cf ColumnFamily) Name() string {
	if n, ok := cfNames[cf]; ok {
		return n
	}
	return "unknown"
}

// String implements fmt.Stringer.
func (cf ColumnFamily) String() string { return cf.Name() }

// valid reports whether cf is a known column family.
func (cf ColumnFamily) valid() bool {
	_, ok := cfNames[cf]
	return ok
}
