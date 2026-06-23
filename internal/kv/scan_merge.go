package kv

import (
	"bytes"
	"sort"

	"github.com/yannick/wavespan/internal/version"
)

// scanRow is one merged scan row.
type scanRow struct {
	key         []byte
	value       []byte
	version     version.Version
	expiresAtMs *int64
}

// rowMerge merges rows from multiple holders, keeping the hlc-last-write-wins version per key
// (design/03 conflict order). Equivalent to a k-way sorted merge with per-key dedup.
type rowMerge struct {
	byKey map[string]scanRow
}

func newRowMerge() *rowMerge { return &rowMerge{byKey: map[string]scanRow{}} }

func (m *rowMerge) add(r scanRow) {
	id := string(r.key)
	if cur, ok := m.byKey[id]; !ok || r.version.Compare(cur.version) > 0 {
		m.byKey[id] = r
	}
}

// sorted returns the merged rows in ascending key order.
func (m *rowMerge) sorted() []scanRow {
	out := make([]scanRow, 0, len(m.byKey))
	for _, r := range m.byKey {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].key, out[j].key) < 0 })
	return out
}
