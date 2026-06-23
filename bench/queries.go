package benchqueries

import (
	"embed"
	"sort"
	"strings"
)

//go:embed queries/*.cypher
var files embed.FS

// Query is a named, comment-stripped, single-statement cypher query.
type Query struct{ Name, Body string }

// All returns the embedded benchmark query suite (the same bench/queries/*.cypher the CLI reads).
func All() []Query {
	ents, _ := files.ReadDir("queries")
	var out []Query
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".cypher") {
			continue
		}
		b, _ := files.ReadFile("queries/" + e.Name())
		out = append(out, Query{Name: strings.TrimSuffix(e.Name(), ".cypher"), Body: stripComments(string(b))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// stripComments removes // line comments and collapses to a single statement (copied from
// internal/bench/query.go to avoid an import cycle).
func stripComments(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		if strings.TrimSpace(line) != "" {
			b.WriteString(strings.TrimSpace(line))
			b.WriteString(" ")
		}
	}
	return strings.TrimSpace(b.String())
}
