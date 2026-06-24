package bench

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// NamedQuery is a query file's name + body.
type NamedQuery struct {
	Name string
	Body string
}

// QueryResult is one query's measured latencies.
type QueryResult struct {
	Name string
	Lat  *Latencies
}

// LoadQueries reads every .cypher file in dir (comments stripped, collapsed to one statement).
func LoadQueries(dir string) ([]NamedQuery, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var qs []NamedQuery
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".cypher") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		qs = append(qs, NamedQuery{Name: strings.TrimSuffix(e.Name(), ".cypher"), Body: stripComments(string(data))})
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].Name < qs[j].Name })
	return qs, nil
}

// RunQueries replays each query under concurrency for dur, returning per-query latencies.
func RunQueries(addr, graph string, queries []NamedQuery, conc int, dur time.Duration) []QueryResult {
	cy := CypherClient(addr)
	out := make([]QueryResult, 0, len(queries))
	for _, q := range queries {
		out = append(out, QueryResult{Name: q.Name, Lat: runOneQuery(cy, graph, q, conc, dur)})
	}
	return out
}

func runOneQuery(cy wavespanv1.CypherClient, graph string, q NamedQuery, conc int, dur time.Duration) *Latencies {
	lat := &Latencies{}
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				start := time.Now()
				if err := OpCypher(ctx, cy, graph, q.Body); err != nil {
					if ctx.Err() == nil {
						lat.AddErr()
					}
					continue
				}
				lat.Add(time.Since(start))
			}
		}()
	}
	wg.Wait()
	return lat
}

// stripComments removes // line comments and collapses to a single statement.
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
