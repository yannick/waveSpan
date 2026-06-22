package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

type namedQuery struct {
	name string
	body string
}

// queryCmd replays every .cypher file in a folder under concurrency for a duration, reporting
// per-query throughput + latency percentiles.
func queryCmd(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address")
	dir := fs.String("queries", "bench/queries", "directory of .cypher query files")
	graph := fs.String("graph", "g", "graph id")
	conc := fs.Int("concurrency", 16, "concurrent clients per query")
	dur := fs.Duration("duration", 15*time.Second, "duration per query")
	if err := fs.Parse(args); err != nil {
		return err
	}
	queries, err := loadQueries(*dir)
	if err != nil {
		return err
	}
	if len(queries) == 0 {
		return fmt.Errorf("no .cypher files in %s", *dir)
	}
	cy := cypherClient(*addr)

	fmt.Printf("# cypher benchmark: %d queries, concurrency=%d, duration=%s each\n", len(queries), *conc, *dur)
	for _, q := range queries {
		lat := runQuery(cy, *graph, q, *conc, *dur)
		fmt.Println(lat.report(q.name, *dur))
	}
	return nil
}

func runQuery(cy interface {
	Query(context.Context, *connect.Request[wavespanv1.CypherRequest]) (*connect.ServerStreamForClient[wavespanv1.CypherResult], error)
}, graph string, q namedQuery, conc int, dur time.Duration) *latencies {
	lat := &latencies{}
	ctx, cancel := context.WithTimeout(context.Background(), dur)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				start := time.Now()
				stream, err := cy.Query(ctx, connect.NewRequest(&wavespanv1.CypherRequest{GraphId: graph, Query: q.body}))
				if err != nil {
					if ctx.Err() == nil {
						lat.addErr()
					}
					continue
				}
				for stream.Receive() { //nolint:revive // drain rows
				}
				if stream.Err() != nil {
					if ctx.Err() == nil {
						lat.addErr()
					}
					continue
				}
				lat.add(time.Since(start))
			}
		}()
	}
	wg.Wait()
	return lat
}

func loadQueries(dir string) ([]namedQuery, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var qs []namedQuery
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".cypher") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		qs = append(qs, namedQuery{name: strings.TrimSuffix(e.Name(), ".cypher"), body: stripComments(string(data))})
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].name < qs[j].name })
	return qs, nil
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
