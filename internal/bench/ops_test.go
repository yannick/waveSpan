package bench

import (
	"context"
	"strings"
	"testing"
	"time"

	benchqueries "github.com/yannick/wavespan/bench"
)

// The exported clients and single-op functions must exist, so both the CLI runners and
// internal/benchengine can share them. Symbol-existence only (compile-time); behavior is in Step 3b.
func TestExportedSurface(t *testing.T) {
	_ = KVClient
	_ = CypherClient
	_ = OpKVRead
	_ = OpKVWrite
	_ = OpMultiGet
	_ = OpCypher
}

// OpKVRead against a dead address must surface a transport error (no server needed).
func TestOpKVReadDeadAddr(t *testing.T) {
	c := KVClient("127.0.0.1:1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := OpKVRead(ctx, c, "ns", "k"); err == nil {
		t.Fatal("expected transport error against dead address, got nil")
	}
}

// The embedded query suite must yield 6 named, comment-stripped queries.
func TestBenchQueriesAll(t *testing.T) {
	qs := benchqueries.All()
	if len(qs) != 6 {
		t.Fatalf("expected 6 queries, got %d", len(qs))
	}
	for _, q := range qs {
		if strings.TrimSpace(q.Body) == "" {
			t.Errorf("query %q has empty body", q.Name)
		}
		if strings.Contains(q.Body, "//") {
			t.Errorf("query %q body still contains a // comment: %q", q.Name, q.Body)
		}
	}
}
