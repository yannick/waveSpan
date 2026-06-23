package benchui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestCollectionsCatalog asserts the workload catalog advertises the collections kinds.
func TestCollectionsCatalog(t *testing.T) {
	s := New(Options{})
	req := httptest.NewRequest(http.MethodGet, "/api/workloads", nil)
	rec := httptest.NewRecorder()
	s.handleWorkloads(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var catalog []workloadDesc
	if err := json.Unmarshal(rec.Body.Bytes(), &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	kinds := map[string]bool{}
	for _, w := range catalog {
		kinds[w.Kind] = true
	}
	for _, want := range []string{"set", "hash", "zset", "bulkremove"} {
		if !kinds[want] {
			t.Errorf("catalog missing kind %q; got %v", want, kinds)
		}
	}
}

// TestCollectionsBulkRemoveError asserts a dead data addr yields a structured non-2xx error,
// not a panic, even with a context that times out fast.
func TestCollectionsBulkRemoveError(t *testing.T) {
	s := New(Options{})
	body := `{"dataAddr":"127.0.0.1:1","namespace":"x","member":"doomed"}`
	req := httptest.NewRequest(http.MethodPost, "/api/collections/bulk-remove", strings.NewReader(body))
	ctx, cancel := context.WithTimeout(req.Context(), 200*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	s.handleCollectionsBulkRemove(rec, req)

	if rec.Code < 400 {
		t.Fatalf("status = %d, want >= 400 (structured error)", rec.Code)
	}
}

// TestCollectionsSeedStream asserts the seed endpoint opens an SSE stream for a well-formed body.
func TestCollectionsSeedStream(t *testing.T) {
	s := New(Options{})
	body := `{"dataAddr":"127.0.0.1:1","namespace":"x","sets":1,"filler":0,"member":"doomed","concurrency":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/collections/seed", strings.NewReader(body))
	ctx, cancel := context.WithTimeout(req.Context(), 500*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	s.handleCollectionsSeed(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
}
