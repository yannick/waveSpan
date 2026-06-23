package benchui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	return New(Options{}).Handler()
}

func TestWorkloadsEndpoint(t *testing.T) {
	h := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/workloads", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, rec.Body.String())
	}
	if len(got) == 0 {
		t.Fatalf("expected at least one workload descriptor")
	}
	kinds := map[string]bool{}
	for _, w := range got {
		kinds[w["kind"].(string)] = true
	}
	for _, k := range []string{"kv", "multiget", "cypher"} {
		if !kinds[k] {
			t.Errorf("missing workload kind %q", k)
		}
	}
}

func postRun(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateRunAndConflict(t *testing.T) {
	h := newTestServer(t)
	body := `{"dataAddr":"127.0.0.1:1","graph":"g","workloads":[{"kind":"kv","params":{"keys":100}}],"concurrency":2,"durationMs":1000}`

	rec := postRun(t, h, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("first create status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID == "" {
		t.Fatalf("expected non-empty id")
	}

	// Second create while the first is active and not finished -> 409.
	rec2 := postRun(t, h, body)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("second create status = %d, want 409", rec2.Code)
	}
}
