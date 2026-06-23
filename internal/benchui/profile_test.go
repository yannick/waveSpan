package benchui

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeDeadNode(t *testing.T) {
	h := New(Options{}).Handler()
	body := `{"dataAddr":"127.0.0.1:1","nodes":[{"name":"n1","adminAddr":"127.0.0.1:1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/target/probe", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	var resp struct {
		Nodes []struct {
			Name      string `json:"name"`
			Reachable bool   `json:"reachable"`
			Profiling bool   `json:"profiling"`
		} `json:"nodes"`
		DataAddr string `json:"dataAddr"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, rec.Body.String())
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(resp.Nodes))
	}
	n := resp.Nodes[0]
	if n.Reachable || n.Profiling {
		t.Fatalf("dead node should be unreachable/non-profiling, got %+v", n)
	}
}

func TestProfileReportUnknown(t *testing.T) {
	h := New(Options{}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/profile/unknown/report", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
