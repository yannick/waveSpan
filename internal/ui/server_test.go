package ui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServesEmbeddedIndex(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `id="root"`) {
		t.Fatalf("index not served: status=%d body=%q", resp.StatusCode, body)
	}
	if resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("index.html should be no-cache, got %q", resp.Header.Get("Cache-Control"))
	}
}

func TestSPAFallback(t *testing.T) {
	ts := httptest.NewServer(NewServer().Handler())
	defer ts.Close()
	// an unknown client-side route falls back to index.html (SPA routing)
	resp, err := http.Get(ts.URL + "/cluster/topology")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `id="root"`) {
		t.Fatalf("SPA fallback failed: status=%d", resp.StatusCode)
	}
}
