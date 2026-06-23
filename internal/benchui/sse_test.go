package benchui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSSEStream(t *testing.T) {
	h := New(Options{}).Handler()
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Create a run against an unroutable addr. The engine still ticks the sampler even when
	// ops error, so samples are emitted without a real cluster.
	body := `{"dataAddr":"127.0.0.1:1","graph":"g","workloads":[{"kind":"kv","params":{"keys":100}}],"concurrency":2,"durationMs":10000}`
	resp, err := http.Post(srv.URL+"/api/runs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var cr struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&cr)
	_ = resp.Body.Close()
	if cr.ID == "" {
		t.Fatal("no run id")
	}

	// Start the run.
	startResp, err := http.Post(srv.URL+"/api/runs/"+cr.ID+"/start", "application/json", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	_ = startResp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/runs/"+cr.ID+"/stream", nil)
	streamResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer func() { _ = streamResp.Body.Close() }()
	if ct := streamResp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	sc := bufio.NewScanner(streamResp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data:") {
			return // got at least one data line
		}
	}
	t.Fatalf("no data: line received within deadline: %v", sc.Err())
}
