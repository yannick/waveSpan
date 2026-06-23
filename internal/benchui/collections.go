package benchui

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/yannick/wavespan/internal/benchengine"
)

// collectionsSeedRequest is the POST /api/collections/seed body. It seeds `Sets` sets, each
// containing `Member` (+ `Filler` bytes), into `Namespace` over `DataAddr`.
type collectionsSeedRequest struct {
	DataAddr    string `json:"dataAddr"`
	Namespace   string `json:"namespace"`
	Sets        int    `json:"sets"`
	Filler      int    `json:"filler"`
	Member      string `json:"member"`
	Concurrency int    `json:"concurrency"`
}

// collectionsBulkRemoveRequest is the POST /api/collections/bulk-remove body.
type collectionsBulkRemoveRequest struct {
	DataAddr  string `json:"dataAddr"`
	Namespace string `json:"namespace"`
	Member    string `json:"member"`
}

// collectionsSweepRequest is the POST /api/collections/sweep body.
type collectionsSweepRequest struct {
	DataAddr    string `json:"dataAddr"`
	Namespace   string `json:"namespace"`
	Member      string `json:"member"`
	Sizes       []int  `json:"sizes"`
	Filler      int    `json:"filler"`
	Concurrency int    `json:"concurrency"`
}

// defaultConcurrency clamps a zero/negative concurrency to a sane default.
func defaultConcurrency(c int) int {
	if c < 1 {
		return 64
	}
	return c
}

// handleCollectionsSeed runs benchengine.SeedSets in a goroutine and streams its progress as SSE.
// Each progress callback emits a JSON frame {"done":d,"total":t}; on completion any seeding error
// is streamed as a final {"error":"..."} frame, then "done".
func (s *Server) handleCollectionsSeed(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req collectionsSeedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	msgs := make(chan string, 64)
	done := make(chan struct{})
	var seedErr error
	go func() {
		defer close(done)
		seedErr = benchengine.SeedSets(ctx, req.DataAddr, req.Namespace, req.Sets, req.Filler,
			defaultConcurrency(req.Concurrency), []byte(req.Member), func(d, total int) {
				b, _ := json.Marshal(map[string]int{"done": d, "total": total})
				select {
				case msgs <- string(b):
				case <-ctx.Done():
				}
			})
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			// Drain buffered progress, emit any error, then close.
			for {
				select {
				case msg := <-msgs:
					writeSSEData(w, flusher, msg)
				default:
					if seedErr != nil {
						b, _ := json.Marshal(map[string]string{"error": seedErr.Error()})
						writeSSEData(w, flusher, string(b))
					}
					writeSSEData(w, flusher, "done")
					return
				}
			}
		case msg := <-msgs:
			writeSSEData(w, flusher, msg)
		}
	}
}

// handleCollectionsBulkRemove runs one full-namespace BulkRemove and returns the timing as plain
// JSON. This is a single blocking call with no mid-progress, so there is no SSE here; on upstream
// failure (e.g. unreachable cluster) it replies 502 Bad Gateway.
func (s *Server) handleCollectionsBulkRemove(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req collectionsBulkRemoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	res, err := benchengine.RunFullNamespaceRemove(r.Context(), req.DataAddr, req.Namespace, []byte(req.Member))
	if err != nil {
		http.Error(w, "bulk remove: "+err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleCollectionsSweep runs benchengine.Sweep in a goroutine, streaming its textual progress as
// SSE. When it returns, it streams a final {"points":[...]} frame (parsed by the UI to draw the
// chart) followed by "done"; any error is streamed as {"error":"..."}.
func (s *Server) handleCollectionsSweep(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req collectionsSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	msgs := make(chan string, 64)
	done := make(chan struct{})
	var points []benchengine.SweepPoint
	var sweepErr error
	go func() {
		defer close(done)
		points, sweepErr = benchengine.Sweep(ctx, req.DataAddr, req.Namespace, []byte(req.Member),
			req.Sizes, req.Filler, defaultConcurrency(req.Concurrency), func(msg string) {
				select {
				case msgs <- msg:
				case <-ctx.Done():
				}
			})
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			// Drain buffered progress, emit the points/error frame, then close.
			for {
				select {
				case msg := <-msgs:
					writeSSEData(w, flusher, msg)
				default:
					if sweepErr != nil {
						b, _ := json.Marshal(map[string]string{"error": sweepErr.Error()})
						writeSSEData(w, flusher, string(b))
					}
					b, _ := json.Marshal(map[string]any{"points": points})
					writeSSEData(w, flusher, string(b))
					writeSSEData(w, flusher, "done")
					return
				}
			}
		case msg := <-msgs:
			writeSSEData(w, flusher, msg)
		}
	}
}
