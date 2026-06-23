package benchui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/yannick/wavespan/internal/bench"
)

// handleStream streams live benchengine samples for a run as Server-Sent Events. It returns when
// the client disconnects (r.Context().Done()) or the run's sample channel closes.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	run, ok := s.run(r.PathValue("id"))
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
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

	ch, unsub := run.Subscribe()
	defer unsub()

	for {
		select {
		case <-r.Context().Done():
			return
		case sample, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(sample)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return // client gone
			}
			flusher.Flush()
		}
	}
}

// datasetLoadRequest is the POST /api/dataset/load body.
type datasetLoadRequest struct {
	DataAddr string `json:"dataAddr"`
	Graph    string `json:"graph"`
	Users    int    `json:"users"`
	Follows  int    `json:"follows"`
	KV       int    `json:"kv"`
}

// handleDatasetLoad runs bench.Load in a goroutine and streams its progress callbacks as SSE.
func (s *Server) handleDatasetLoad(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req datasetLoadRequest
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
	go func() {
		defer close(done)
		bench.Load(ctx, req.DataAddr, bench.LoadOptions{
			Graph:   req.Graph,
			Users:   req.Users,
			Follows: req.Follows,
			KV:      req.KV,
		}, func(msg string) {
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
			// Drain any remaining buffered messages, then close.
			for {
				select {
				case msg := <-msgs:
					writeSSEData(w, flusher, msg)
				default:
					writeSSEData(w, flusher, "done")
					return
				}
			}
		case msg := <-msgs:
			writeSSEData(w, flusher, msg)
		}
	}
}

func writeSSEData(w http.ResponseWriter, flusher http.Flusher, msg string) {
	_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
	flusher.Flush()
}
