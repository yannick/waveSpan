// Package benchui implements the HTTP server for the benchmarking web UI: it drives the
// benchengine run engine, streams live samples over SSE, exposes pprof-based profiling
// endpoints, and serves the embedded single-page app.
package benchui

import (
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/yannick/wavespan/internal/benchengine"
	"github.com/yannick/wavespan/internal/profile"
)

// Options configures the benchui server.
type Options struct {
	Addr string
}

// profileResult holds a completed profiling capture: the analyzed report plus the raw
// pprof bytes keyed by "<node>.<kind>" for download.
type profileResult struct {
	report *profile.Report
	raw    map[string][]byte // "node.kind" -> raw pb.gz
}

// Server is the benchui HTTP server. It owns a single active run (a second concurrent run is
// rejected with 409 until the active one finishes) and a registry of profiling results.
type Server struct {
	opts Options
	spa  fs.FS

	mu     sync.Mutex
	active *benchengine.Run
	runs   map[string]*benchengine.Run
	runSeq int

	profMu   sync.Mutex
	profiles map[string]*profileResult
	profSeq  int
}

// New builds a Server.
func New(opts Options) *Server {
	return &Server{
		opts:     opts,
		spa:      spaFS(),
		runs:     map[string]*benchengine.Run{},
		profiles: map[string]*profileResult{},
	}
}

// Handler returns the HTTP handler with the /api routes and the SPA catch-all.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/workloads", s.handleWorkloads)
	mux.HandleFunc("POST /api/runs", s.handleCreateRun)
	mux.HandleFunc("GET /api/runs/{id}", s.handleGetRun)
	mux.HandleFunc("POST /api/runs/{id}/start", s.handleRunControl("start"))
	mux.HandleFunc("POST /api/runs/{id}/pause", s.handleRunControl("pause"))
	mux.HandleFunc("POST /api/runs/{id}/resume", s.handleRunControl("resume"))
	mux.HandleFunc("POST /api/runs/{id}/stop", s.handleRunControl("stop"))

	// SSE + dataset load (TASK 5)
	mux.HandleFunc("GET /api/runs/{id}/stream", s.handleStream)
	mux.HandleFunc("POST /api/dataset/load", s.handleDatasetLoad)

	// Profiling (TASK 6)
	mux.HandleFunc("POST /api/target/probe", s.handleProbe)
	mux.HandleFunc("POST /api/runs/{id}/profile", s.handleProfileRun)
	mux.HandleFunc("GET /api/profile/{pid}/report", s.handleProfileReport)
	mux.HandleFunc("GET /api/profile/{pid}/raw/{file}", s.handleProfileRaw)

	mux.HandleFunc("/", s.serveSPA)
	return mux
}

// serveSPA mirrors internal/ui/server.go: it serves embedded files, falling back to index.html
// for unknown routes (SPA client-side routing), with immutable caching for hashed assets.
func (s *Server) serveSPA(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	if p == "" {
		p = "index.html"
	}
	f, err := s.spa.Open(p)
	if err != nil {
		s.serveIndex(w)
		return
	}
	_ = f.Close()
	if strings.HasPrefix(p, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.FileServer(http.FS(s.spa)).ServeHTTP(w, r)
}

func (s *Server) serveIndex(w http.ResponseWriter) {
	data, err := fs.ReadFile(s.spa, "index.html")
	if err != nil {
		http.Error(w, "ui not built", http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// run looks up a run by id under the lock.
func (s *Server) run(id string) (*benchengine.Run, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.runs[id]
	return r, ok
}

// nextRunID generates the next monotonic run id. Caller holds s.mu.
func (s *Server) nextRunID() string {
	s.runSeq++
	return "run-" + strconv.Itoa(s.runSeq)
}
