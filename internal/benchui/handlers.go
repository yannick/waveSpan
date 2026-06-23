package benchui

import (
	"encoding/json"
	"net/http"
	"time"

	benchqueries "github.com/yannick/wavespan/bench"
	"github.com/yannick/wavespan/internal/benchengine"
)

// paramDesc describes a single workload parameter for the UI form.
type paramDesc struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Default any    `json:"default"`
}

// workloadDesc describes a workload kind and its parameters.
type workloadDesc struct {
	Kind   string      `json:"kind"`
	Params []paramDesc `json:"params"`
}

var workloadCatalog = []workloadDesc{
	{Kind: "kv", Params: []paramDesc{
		{Name: "keys", Type: "int", Default: 10000},
		{Name: "valueSize", Type: "int", Default: 256},
		{Name: "readRatio", Type: "float", Default: 0.9},
	}},
	{Kind: "multiget", Params: []paramDesc{
		{Name: "keys", Type: "int", Default: 10000},
		{Name: "batch", Type: "int", Default: 16},
	}},
	{Kind: "cypher", Params: []paramDesc{
		{Name: "queries", Type: "string", Default: "all"},
	}},
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleWorkloads(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, workloadCatalog)
}

// createRunRequest is the POST /api/runs body.
type createRunRequest struct {
	DataAddr  string `json:"dataAddr"`
	Graph     string `json:"graph"`
	Workloads []struct {
		Kind   string         `json:"kind"`
		Params map[string]any `json:"params"`
	} `json:"workloads"`
	Concurrency int `json:"concurrency"`
	DurationMs  int `json:"durationMs"`
}

// runFinished reports whether a run is in a terminal state and can be replaced.
func runFinished(r *benchengine.Run) bool {
	switch r.State() {
	case benchengine.StateStopped, benchengine.StateDone:
		return true
	default:
		return false
	}
}

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	specs := make([]benchengine.WorkloadSpec, 0, len(req.Workloads))
	hasCypher := false
	for _, wl := range req.Workloads {
		if wl.Kind == "cypher" {
			hasCypher = true
		}
		specs = append(specs, benchengine.WorkloadSpec{Kind: wl.Kind, Params: wl.Params})
	}

	cfg := benchengine.Config{
		DataAddr:    req.DataAddr,
		Graph:       req.Graph,
		Workloads:   specs,
		Concurrency: req.Concurrency,
		Duration:    time.Duration(req.DurationMs) * time.Millisecond,
	}
	if hasCypher {
		cfg.CypherQueries = benchqueries.All()
	}

	s.mu.Lock()
	if s.active != nil && !runFinished(s.active) {
		s.mu.Unlock()
		http.Error(w, "a run is already active", http.StatusConflict)
		return
	}
	run, err := benchengine.New(cfg)
	if err != nil {
		s.mu.Unlock()
		http.Error(w, "create run: "+err.Error(), http.StatusBadRequest)
		return
	}
	id := s.nextRunID()
	s.runs[id] = run
	s.active = run
	s.activeID = id
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	run, ok := s.run(r.PathValue("id"))
	if !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"state":   run.State().String(),
		"summary": run.Summary(),
	})
}

func (s *Server) handleRunControl(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		run, ok := s.run(r.PathValue("id"))
		if !ok {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		switch action {
		case "start":
			run.Start()
		case "pause":
			run.Pause()
		case "resume":
			run.Resume()
		case "stop":
			run.Stop()
		}
		w.WriteHeader(http.StatusOK)
	}
}
