package benchui

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yannick/wavespan/internal/profile"
)

// nodeSpec is a profiling target as sent by the UI.
type nodeSpec struct {
	Name      string `json:"name"`
	AdminAddr string `json:"adminAddr"`
}

// probeRequest is the POST /api/target/probe body.
type probeRequest struct {
	DataAddr string     `json:"dataAddr"`
	Nodes    []nodeSpec `json:"nodes"`
}

// nodeProbe is the per-node probe result. profiling == reachable: the admin pprof endpoint being
// up IS the profiling-capable signal.
type nodeProbe struct {
	Name      string `json:"name"`
	Reachable bool   `json:"reachable"`
	Profiling bool   `json:"profiling"`
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req probeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	results := make([]nodeProbe, 0, len(req.Nodes))
	for _, n := range req.Nodes {
		ok := profile.Reachable(ctx, profile.Node{Name: n.Name, AdminAddr: n.AdminAddr})
		results = append(results, nodeProbe{Name: n.Name, Reachable: ok, Profiling: ok})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nodes":    results,
		"dataAddr": req.DataAddr,
	})
}

// profileRunRequest is the POST /api/runs/{id}/profile body.
type profileRunRequest struct {
	CPUSeconds int        `json:"cpuSeconds"`
	Nodes      []nodeSpec `json:"nodes"`
}

func (s *Server) handleProfileRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.run(r.PathValue("id")); !ok {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB; these bodies are tiny
	var req profileRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.CPUSeconds <= 0 {
		req.CPUSeconds = 10
	}

	// CaptureCPU blocks for cpuSeconds; give the request a generous timeout.
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.CPUSeconds+30)*time.Second)
	defer cancel()

	// Filter to reachable nodes.
	var nodes []profile.Node
	var nodeNames []string
	for _, n := range req.Nodes {
		pn := profile.Node{Name: n.Name, AdminAddr: n.AdminAddr}
		if profile.Reachable(ctx, pn) {
			nodes = append(nodes, pn)
			nodeNames = append(nodeNames, n.Name)
		}
	}

	cpuRaw := profile.CaptureCPU(ctx, nodes, req.CPUSeconds)
	snapRaw := profile.CaptureSnapshots(ctx, nodes)
	rep := profile.BuildReport("benchui run", nodeNames, req.CPUSeconds, cpuRaw, snapRaw)

	// Flatten raw bytes keyed by "<node>.<kind>" for download.
	raw := map[string][]byte{}
	for node, b := range cpuRaw {
		raw[node+".cpu"] = b
	}
	for node, kinds := range snapRaw {
		for kind, b := range kinds {
			raw[node+"."+kind] = b
		}
	}

	s.profMu.Lock()
	s.profSeq++
	pid := "prof-" + strconv.Itoa(s.profSeq)
	s.profiles[pid] = &profileResult{report: rep, raw: raw}
	s.profMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]string{"pid": pid})
}

func (s *Server) profile(pid string) (*profileResult, bool) {
	s.profMu.Lock()
	defer s.profMu.Unlock()
	p, ok := s.profiles[pid]
	return p, ok
}

func (s *Server) handleProfileReport(w http.ResponseWriter, r *http.Request) {
	p, ok := s.profile(r.PathValue("pid"))
	if !ok {
		http.Error(w, "profile not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, p.report)
}

// handleProfileRaw serves a stored raw pprof file. The {file} wildcard is "<node>.<kind>.pb.gz".
func (s *Server) handleProfileRaw(w http.ResponseWriter, r *http.Request) {
	p, ok := s.profile(r.PathValue("pid"))
	if !ok {
		http.Error(w, "profile not found", http.StatusNotFound)
		return
	}
	file := r.PathValue("file")
	key := strings.TrimSuffix(file, ".pb.gz")
	b, ok := p.raw[key]
	if !ok {
		http.Error(w, "raw profile not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+file+"\"")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}
