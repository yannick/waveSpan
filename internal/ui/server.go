package ui

import (
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

// Server serves the embedded SPA with single-page-app fallback (unknown routes -> index.html) and
// content-hashed asset caching. When WAVESPAN_UI_DEV=1 it reverse-proxies asset requests to the
// Vite dev server instead (RPC requests always hit the in-process handlers, which are mounted
// separately on the admin mux).
type Server struct {
	assets   fs.FS
	devProxy http.Handler
}

// NewServer builds the UI server from the embedded assets, enabling the Vite dev proxy when
// WAVESPAN_UI_DEV=1.
func NewServer() *Server {
	s := &Server{assets: Assets()}
	if os.Getenv("WAVESPAN_UI_DEV") == "1" {
		if u, err := url.Parse("http://localhost:5173"); err == nil {
			s.devProxy = httputil.NewSingleHostReverseProxy(u)
		}
	}
	return s
}

// Handler returns the HTTP handler serving the SPA.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.devProxy != nil {
			s.devProxy.ServeHTTP(w, r)
			return
		}
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "index.html"
		}
		f, err := s.assets.Open(p)
		if err != nil {
			s.serveIndex(w, r) // SPA fallback
			return
		}
		_ = f.Close()
		// content-hashed assets are immutable; index.html must not be cached.
		if strings.HasPrefix(p, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		http.FileServer(http.FS(s.assets)).ServeHTTP(w, r)
	})
}

func (s *Server) serveIndex(w http.ResponseWriter, _ *http.Request) {
	data, err := fs.ReadFile(s.assets, "index.html")
	if err != nil {
		http.Error(w, "ui not built", http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}
