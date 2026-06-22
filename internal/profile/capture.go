package profile

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Node is a cluster member to profile, addressed by its admin (pprof) endpoint.
type Node struct {
	Name      string
	AdminAddr string // host:port of the admin server exposing /debug/pprof
}

// SnapshotKinds are the instantaneous profiles captured after the workload.
var SnapshotKinds = []string{"alloc", "block", "mutex", "goroutine"}

// pprofPath maps an analysis kind to its pprof endpoint path.
var pprofPath = map[string]string{
	"alloc":     "heap",      // heap profile carries alloc_space (cumulative)
	"block":     "block",     // off-CPU blocking (channel/select/sync/network waits)
	"mutex":     "mutex",     // lock contention
	"goroutine": "goroutine", // goroutine stacks (concurrency snapshot)
}

func get(ctx context.Context, addr, path string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	url := fmt.Sprintf("http://%s/debug/pprof/%s", addr, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// CaptureCPU starts a timed CPU profile on every node concurrently and blocks until all complete
// (~seconds). Run the workload concurrently so the profile covers it. Returns node -> raw bytes.
func CaptureCPU(ctx context.Context, nodes []Node, seconds int) map[string][]byte {
	out := map[string][]byte{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n Node) {
			defer wg.Done()
			raw, err := get(ctx, n.AdminAddr, fmt.Sprintf("profile?seconds=%d", seconds), time.Duration(seconds+15)*time.Second)
			if err != nil {
				return
			}
			mu.Lock()
			out[n.Name] = raw
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return out
}

// CaptureSnapshots fetches the instantaneous profiles (alloc/block/mutex/goroutine) from every node.
// Returns node -> kind -> raw bytes.
func CaptureSnapshots(ctx context.Context, nodes []Node) map[string]map[string][]byte {
	out := map[string]map[string][]byte{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, n := range nodes {
		for _, kind := range SnapshotKinds {
			wg.Add(1)
			go func(n Node, kind string) {
				defer wg.Done()
				raw, err := get(ctx, n.AdminAddr, pprofPath[kind], 30*time.Second)
				if err != nil {
					return
				}
				mu.Lock()
				if out[n.Name] == nil {
					out[n.Name] = map[string][]byte{}
				}
				out[n.Name][kind] = raw
				mu.Unlock()
			}(n, kind)
		}
	}
	wg.Wait()
	return out
}

// Reachable reports whether a node's pprof endpoint is up (profiling enabled).
func Reachable(ctx context.Context, n Node) bool {
	_, err := get(ctx, n.AdminAddr, "goroutine?debug=1", 5*time.Second)
	return err == nil
}
