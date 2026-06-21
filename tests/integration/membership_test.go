//go:build integration

// Package integration holds docker-based integration tests. Run with:
//
//	go test -tags integration ./tests/integration/...
//
// They require a working Docker daemon and the sibling ../wavesdb checkout (the image build
// context is the repo parent). The unit suite (`make test`) does not run these.
package integration

import (
	"encoding/json"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

type memberEntry struct {
	MemberID string `json:"memberId"`
	State    string `json:"state"`
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", "../../docker/docker-compose.yaml"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
}

func membership(t *testing.T, adminPort string) map[string]string {
	t.Helper()
	resp, err := http.Get("http://localhost:" + adminPort + "/admin/membership")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var entries []memberEntry
	if json.NewDecoder(resp.Body).Decode(&entries) != nil {
		return nil
	}
	states := map[string]string{}
	for _, e := range entries {
		states[e.MemberID] = e.State
	}
	return states
}

// waitFor polls until cond is true or the deadline elapses.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestThreeNodeClusterFormsAndDetectsKill(t *testing.T) {
	compose(t, "up", "-d", "--build")
	t.Cleanup(func() { compose(t, "down", "-v") })

	ports := map[string]string{"node1": "7901", "node2": "7902", "node3": "7903"}

	// (TS-020) all three nodes converge to a full ALIVE roster
	waitFor(t, "3-node form-up", 60*time.Second, func() bool {
		for _, p := range ports {
			m := membership(t, p)
			if len(m) != 3 {
				return false
			}
			for _, st := range m {
				if st != "ALIVE" {
					return false
				}
			}
		}
		return true
	})

	// (TS-021) killing node3 makes survivors mark it SUSPECT then UNREACHABLE
	compose(t, "kill", "node3")
	waitFor(t, "node3 suspect/unreachable on survivors", 60*time.Second, func() bool {
		for _, p := range []string{ports["node1"], ports["node2"]} {
			st := membership(t, p)["node3"]
			if st != "SUSPECT" && st != "UNREACHABLE" && st != "DEAD" {
				return false
			}
		}
		return true
	})
}
