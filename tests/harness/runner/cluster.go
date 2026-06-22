//go:build harness

package runner

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"time"
)

// membershipStates fetches the member->state map from a node's admin endpoint.
func membershipStates(adminAddr string) map[string]string {
	resp, err := http.Get("http://" + adminAddr + "/admin/membership")
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	var entries []struct {
		MemberID string `json:"memberId"`
		State    string `json:"state"`
	}
	if json.NewDecoder(resp.Body).Decode(&entries) != nil {
		return nil
	}
	out := map[string]string{}
	for _, e := range entries {
		out[e.MemberID] = e.State
	}
	return out
}

// MembershipStates returns the member->liveness map a node currently sees.
func (c *Cluster) MembershipStates(member string) map[string]string {
	return membershipStates(c.AdminAddr(member))
}

// Cluster is a live multi-node WaveSpan cluster the harness drives via docker compose (design/24).
// It maps member ids to host-mapped data/admin ports so workloads can read from a SPECIFIC replica
// (Jepsen reads each node independently) and nemeses can inject faults from the host.
type Cluster struct {
	composeFile string
	project     string
	members     []string
	dataPort    map[string]string
	adminPort   map[string]string
	container   map[string]string // member -> docker container name
}

// DevCluster is the 3-node docker-compose dev cluster.
func DevCluster() *Cluster {
	return &Cluster{
		composeFile: "../../docker/docker-compose.yaml",
		project:     "wavespan-dev",
		members:     []string{"node1", "node2", "node3"},
		dataPort:    map[string]string{"node1": "7811", "node2": "7812", "node3": "7813"},
		adminPort:   map[string]string{"node1": "7901", "node2": "7902", "node3": "7903"},
		container:   map[string]string{"node1": "wavespan-dev-node1-1", "node2": "wavespan-dev-node2-1", "node3": "wavespan-dev-node3-1"},
	}
}

// Members returns the member ids.
func (c *Cluster) Members() []string { return c.members }

// DataAddr returns the host-mapped data address of a member.
func (c *Cluster) DataAddr(member string) string { return "localhost:" + c.dataPort[member] }

// AdminAddr returns the host-mapped admin address of a member.
func (c *Cluster) AdminAddr(member string) string { return "localhost:" + c.adminPort[member] }

// Container returns the docker container name for a member (for nemesis fault injection).
func (c *Cluster) Container(member string) string { return c.container[member] }

// ComposeFile / Project expose the compose target for nemeses.
func (c *Cluster) ComposeFile() string { return c.composeFile }
func (c *Cluster) Project() string     { return c.project }

func (c *Cluster) compose(args ...string) error {
	out, err := exec.Command("docker", append([]string{"compose", "-p", c.project, "-f", c.composeFile}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker compose %v: %w\n%s", args, err, out)
	}
	return nil
}

// Up brings the cluster up and waits for membership to form.
func (c *Cluster) Up(formTimeout time.Duration) error {
	if err := c.compose("up", "-d"); err != nil {
		return err
	}
	deadline := time.Now().Add(formTimeout)
	for time.Now().Before(deadline) {
		if c.allFormed() {
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("cluster did not form within %s", formTimeout)
}

// Down tears the cluster down (removing volumes).
func (c *Cluster) Down() error { return c.compose("down", "-v") }

// LiveMembers returns members that currently report ALIVE in their own roster.
func (c *Cluster) allFormed() bool {
	for _, m := range c.members {
		if len(membershipStates(c.AdminAddr(m))) != len(c.members) {
			return false
		}
	}
	return true
}
