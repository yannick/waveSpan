//go:build harness

package nemesis

import (
	"os/exec"

	"github.com/cwire/wavespan/tests/harness/runner"
)

// Live docker nemeses inject faults from the HOST onto the compose network — exactly Jepsen's
// out-of-band control plane (the scratch containers have no shell/tc/iptables inside). All are
// pure os/exec (no CGO, design/17).

func dockerRun(args ...string) { _ = exec.Command("docker", args...).Run() }

// DockerKill kills a member (SIGKILL) on Start and restarts it on Stop — Jepsen's node-start-stopper.
func DockerKill(c *runner.Cluster) Nemesis {
	return New("node-kill",
		func(targets []string) {
			for _, m := range targets {
				dockerRun("kill", c.Container(m))
			}
		},
		func(targets []string) {
			for _, m := range targets {
				dockerRun("start", c.Container(m))
			}
		})
}

// DockerPause stops the world on a member (SIGSTOP) and resumes it (SIGCONT) — Jepsen's hammer-time.
func DockerPause(c *runner.Cluster) Nemesis {
	return New("pause",
		func(targets []string) {
			for _, m := range targets {
				dockerRun("pause", c.Container(m))
			}
		},
		func(targets []string) {
			for _, m := range targets {
				dockerRun("unpause", c.Container(m))
			}
		})
}

// DockerPartition isolates the target members from the compose network (a network partition) and
// reconnects them on Stop — Jepsen's partition-halves/partition-random-node.
func DockerPartition(c *runner.Cluster) Nemesis {
	network := c.Project() + "_default"
	return New("partition-halves",
		func(targets []string) {
			for _, m := range targets {
				dockerRun("network", "disconnect", network, c.Container(m))
			}
		},
		func(targets []string) {
			for _, m := range targets {
				dockerRun("network", "connect", network, c.Container(m))
			}
		})
}
