//go:build e2e

// Package e2e runs the operator against a real cluster (kind/k3d). It is gated behind the `e2e`
// build tag and skips unless a cluster is reachable, because it needs kubectl + a running cluster
// with the CRDs and operator installed:
//
//	kind create cluster
//	kubectl apply -f operator/config/crd/bases
//	# deploy the operator (helm/kustomize), then:
//	go test -tags e2e ./operator/test/e2e -run OperatorKind
package e2e

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

func kubectl(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	return string(out), err
}

func requireCluster(t *testing.T) {
	t.Helper()
	if _, err := kubectl(t, "cluster-info"); err != nil {
		t.Skip("no reachable Kubernetes cluster; skipping operator e2e")
	}
}

func TestOperatorKindDeploysCluster(t *testing.T) {
	requireCluster(t)
	if out, err := kubectl(t, "apply", "-f", "../../config/samples/wavespancluster.yaml"); err != nil {
		t.Fatalf("apply samples: %v\n%s", err, out)
	}
	t.Cleanup(func() { _, _ = kubectl(t, "delete", "-f", "../../config/samples/wavespancluster.yaml", "--ignore-not-found") })

	// the StatefulSet + headless service + PDB should appear
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if out, _ := kubectl(t, "get", "statefulset", "demo", "-o", "jsonpath={.spec.replicas}"); strings.TrimSpace(out) == "3" {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("operator did not create the data StatefulSet")
}

func TestOperatorKindRejectsInvalidPolicy(t *testing.T) {
	requireCluster(t)
	// the validating webhook must reject require-local-geo with no compliance boundary
	out, err := kubectl(t, "apply", "-f", "../../config/samples/invalid_policy.yaml")
	if err == nil {
		t.Fatalf("invalid policy should have been rejected, got: %s", out)
	}
}
