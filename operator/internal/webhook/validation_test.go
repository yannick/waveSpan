package webhook

import (
	"testing"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
)

func validPolicy() *dbv1alpha1.ReplicationPolicySpec {
	return &dbv1alpha1.ReplicationPolicySpec{
		Mode:     dbv1alpha1.ModeLocalCache,
		Local:    dbv1alpha1.LocalReplicationSpec{TargetNearbyReplicas: 3, MinAckNearbyReplicas: 1},
		Conflict: dbv1alpha1.ConflictSpec{Policy: "hlc-last-write-wins"},
	}
}

func TestValidPolicyAccepted(t *testing.T) {
	if errs := ValidateReplicationPolicy(validPolicy()); len(errs) != 0 {
		t.Fatalf("valid policy rejected: %v", errs)
	}
}

func TestRejectMinAckBelowOne(t *testing.T) {
	p := validPolicy()
	p.Local.MinAckNearbyReplicas = 0
	if len(ValidateReplicationPolicy(p)) == 0 {
		t.Fatal("minAck < 1 in local-cache mode must be rejected")
	}
}

func TestRejectRequireLocalGeoWithoutBoundary(t *testing.T) {
	p := validPolicy()
	p.Local.Geo = "require-local-geo"
	errs := ValidateReplicationPolicy(p)
	if len(errs) == 0 {
		t.Fatal("require-local-geo without a compliance boundary must be rejected")
	}
	// with a boundary it passes
	p.Local.ComplianceBoundary = &dbv1alpha1.ComplianceBoundary{Label: "topology.kubernetes.io/region", Value: "eu"}
	if len(ValidateReplicationPolicy(p)) != 0 {
		t.Fatal("require-local-geo with a compliance boundary should be accepted")
	}
}

func TestRejectTargetBelowMinAck(t *testing.T) {
	p := validPolicy()
	p.Local.TargetNearbyReplicas = 1
	p.Local.MinAckNearbyReplicas = 2
	if len(ValidateReplicationPolicy(p)) == 0 {
		t.Fatal("target < minAck must be rejected")
	}
}

func TestRejectGlobalWithoutTLS(t *testing.T) {
	p := validPolicy()
	p.Global = dbv1alpha1.GlobalReplicationSpec{Enabled: true}
	if len(ValidateReplicationPolicy(p)) == 0 {
		t.Fatal("global.enabled without TLS must be rejected")
	}
}

func TestRejectUnknownConflictPolicy(t *testing.T) {
	p := validPolicy()
	p.Conflict.Policy = "crdt-or-set"
	if len(ValidateReplicationPolicy(p)) == 0 {
		t.Fatal("a deferred/unknown conflict policy must be rejected in v1")
	}
}

func TestVectorIndexValidation(t *testing.T) {
	if len(ValidateVectorIndex(&dbv1alpha1.VectorIndexSpec{Dimensions: 0}, nil)) == 0 {
		t.Fatal("dimensions 0 must be rejected")
	}
	exists := func(name string) bool { return name == "prod" }
	if len(ValidateVectorIndex(&dbv1alpha1.VectorIndexSpec{Dimensions: 8, ClusterRef: "missing"}, exists)) == 0 {
		t.Fatal("missing clusterRef must be rejected")
	}
	if len(ValidateVectorIndex(&dbv1alpha1.VectorIndexSpec{Dimensions: 8, ClusterRef: "prod"}, exists)) != 0 {
		t.Fatal("valid vector index should be accepted")
	}
}
