// Package webhook validates WaveSpan CRD policies (design/12 "Operator validation rules"). The
// validation logic here is pure (spec -> errors) so it is unit-tested without an apiserver; the
// admission wrappers compose these checks with cross-resource lookups.
package webhook

import (
	"fmt"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
)

// ValidateReplicationPolicy returns all violations of the design/12 rules for a ReplicationPolicy.
// clusterExists/peerHasTLS are optional cross-resource checks (nil skips them).
func ValidateReplicationPolicy(p *dbv1alpha1.ReplicationPolicySpec) []error {
	var errs []error
	local := p.Local

	// local-cache mode requires origin+1 (minAck >= 1).
	mode := p.Mode
	if mode == "" {
		mode = dbv1alpha1.ModeLocalCache
	}
	if mode == dbv1alpha1.ModeLocalCache && local.MinAckNearbyReplicas < 1 {
		errs = append(errs, fmt.Errorf("local.minAckNearbyReplicas must be >= 1 in %s mode (origin+1)", dbv1alpha1.ModeLocalCache))
	}

	// require-local-geo must name a compliance boundary that placement can enforce.
	if local.Geo == "require-local-geo" && (local.ComplianceBoundary == nil || local.ComplianceBoundary.Label == "" || local.ComplianceBoundary.Value == "") {
		errs = append(errs, fmt.Errorf("local.geo=require-local-geo requires local.complianceBoundary (label and value)"))
	}

	// target must be at least the ack threshold.
	if local.TargetNearbyReplicas < local.MinAckNearbyReplicas {
		errs = append(errs, fmt.Errorf("local.targetNearbyReplicas (%d) must be >= local.minAckNearbyReplicas (%d)", local.TargetNearbyReplicas, local.MinAckNearbyReplicas))
	}

	// global active-active requires TLS configuration.
	if p.Global.Enabled && p.Global.TLSSecretName == "" {
		errs = append(errs, fmt.Errorf("global.enabled requires global.tlsSecretName"))
	}

	// conflict policy must be a v1 policy.
	switch p.Conflict.Policy {
	case "", "hlc-last-write-wins", "keep-siblings":
	default:
		errs = append(errs, fmt.Errorf("conflict.policy %q is unsupported in v1 (use hlc-last-write-wins or keep-siblings)", p.Conflict.Policy))
	}
	return errs
}

// ValidateVectorIndex returns violations for a VectorIndex. clusterExists is an optional
// cross-resource check (nil skips it).
func ValidateVectorIndex(s *dbv1alpha1.VectorIndexSpec, clusterExists func(name string) bool) []error {
	var errs []error
	if s.Dimensions <= 0 {
		errs = append(errs, fmt.Errorf("dimensions must be > 0"))
	}
	if clusterExists != nil && s.ClusterRef != "" && !clusterExists(s.ClusterRef) {
		errs = append(errs, fmt.Errorf("clusterRef %q does not exist", s.ClusterRef))
	}
	return errs
}

// ValidateGraph returns violations for a Graph (clusterRef must exist).
func ValidateGraph(s *dbv1alpha1.GraphSpec, clusterExists func(name string) bool) []error {
	var errs []error
	if clusterExists != nil && s.ClusterRef != "" && !clusterExists(s.ClusterRef) {
		errs = append(errs, fmt.Errorf("clusterRef %q does not exist", s.ClusterRef))
	}
	return errs
}
