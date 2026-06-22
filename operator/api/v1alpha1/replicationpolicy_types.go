package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// LocalReplicationSpec configures nearby durable replication (design/12).
type LocalReplicationSpec struct {
	// +kubebuilder:validation:Minimum=0
	TargetNearbyReplicas int32 `json:"targetNearbyReplicas"`
	// +kubebuilder:validation:Minimum=0
	MinAckNearbyReplicas int32 `json:"minAckNearbyReplicas"`
	RequireDistinctNodes bool  `json:"requireDistinctNodes,omitempty"`
	// Geo placement: prefer-local-geo | require-local-geo | latency-only.
	Geo string `json:"geo,omitempty"`
	// ComplianceBoundary is the node label/value pair that bounds require-local-geo placement.
	ComplianceBoundary          *ComplianceBoundary `json:"complianceBoundary,omitempty"`
	AllowSpilloverForDurability bool                `json:"allowSpilloverForDurability,omitempty"`
}

// ComplianceBoundary is a node label/value that bounds geo-restricted placement.
type ComplianceBoundary struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// GlobalReplicationSpec configures active-active replication (design/12).
type GlobalReplicationSpec struct {
	Enabled       bool     `json:"enabled,omitempty"`
	Mode          string   `json:"mode,omitempty"`
	PeerRefs      []string `json:"peerRefs,omitempty"`
	TLSSecretName string   `json:"tlsSecretName,omitempty"`
}

// ConflictSpec selects a conflict policy (v1: hlc-last-write-wins | keep-siblings).
type ConflictSpec struct {
	Policy string `json:"policy,omitempty"`
}

// WaveSpanClusterMode is the cluster operating mode.
type WaveSpanClusterMode string

// Cluster modes.
const (
	ModeLocalCache WaveSpanClusterMode = "local-cache"
)

// ReplicationPolicySpec is the desired replication policy (design/12).
type ReplicationPolicySpec struct {
	// Mode is the operating mode (local-cache in v1).
	Mode     WaveSpanClusterMode   `json:"mode,omitempty"`
	Local    LocalReplicationSpec  `json:"local"`
	Global   GlobalReplicationSpec `json:"global,omitempty"`
	Conflict ConflictSpec          `json:"conflict,omitempty"`
}

// ReplicationPolicyStatus is the observed state.
type ReplicationPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ReplicationPolicy is a reusable replication policy referenced by clusters/namespaces.
type ReplicationPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ReplicationPolicySpec   `json:"spec,omitempty"`
	Status            ReplicationPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ReplicationPolicyList is a list of ReplicationPolicies.
type ReplicationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ReplicationPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ReplicationPolicy{}, &ReplicationPolicyList{})
}
