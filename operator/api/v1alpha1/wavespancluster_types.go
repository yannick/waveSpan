package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// StorageSpec configures the data PVCs (design/09 "Storage").
type StorageSpec struct {
	// VolumeSize is the size of each data PVC (e.g. "50Gi").
	// +kubebuilder:validation:MinLength=1
	VolumeSize string `json:"volumeSize"`
	// StorageClassName selects the PVC storage class.
	StorageClassName string `json:"storageClassName,omitempty"`
}

// GatewaySpec configures the optional stateless gateway (design/09).
type GatewaySpec struct {
	Enabled  bool  `json:"enabled,omitempty"`
	Replicas int32 `json:"replicas,omitempty"`
}

// DisruptionSpec configures the PodDisruptionBudget (design/09).
type DisruptionSpec struct {
	// +kubebuilder:validation:Minimum=0
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`
}

// SecuritySpec configures transport security.
type SecuritySpec struct {
	TLSSecretName   string `json:"tlsSecretName,omitempty"`
	InsecureDevMode bool   `json:"insecureDevMode,omitempty"`
}

// TopologySpec names the node labels used for topology spread (design/09).
type TopologySpec struct {
	ZoneLabel   string `json:"zoneLabel,omitempty"`
	RegionLabel string `json:"regionLabel,omitempty"`
}

// WaveSpanClusterSpec is the desired state of a WaveSpan cluster (design/12).
type WaveSpanClusterSpec struct {
	// Image is the wavespan-node container image.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// Replicas is the number of data pods.
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`
	// ClusterID is the logical cluster id.
	// +kubebuilder:validation:MinLength=1
	ClusterID string `json:"clusterId"`

	Storage    StorageSpec    `json:"storage"`
	Topology   TopologySpec   `json:"topology,omitempty"`
	Gateway    GatewaySpec    `json:"gateway,omitempty"`
	Disruption DisruptionSpec `json:"disruption,omitempty"`
	Security   SecuritySpec   `json:"security,omitempty"`

	// ReplicationPolicyRef references a ReplicationPolicy in the same namespace.
	ReplicationPolicyRef string `json:"replicationPolicyRef,omitempty"`
}

// WaveSpanClusterPhase is the high-level lifecycle phase.
type WaveSpanClusterPhase string

// Cluster phases (design/09 "Status").
const (
	PhaseReady     WaveSpanClusterPhase = "Ready"
	PhaseScaling   WaveSpanClusterPhase = "Scaling"
	PhaseUpgrading WaveSpanClusterPhase = "Upgrading"
	PhaseDegraded  WaveSpanClusterPhase = "Degraded"
	PhaseError     WaveSpanClusterPhase = "Error"
)

// WaveSpanClusterStatus is the observed state.
type WaveSpanClusterStatus struct {
	ObservedGeneration int64                `json:"observedGeneration,omitempty"`
	Phase              WaveSpanClusterPhase `json:"phase,omitempty"`
	ReadyMembers       int32                `json:"readyMembers,omitempty"`
	DesiredMembers     int32                `json:"desiredMembers,omitempty"`
	Conditions         []metav1.Condition   `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyMembers`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredMembers`

// WaveSpanCluster deploys and reconciles a WaveSpan data cluster.
type WaveSpanCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WaveSpanClusterSpec   `json:"spec,omitempty"`
	Status WaveSpanClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// WaveSpanClusterList is a list of WaveSpanClusters.
type WaveSpanClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WaveSpanCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WaveSpanCluster{}, &WaveSpanClusterList{})
}
