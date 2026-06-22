package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// --- KVNamespace ---

// KVNamespaceSpec configures a KV namespace (design/12).
type KVNamespaceSpec struct {
	ClusterRef               string `json:"clusterRef"`
	ReplicationPolicyRef     string `json:"replicationPolicyRef,omitempty"`
	HideExpiredOnRead        bool   `json:"hideExpiredOnRead,omitempty"`
	GlobalDurabilityRequired bool   `json:"globalDurabilityRequired,omitempty"`
}

// +kubebuilder:object:root=true

// KVNamespace declares a KV namespace.
type KVNamespace struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KVNamespaceSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// KVNamespaceList is a list of KVNamespaces.
type KVNamespaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KVNamespace `json:"items"`
}

// --- Graph ---

// GraphSpec declares a property graph (design/12).
type GraphSpec struct {
	ClusterRef           string `json:"clusterRef"`
	ReplicationPolicyRef string `json:"replicationPolicyRef,omitempty"`
}

// +kubebuilder:object:root=true

// Graph declares a property graph.
type Graph struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              GraphSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// GraphList is a list of Graphs.
type GraphList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Graph `json:"items"`
}

// --- VectorIndex ---

// ApproximateIndexSpec configures the ANN index (inert until M10 at runtime).
type ApproximateIndexSpec struct {
	Enabled         bool  `json:"enabled,omitempty"`
	M               int32 `json:"m,omitempty"`
	EfConstruction  int32 `json:"efConstruction,omitempty"`
	EfSearchDefault int32 `json:"efSearchDefault,omitempty"`
}

// ExactIndexSpec configures exact search.
type ExactIndexSpec struct {
	Enabled bool `json:"enabled,omitempty"`
}

// VectorIndexSpec configures a vector index (design/12).
type VectorIndexSpec struct {
	ClusterRef string `json:"clusterRef"`
	Collection string `json:"collection,omitempty"`
	Label      string `json:"label,omitempty"`
	Property   string `json:"property,omitempty"`
	// +kubebuilder:validation:Minimum=1
	Dimensions  int32                `json:"dimensions"`
	Dtype       string               `json:"dtype,omitempty"`
	Metric      string               `json:"metric,omitempty"`
	Exact       ExactIndexSpec       `json:"exact,omitempty"`
	Approximate ApproximateIndexSpec `json:"approximate,omitempty"`
	Visibility  string               `json:"visibility,omitempty"`
}

// +kubebuilder:object:root=true

// VectorIndex declares a vector index.
type VectorIndex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VectorIndexSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// VectorIndexList is a list of VectorIndexes.
type VectorIndexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VectorIndex `json:"items"`
}

// --- ClusterPeer ---

// ClusterPeerSpec declares a peer cluster for global replication (design/12).
type ClusterPeerSpec struct {
	ClusterID     string `json:"clusterId"`
	Geo           string `json:"geo,omitempty"`
	ReplEndpoint  string `json:"replEndpoint"`
	TLSSecretName string `json:"tlsSecretName,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPeer declares a peer cluster.
type ClusterPeer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ClusterPeerSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPeerList is a list of ClusterPeers.
type ClusterPeerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPeer `json:"items"`
}

// --- Backup / Restore ---

// WaveSpanBackupSpec requests a backup (design/12; wired to wavesdb object-store in M12).
type WaveSpanBackupSpec struct {
	ClusterRef  string `json:"clusterRef"`
	Destination string `json:"destination"`
}

// +kubebuilder:object:root=true

// WaveSpanBackup requests a cluster backup.
type WaveSpanBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WaveSpanBackupSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// WaveSpanBackupList is a list of WaveSpanBackups.
type WaveSpanBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WaveSpanBackup `json:"items"`
}

// WaveSpanRestoreSpec requests a restore.
type WaveSpanRestoreSpec struct {
	ClusterRef string `json:"clusterRef"`
	Source     string `json:"source"`
}

// +kubebuilder:object:root=true

// WaveSpanRestore requests a cluster restore.
type WaveSpanRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              WaveSpanRestoreSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// WaveSpanRestoreList is a list of WaveSpanRestores.
type WaveSpanRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WaveSpanRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(
		&KVNamespace{}, &KVNamespaceList{},
		&Graph{}, &GraphList{},
		&VectorIndex{}, &VectorIndexList{},
		&ClusterPeer{}, &ClusterPeerList{},
		&WaveSpanBackup{}, &WaveSpanBackupList{},
		&WaveSpanRestore{}, &WaveSpanRestoreList{},
	)
}
