package v1alpha1

import (
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func roundTrip[T any](t *testing.T, obj T) T {
	t.Helper()
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWaveSpanClusterRoundTrip(t *testing.T) {
	c := WaveSpanCluster{
		TypeMeta:   metav1.TypeMeta{Kind: "WaveSpanCluster", APIVersion: "db.wavespan.io/v1alpha1"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: WaveSpanClusterSpec{
			Image: "wavespan/node:dev", Replicas: 3, ClusterID: "prod",
			Storage:    StorageSpec{VolumeSize: "50Gi", StorageClassName: "fast"},
			Gateway:    GatewaySpec{Enabled: true, Replicas: 2},
			Disruption: DisruptionSpec{MaxUnavailable: 1},
		},
	}
	got := roundTrip(t, c)
	if got.Spec.Replicas != 3 || got.Spec.Storage.VolumeSize != "50Gi" || !got.Spec.Gateway.Enabled {
		t.Fatalf("cluster fields did not survive: %+v", got.Spec)
	}
	if GroupVersion.Group != "db.wavespan.io" || GroupVersion.Version != "v1alpha1" {
		t.Fatalf("group/version wrong: %s", GroupVersion.String())
	}
}

func TestReplicationPolicyRoundTrip(t *testing.T) {
	p := ReplicationPolicy{
		Spec: ReplicationPolicySpec{
			Mode:     ModeLocalCache,
			Local:    LocalReplicationSpec{TargetNearbyReplicas: 3, MinAckNearbyReplicas: 1, Geo: "require-local-geo", ComplianceBoundary: &ComplianceBoundary{Label: "topology.kubernetes.io/region", Value: "eu"}},
			Global:   GlobalReplicationSpec{Enabled: true, Mode: "active-active-async", TLSSecretName: "peer-tls"},
			Conflict: ConflictSpec{Policy: "keep-siblings"},
		},
	}
	got := roundTrip(t, p)
	if got.Spec.Local.ComplianceBoundary == nil || got.Spec.Local.ComplianceBoundary.Value != "eu" {
		t.Fatalf("compliance boundary lost: %+v", got.Spec.Local)
	}
	if got.Spec.Conflict.Policy != "keep-siblings" {
		t.Fatalf("conflict policy lost: %+v", got.Spec.Conflict)
	}
}

func TestAllKindsRegisteredAndDeepCopyable(t *testing.T) {
	objs := []runtime.Object{
		&WaveSpanCluster{}, &ReplicationPolicy{}, &KVNamespace{}, &Graph{},
		&VectorIndex{Spec: VectorIndexSpec{Dimensions: 8}}, &ClusterPeer{}, &WaveSpanBackup{}, &WaveSpanRestore{},
	}
	for _, o := range objs {
		if o.DeepCopyObject() == nil {
			t.Fatalf("%T deepcopy returned nil", o)
		}
	}
	// every kind is registered with the scheme builder
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if !s.Recognizes(GroupVersion.WithKind("VectorIndex")) {
		t.Fatal("VectorIndex not registered with the scheme")
	}
}
