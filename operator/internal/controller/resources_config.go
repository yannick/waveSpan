package controller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
)

// ConfigMapName derives the ConfigMap name for a cluster.
func ConfigMapName(c *dbv1alpha1.WaveSpanCluster) string { return c.Name + "-config" }

// BuildConfigMap renders the node config as a ConfigMap (design/09). The data pod also reads
// WAVESPAN_* env from the StatefulSet; this captures the declarative, non-per-pod config.
func BuildConfigMap(c *dbv1alpha1.WaveSpanCluster) *corev1.ConfigMap {
	cfg := fmt.Sprintf("clusterId: %s\nstorage:\n  path: /var/lib/wavespan\n  engine: wavesdb\nmembership:\n  runtime: kubernetes\nadmin:\n  listen: \":%d\"\n",
		c.Spec.ClusterID, adminPort)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName(c), Namespace: c.Namespace, Labels: dataLabels(c)},
		Data:       map[string]string{"node.yaml": cfg},
	}
}
