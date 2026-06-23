// Package controller reconciles WaveSpanCluster resources into Kubernetes objects (design/09). The
// resource builders here are pure functions (spec -> object), unit-tested without a live cluster.
package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
)

// Node ports (design/09). The data port serves KV + Cypher + Vector + internal replication.
const (
	gossipPort = 7700
	dataPort   = 7800
	adminPort  = 7900
)

// StatefulSetName is the data StatefulSet name for a cluster.
func StatefulSetName(c *dbv1alpha1.WaveSpanCluster) string { return c.Name }

// HeadlessServiceName is the peer-discovery headless Service name for a cluster.
func HeadlessServiceName(c *dbv1alpha1.WaveSpanCluster) string { return c.Name + "-peers" }

// GatewayName is the gateway Deployment/Service name for a cluster.
func GatewayName(c *dbv1alpha1.WaveSpanCluster) string { return c.Name + "-gateway" }

func dataLabels(c *dbv1alpha1.WaveSpanCluster) map[string]string {
	return map[string]string{"app.kubernetes.io/name": "wavespan", "app.kubernetes.io/instance": c.Name, "wavespan.io/role": "data"}
}

func zoneLabel(c *dbv1alpha1.WaveSpanCluster) string {
	if c.Spec.Topology.ZoneLabel != "" {
		return c.Spec.Topology.ZoneLabel
	}
	return "topology.kubernetes.io/zone"
}

// BuildStatefulSet renders the data StatefulSet (design/09 "Data StatefulSet").
func BuildStatefulSet(c *dbv1alpha1.WaveSpanCluster) *appsv1.StatefulSet {
	labels := dataLabels(c)
	storageQty := resource.MustParse(c.Spec.Storage.VolumeSize)
	var sc *string
	if c.Spec.Storage.StorageClassName != "" {
		sc = &c.Spec.Storage.StorageClassName
	}
	env := []corev1.EnvVar{
		{Name: "WAVESPAN_RUNTIME", Value: "kubernetes"},
		{Name: "WAVESPAN_CLUSTER_ID", Value: c.Spec.ClusterID},
		{Name: "WAVESPAN_STORAGE_PATH", Value: "/var/lib/wavespan"},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
		// the pod is reachable as <pod>.<headless-svc>; advertise that for gossip + replication.
		{Name: "WAVESPAN_MEMBER_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "WAVESPAN_ADVERTISE_HOST", Value: "$(POD_NAME)." + HeadlessServiceName(c)},
		{Name: "WAVESPAN_NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
	}
	if c.Spec.Security.InsecureDevMode {
		env = append(env, corev1.EnvVar{Name: "WAVESPAN_INSECURE_DEV_MODE", Value: "true"})
	}

	replicas := c.Spec.Replicas
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: StatefulSetName(c), Namespace: c.Namespace, Labels: labels},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: HeadlessServiceName(c),
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// Spot-heavy clusters: spread best-effort; runtime repair is authoritative (design/09).
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{MaxSkew: 1, TopologyKey: "kubernetes.io/hostname", WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: labels}},
						{MaxSkew: 2, TopologyKey: zoneLabel(c), WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: &metav1.LabelSelector{MatchLabels: labels}},
					},
					Containers: []corev1.Container{{
						Name:  "wavespan-node",
						Image: c.Spec.Image,
						Ports: []corev1.ContainerPort{
							{Name: "gossip", ContainerPort: gossipPort},
							{Name: "data", ContainerPort: dataPort},
							{Name: "admin", ContainerPort: adminPort},
						},
						Env:            env,
						VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/wavespan"}},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromInt32(adminPort)}}},
						LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(adminPort)}}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: sc,
					Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storageQty}},
				},
			}},
		},
	}
}
