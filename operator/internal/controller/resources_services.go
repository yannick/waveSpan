package controller

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dbv1alpha1 "github.com/cwire/wavespan/operator/api/v1alpha1"
)

// BuildHeadlessService renders the ClusterIP:None peer-discovery Service for the StatefulSet
// (design/09 "Peer discovery").
func BuildHeadlessService(c *dbv1alpha1.WaveSpanCluster) *corev1.Service {
	labels := dataLabels(c)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: HeadlessServiceName(c), Namespace: c.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			Selector:                 labels,
			PublishNotReadyAddresses: true, // gossip must reach not-yet-ready peers during form-up
			Ports: []corev1.ServicePort{
				{Name: "gossip", Port: gossipPort},
				{Name: "data", Port: dataPort},
			},
		},
	}
}

func gatewayLabels(c *dbv1alpha1.WaveSpanCluster) map[string]string {
	return map[string]string{"app.kubernetes.io/name": "wavespan", "app.kubernetes.io/instance": c.Name, "wavespan.io/role": "gateway"}
}

// BuildGatewayService renders the gateway Service (only when the gateway is enabled).
func BuildGatewayService(c *dbv1alpha1.WaveSpanCluster) *corev1.Service {
	labels := gatewayLabels(c)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: GatewayName(c), Namespace: c.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports:    []corev1.ServicePort{{Name: "data", Port: dataPort}},
		},
	}
}

// BuildGatewayDeployment renders the stateless gateway Deployment.
func BuildGatewayDeployment(c *dbv1alpha1.WaveSpanCluster) *appsv1.Deployment {
	labels := gatewayLabels(c)
	replicas := c.Spec.Gateway.Replicas
	if replicas == 0 {
		replicas = 2
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: GatewayName(c), Namespace: c.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "wavespan-gateway",
					Image: c.Spec.Image,
					Ports: []corev1.ContainerPort{{Name: "data", ContainerPort: dataPort}},
				}}},
			},
		},
	}
}
