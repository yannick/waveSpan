package controller

import (
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	dbv1alpha1 "github.com/cwire/wavespan/operator/api/v1alpha1"
)

// BuildPodDisruptionBudget renders the data PDB (design/09 "Disruption budget"). maxUnavailable
// defaults to 1 so voluntary disruptions never take more than one data pod at a time.
func BuildPodDisruptionBudget(c *dbv1alpha1.WaveSpanCluster) *policyv1.PodDisruptionBudget {
	maxUnavail := c.Spec.Disruption.MaxUnavailable
	if maxUnavail == 0 {
		maxUnavail = 1
	}
	mu := intstr.FromInt32(maxUnavail)
	labels := dataLabels(c)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: c.Name + "-data", Namespace: c.Namespace, Labels: labels},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &mu,
			Selector:       &metav1.LabelSelector{MatchLabels: labels},
		},
	}
}
