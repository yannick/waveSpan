package controller

import (
	appsv1 "k8s.io/api/apps/v1"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
)

// deriveStatus computes the cluster status from the data StatefulSet status (design/09 "Status").
func deriveStatus(c *dbv1alpha1.WaveSpanCluster, ss *appsv1.StatefulSet) dbv1alpha1.WaveSpanClusterStatus {
	desired := c.Spec.Replicas
	ready := int32(0)
	if ss != nil {
		ready = ss.Status.ReadyReplicas
	}
	phase := dbv1alpha1.PhaseDegraded
	switch {
	case ss == nil:
		phase = dbv1alpha1.PhaseError
	case ready == desired && desired > 0:
		phase = dbv1alpha1.PhaseReady
	case ss.Status.Replicas != desired:
		phase = dbv1alpha1.PhaseScaling
	case ss.Status.UpdatedReplicas != ss.Status.Replicas:
		phase = dbv1alpha1.PhaseUpgrading
	}
	return dbv1alpha1.WaveSpanClusterStatus{
		ObservedGeneration: c.Generation,
		Phase:              phase,
		ReadyMembers:       ready,
		DesiredMembers:     desired,
	}
}
