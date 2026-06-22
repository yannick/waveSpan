package controller

import corev1 "k8s.io/api/core/v1"

// Drain annotations (design/09 "Rolling upgrades"). The operator requests a drain before replacing
// a pod; the pod marks itself drained once it has left gossip cleanly and flushed its log.
const (
	AnnotationDrainRequested = "wavespan.io/drain-requested"
	AnnotationDrained        = "wavespan.io/drained"
)

// SetDrainRequested annotates a pod to begin draining.
func SetDrainRequested(pod *corev1.Pod) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[AnnotationDrainRequested] = "true"
}

// IsDrainRequested reports whether a drain has been requested.
func IsDrainRequested(pod *corev1.Pod) bool {
	return pod.Annotations[AnnotationDrainRequested] == "true"
}

// IsDrained reports whether the pod has finished draining (set by the node's drain readiness gate).
func IsDrained(pod *corev1.Pod) bool {
	return pod.Annotations[AnnotationDrained] == "true"
}

// CanTerminate reports whether a pod may be terminated for a rolling update: either it has drained,
// or the drain deadline has elapsed (timeoutElapsed) so the cluster falls back to repair.
func CanTerminate(pod *corev1.Pod, timeoutElapsed bool) bool {
	if !IsDrainRequested(pod) {
		return false
	}
	return IsDrained(pod) || timeoutElapsed
}
