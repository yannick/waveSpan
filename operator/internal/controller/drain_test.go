package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDrainAnnotationProtocol(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-2"}}

	if CanTerminate(pod, false) {
		t.Fatal("a pod with no drain request must not be terminable")
	}
	SetDrainRequested(pod)
	if !IsDrainRequested(pod) {
		t.Fatal("drain request annotation not set")
	}
	// drain requested but not yet drained, before timeout -> must wait
	if CanTerminate(pod, false) {
		t.Fatal("must wait for the drain readiness gate before terminating")
	}
	// the node marks itself drained
	pod.Annotations[AnnotationDrained] = "true"
	if !CanTerminate(pod, false) {
		t.Fatal("a drained pod should be terminable")
	}
}

func TestDrainTimeoutFallsBackToRepair(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-2"}}
	SetDrainRequested(pod)
	// not drained, but the deadline elapsed -> terminate anyway and let repair restore replicas
	if !CanTerminate(pod, true) {
		t.Fatal("after the drain timeout, the pod should be terminable (repair is authoritative)")
	}
}
