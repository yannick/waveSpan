package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dbv1alpha1 "github.com/cwire/wavespan/operator/api/v1alpha1"
)

func newReconciler(t *testing.T, objs ...client.Object) (*WaveSpanClusterReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = dbv1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&dbv1alpha1.WaveSpanCluster{}).
		WithObjects(objs...).Build()
	return &WaveSpanClusterReconciler{Client: cl, Scheme: scheme}, cl
}

func reconcile(t *testing.T, r *WaveSpanClusterReconciler, c *dbv1alpha1.WaveSpanCluster) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: c.Name, Namespace: c.Namespace}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestReconcileCreatesStatefulSetAndServices(t *testing.T) {
	c := sampleCluster()
	r, cl := newReconciler(t, c)
	reconcile(t, r, c)

	var ss appsv1.StatefulSet
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, &ss); err != nil {
		t.Fatalf("StatefulSet not created: %v", err)
	}
	if *ss.Spec.Replicas != 3 || len(ss.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("StatefulSet wrong: replicas=%d vct=%d", *ss.Spec.Replicas, len(ss.Spec.VolumeClaimTemplates))
	}
	// owner reference set for GC
	if len(ss.OwnerReferences) == 0 || ss.OwnerReferences[0].Name != "demo" {
		t.Fatal("StatefulSet missing owner reference")
	}
	var svc corev1.Service
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "demo-peers", Namespace: "ns"}, &svc); err != nil {
		t.Fatalf("headless service not created: %v", err)
	}
	var pdb policyv1.PodDisruptionBudget
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "demo-data", Namespace: "ns"}, &pdb); err != nil {
		t.Fatalf("PDB not created: %v", err)
	}
}

func TestReconcileIsIdempotent(t *testing.T) {
	c := sampleCluster()
	r, _ := newReconciler(t, c)
	reconcile(t, r, c)
	reconcile(t, r, c) // second reconcile must not error (update path)
}

func TestScaleUpUpdatesStatefulSet(t *testing.T) {
	c := sampleCluster()
	r, cl := newReconciler(t, c)
	reconcile(t, r, c)

	// scale to 5
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, c); err != nil {
		t.Fatal(err)
	}
	c.Spec.Replicas = 5
	if err := cl.Update(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, c)

	var ss appsv1.StatefulSet
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, &ss)
	if *ss.Spec.Replicas != 5 {
		t.Fatalf("scale up not applied: replicas=%d", *ss.Spec.Replicas)
	}
}

func TestScaleDownRetainsPVC(t *testing.T) {
	c := sampleCluster()
	// a PVC left behind by a prior replica
	orphan := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-demo-2", Namespace: "ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}},
	}
	r, cl := newReconciler(t, c, orphan)
	reconcile(t, r, c)

	// scale down to 2
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, c)
	c.Spec.Replicas = 2
	_ = cl.Update(context.Background(), c)
	reconcile(t, r, c)

	// the orphaned PVC must still exist (v1 retains PVCs on scale-down)
	var pvc corev1.PersistentVolumeClaim
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "data-demo-2", Namespace: "ns"}, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatal("operator must NOT delete orphaned PVCs on scale-down (v1 rule)")
		}
		t.Fatal(err)
	}
}

func TestStatusReportsDesiredMembers(t *testing.T) {
	c := sampleCluster()
	r, cl := newReconciler(t, c)
	reconcile(t, r, c)
	var got dbv1alpha1.WaveSpanCluster
	_ = cl.Get(context.Background(), types.NamespacedName{Name: "demo", Namespace: "ns"}, &got)
	if got.Status.DesiredMembers != 3 {
		t.Fatalf("status.desiredMembers = %d, want 3", got.Status.DesiredMembers)
	}
}
