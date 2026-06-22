package controller

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dbv1alpha1 "github.com/cwire/wavespan/operator/api/v1alpha1"
)

// WaveSpanClusterReconciler reconciles a WaveSpanCluster into its Kubernetes resources (design/09).
type WaveSpanClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=db.wavespan.io,resources=wavespanclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=db.wavespan.io,resources=wavespanclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;deployments,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch

// Reconcile ensures the cluster's resources exist and updates status. PVCs are never deleted by the
// operator (design/09 v1 "Scale down" — retain on scale-down).
func (r *WaveSpanClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var c dbv1alpha1.WaveSpanCluster
	if err := r.Get(ctx, req.NamespacedName, &c); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	objs := []client.Object{
		BuildConfigMap(&c),
		BuildHeadlessService(&c),
		BuildStatefulSet(&c),
		BuildPodDisruptionBudget(&c),
	}
	if c.Spec.Gateway.Enabled {
		objs = append(objs, BuildGatewayDeployment(&c), BuildGatewayService(&c))
	}
	for _, obj := range objs {
		if err := r.ensure(ctx, &c, obj); err != nil {
			return ctrl.Result{}, err
		}
	}

	var ss appsv1.StatefulSet
	ssPtr := &ss
	if err := r.Get(ctx, types.NamespacedName{Name: StatefulSetName(&c), Namespace: c.Namespace}, &ss); err != nil {
		ssPtr = nil
	}
	c.Status = deriveStatus(&c, ssPtr)
	if err := r.Status().Update(ctx, &c); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// ensure creates the object if absent, else updates it; the cluster owns it (GC on delete).
func (r *WaveSpanClusterReconciler) ensure(ctx context.Context, owner *dbv1alpha1.WaveSpanCluster, obj client.Object) error {
	if err := controllerutil.SetControllerReference(owner, obj, r.Scheme); err != nil {
		return err
	}
	existing, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil
	}
	err := r.Get(ctx, client.ObjectKeyFromObject(obj), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, obj)
	}
	if err != nil {
		return err
	}
	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.Update(ctx, obj)
}

// SetupWithManager registers the controller and its owned resources.
func (r *WaveSpanClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dbv1alpha1.WaveSpanCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
