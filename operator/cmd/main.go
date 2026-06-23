// Command manager runs the WaveSpan operator (design/09). It reconciles WaveSpanCluster resources
// into StatefulSets, Services, PDBs, and ConfigMaps.
package main

import (
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	dbv1alpha1 "github.com/yannick/wavespan/operator/api/v1alpha1"
	"github.com/yannick/wavespan/operator/internal/controller"
)

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fatal(err)
	}
	if err := dbv1alpha1.AddToScheme(scheme); err != nil {
		fatal(err)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme})
	if err != nil {
		fatal(err)
	}
	if err := (&controller.WaveSpanClusterReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		fatal(err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		fatal(err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		fatal(err)
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	ctrl.Log.Error(err, "operator startup failed")
	os.Exit(1)
}
