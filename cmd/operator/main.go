package main

import (
	"flag"
	"os"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/controller"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"helm.sh/helm/v3/pkg/chartutil"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var scheme = apiruntime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(operatorv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var planOnly bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager.")
	flag.BoolVar(&planOnly, "plan-only", false, "Only serialize installation plans; never apply workloads.")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zap.Options{Development: false})))

	config := ctrl.GetConfigOrDie()
	config.Timeout = 30 * time.Second
	mgr, err := ctrl.NewManager(config, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "kube-ovn-operator.kubeovn.io",
	})
	if err != nil {
		os.Exit(1)
	}
	reconciler := &controller.KubeOVNInstallationReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if !planOnly {
		directClient, err := client.New(config, client.Options{Scheme: scheme})
		if err != nil {
			ctrl.Log.Error(err, "create lease client")
			os.Exit(1)
		}
		reconciler.Leases = lease.New(directClient)
		podExecutor, err := health.NewPodExecutor(config)
		if err != nil {
			ctrl.Log.Error(err, "create OVN health executor")
			os.Exit(1)
		}
		reconciler.OVN = &health.OVNObserver{Reader: mgr.GetAPIReader(), Runner: podExecutor}
		reconciler.Runtime = &health.RuntimeObserver{Reader: mgr.GetAPIReader(), Runner: podExecutor}
		reconciler.Network = &health.NetworkObserver{Reader: mgr.GetAPIReader(), Runner: podExecutor}
		dynamicClient, err := dynamic.NewForConfig(config)
		if err != nil {
			ctrl.Log.Error(err, "create workload client")
			os.Exit(1)
		}
		discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
		if err != nil {
			ctrl.Log.Error(err, "create discovery client")
			os.Exit(1)
		}
		version, err := discoveryClient.ServerVersion()
		if err != nil {
			ctrl.Log.Error(err, "discover Kubernetes version")
			os.Exit(1)
		}
		caps := chartutil.DefaultCapabilities.Copy()
		caps.KubeVersion = chartutil.KubeVersion{Version: version.GitVersion, Major: version.Major, Minor: version.Minor}
		reconciler.RendererFactory = func(installation *operatorv1alpha1.KubeOVNInstallation) chart.Renderer {
			return &chart.HelmRenderer{Name: installation.Name, Namespace: installation.Namespace, Reader: mgr.GetAPIReader(), Mapper: mgr.GetRESTMapper(), Capabilities: caps}
		}
		reconciler.WorkloadFactory = func(identity apply.InstallationIdentity) apply.ApplyClient {
			return &apply.Client{Dynamic: dynamicClient, Mapper: mgr.GetRESTMapper(), Identity: identity, BeforeWrite: lease.CheckAll}
		}
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.RecoveryRequestReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}
