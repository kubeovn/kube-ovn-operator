package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/controller"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
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
	var backupDir string
	var backupMaxAge time.Duration
	var backupMaxBytes int64
	var fenceEndpointTemplate string
	var fenceCAFile string
	var fenceClientCertFile string
	var fenceClientKeyFile string
	var fenceAgentImage string
	var fenceAgentServerTLSSecret string
	var fenceAgentClientCASecret string
	var fenceAgentPort int
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager.")
	flag.BoolVar(&planOnly, "plan-only", false, "Only serialize installation plans; never apply workloads.")
	flag.StringVar(&backupDir, "backup-dir", "", "Existing operator-owned directory for lifecycle backups; required when lifecycle execution is used.")
	flag.DurationVar(&backupMaxAge, "backup-max-age", 24*time.Hour, "Maximum age accepted for lifecycle backups.")
	flag.Int64Var(&backupMaxBytes, "backup-max-bytes", 1<<30, "Maximum size accepted for one lifecycle backup.")
	flag.StringVar(&fenceEndpointTemplate, "fence-endpoint-template", "", "Trusted HTTPS URL template for atomic membership fencing and restore; use {pod}, {namespace}, {container} path placeholders or {node} for the live Pod/Node IP.")
	flag.StringVar(&fenceCAFile, "fence-ca-file", "", "CA certificate for the remote fence service; required with --fence-endpoint-template.")
	flag.StringVar(&fenceClientCertFile, "fence-client-cert-file", "", "Client certificate for the remote fence service; required with --fence-endpoint-template.")
	flag.StringVar(&fenceClientKeyFile, "fence-client-key-file", "", "Client private key for the remote fence service; required with --fence-endpoint-template.")
	flag.StringVar(&fenceAgentImage, "fence-agent-image", "", "Digest-pinned fence-agent image to inject into rendered ovn-central workloads; empty disables sidecar injection.")
	flag.StringVar(&fenceAgentServerTLSSecret, "fence-agent-server-tls-secret", "", "Secret name in each installation namespace containing fence-agent tls.crt and tls.key.")
	flag.StringVar(&fenceAgentClientCASecret, "fence-agent-client-ca-secret", "", "Secret name in each installation namespace containing the operator client CA as ca.crt.")
	flag.IntVar(&fenceAgentPort, "fence-agent-port", 8443, "HTTPS port used by injected fence-agent sidecars.")
	flag.Parse()
	if strings.TrimSpace(fenceAgentImage) != "" && strings.TrimSpace(fenceEndpointTemplate) == "" {
		_, _ = fmt.Fprintln(os.Stderr, "invalid fence-agent configuration: --fence-agent-image requires --fence-endpoint-template")
		os.Exit(2)
	}
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
	reconciler := &controller.KubeOVNInstallationReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Scheme: mgr.GetScheme(), Recorder: mgr.GetEventRecorderFor("kube-ovn-operator")}
	var recoveryExecutor controller.RecoveryExecutor
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
		var commandRunner health.CommandRunner = podExecutor
		var backupRunner health.BackupRunner = podExecutor
		var fenceClient *health.FenceHTTPClient
		if strings.TrimSpace(fenceEndpointTemplate) != "" {
			fenceClient, err = health.NewFenceHTTPClientWithTLS(fenceEndpointTemplate, fenceCAFile, fenceClientCertFile, fenceClientKeyFile)
			if err != nil {
				ctrl.Log.Error(err, "create atomic membership fence client")
				os.Exit(1)
			}
			fencedExecutor, err := health.NewFencedPodExecutorWithRemote(config, fenceClient)
			if err != nil {
				ctrl.Log.Error(err, "create atomic membership fence executor")
				os.Exit(1)
			}
			commandRunner, backupRunner = fencedExecutor, fencedExecutor
		}
		topologyProvider := &topology.Kubernetes{Client: directClient, Runner: commandRunner}
		reconciler.HCP = &hcp.KubernetesPeerProvider{Reader: directClient}
		lifecycleManager := &lifecycle.Manager{Client: directClient, Runner: backupRunner, MaxAge: backupMaxAge, MaxBytes: backupMaxBytes}
		if fenceClient != nil {
			lifecycleManager.RestoreFence = &health.RemoteRestoreFence{Remote: fenceClient}
		}
		reconciler.Topology = topologyProvider
		reconciler.Lifecycle = lifecycleManager
		reconciler.Lifecycle.SetStoreFactory(func(installation *operatorv1alpha1.KubeOVNInstallation) (backup.Store, error) {
			return configuredBackupStore(backupDir, installation)
		})
		lifecycleRecoveryExecutor := &controller.LifecycleRecoveryExecutor{Client: directClient, Leases: reconciler.Leases, Topology: topologyProvider, Lifecycle: lifecycleManager}
		recoveryExecutor = lifecycleRecoveryExecutor
		reconciler.OVN = &health.OVNObserver{Reader: mgr.GetAPIReader(), Runner: commandRunner}
		reconciler.Runtime = &health.RuntimeObserver{Reader: mgr.GetAPIReader(), Runner: commandRunner}
		reconciler.Network = &health.NetworkObserver{Reader: mgr.GetAPIReader(), Runner: commandRunner}
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
		var fenceAgent *chart.FenceAgentConfig
		if strings.TrimSpace(fenceAgentImage) != "" {
			fenceAgent = &chart.FenceAgentConfig{Image: fenceAgentImage, ServerTLSSecret: fenceAgentServerTLSSecret, ClientCASecret: fenceAgentClientCASecret, Port: int32(fenceAgentPort)}
		}
		reconciler.RendererFactory = func(installation *operatorv1alpha1.KubeOVNInstallation) chart.Renderer {
			return &chart.HelmRenderer{Name: installation.Name, Namespace: installation.Namespace, Reader: mgr.GetAPIReader(), Mapper: mgr.GetRESTMapper(), Capabilities: caps, FenceAgent: fenceAgent}
		}
		reconciler.WorkloadFactory = func(identity apply.InstallationIdentity) apply.ApplyClient {
			return &apply.Client{Dynamic: dynamicClient, Mapper: mgr.GetRESTMapper(), Identity: identity, BeforeWrite: lease.CheckAll}
		}
		reconciler.Rollback = &controller.KubernetesRollbackExecutor{
			Client:          directClient,
			Lifecycle:       lifecycleManager,
			Topology:        topologyProvider,
			RendererFactory: reconciler.RendererFactory,
			WorkloadFactory: reconciler.WorkloadFactory,
			OVN:             reconciler.OVN,
			Runtime:         reconciler.Runtime,
			ApprovalFactory: func(installation *operatorv1alpha1.KubeOVNInstallation) (approval.RecoveryApprover, error) {
				return approval.NewRecovery(directClient, installation)
			},
		}
		lifecycleRecoveryExecutor.Rollback = reconciler.Rollback
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.RecoveryRequestReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Executor: recoveryExecutor}).SetupWithManager(mgr); err != nil {
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

func configuredBackupStore(backupDir string, installation *operatorv1alpha1.KubeOVNInstallation) (backup.Store, error) {
	if installation == nil {
		return nil, fmt.Errorf("backup store requires an installation")
	}
	backupDir = strings.TrimSpace(backupDir)
	destination := strings.TrimSpace(installation.Spec.Upgrade.Backup.Destination)
	if destination == "" {
		if backupDir == "" {
			return nil, fmt.Errorf("--backup-dir is required when lifecycle backup execution is used")
		}
		return backup.OpenFileStore(backupDir)
	}
	if strings.HasPrefix(destination, "s3://") {
		credentials, err := backup.CredentialsFromEnvironment()
		if err != nil {
			return nil, err
		}
		return backup.NewS3Store(destination, credentials, nil)
	}
	if backupDir == "" {
		return nil, fmt.Errorf("--backup-dir is required for a file backup destination")
	}
	path, err := backup.FileDestination(destination)
	if err != nil {
		return nil, err
	}
	configured, err := filepath.Abs(backupDir)
	if err != nil {
		return nil, fmt.Errorf("resolve --backup-dir: %w", err)
	}
	if filepath.Clean(configured) != path {
		return nil, fmt.Errorf("backup destination %q does not match the operator --backup-dir %q", destination, backupDir)
	}
	return backup.OpenFileStore(backupDir)
}
