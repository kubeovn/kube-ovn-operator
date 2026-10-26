package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	"github.com/kubeovn/kube-ovn-operator/internal/upgrade"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	planConfigMapPrefix = "kube-ovn-operator-plan-"
)

var (
	reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubeovn_operator_reconcile_total",
		Help: "Total number of KubeOVNInstallation reconciliations by outcome.",
	}, []string{"outcome"})
	reconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "kubeovn_operator_reconcile_duration_seconds",
		Help: "Duration of KubeOVNInstallation reconciliations in seconds.",
	}, []string{"outcome"})
	installationReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubeovn_operator_installation_ready",
		Help: "Whether a named KubeOVNInstallation passed all configured verification gates.",
	}, []string{"namespace", "name"})
	installationCondition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kubeovn_operator_installation_condition",
		Help: "Latest KubeOVNInstallation condition state: 1 true, 0 false, -1 unknown.",
	}, []string{"namespace", "name", "condition"})
)

func init() {
	crmetrics.Registry.MustRegister(reconcileTotal, reconcileDuration, installationReady, installationCondition)
}

type KubeOVNInstallationReconciler struct {
	client.Client
	// APIReader is an uncached reader for security-sensitive identity pins such
	// as HCP TLS Secret UID/resourceVersion. Tests and plan-only callers may
	// leave it nil, in which case the controller client is used.
	APIReader       client.Reader
	Scheme          *apiruntime.Scheme
	Clock           func() time.Time
	Renderer        chart.Renderer
	RendererFactory func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer
	Workload        apply.ApplyClient
	Leases          lease.Runner
	OVN             health.OVNProvider
	HCP             hcp.PeerProvider
	Runtime         health.RuntimeProvider
	Network         health.NetworkProvider
	Topology        topology.Provider
	Lifecycle       lifecycleCoordinator
	Rollback        RollbackStepExecutor
	Recorder        record.EventRecorder
	WorkloadFactory func(apply.InstallationIdentity) apply.ApplyClient
	// ScheduledBackup is an injectable checkpoint seam. Production uses the
	// lifecycle manager; tests can provide a verified pair capture without
	// constructing Pod and object-store adapters.
	ScheduledBackup func(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*operatorv1alpha1.BackupStatus, error)
}

// lifecycleCoordinator is the controller boundary for durable central
// operations. Keeping it narrow makes the admission path testable without
// replacing the Kubernetes identity and storage checks performed by the
// production Manager.
type lifecycleCoordinator interface {
	Reader() client.Reader
	SetStoreFactory(func(*operatorv1alpha1.KubeOVNInstallation) (backup.Store, error))
	Begin(context.Context, *operatorv1alpha1.KubeOVNInstallation, lifecycle.Request) (lifecycle.Reference, error)
	Resume(context.Context, *operatorv1alpha1.KubeOVNInstallation, *operatorv1alpha1.CentralOperationReference) (*lifecycle.Recovery, error)
	ProtectedBackupRefs(context.Context, *operatorv1alpha1.KubeOVNInstallation, *operatorv1alpha1.CentralOperationReference) ([]string, error)
	CleanupBackups(context.Context, *operatorv1alpha1.KubeOVNInstallation, backup.RetentionPolicy, []string, time.Time) (backup.RetentionResult, error)
	CaptureBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*operatorv1alpha1.BackupStatus, error)
	VerifyBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *operatorv1alpha1.BackupStatus) error
	RestoreBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *operatorv1alpha1.BackupStatus) error
}

func (r *KubeOVNInstallationReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		For(&operatorv1alpha1.KubeOVNInstallation{}).
		Owns(&corev1.ConfigMap{}).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.mapInstallationObject)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.mapInstallationObject)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.mapInstallationObject)).
		Watches(&corev1.PersistentVolumeClaim{}, handler.EnqueueRequestsFromMapFunc(r.mapInstallationObject)).
		Watches(&coordinationv1.Lease{}, handler.EnqueueRequestsFromMapFunc(r.mapInstallationObject)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToInstallations)).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.mapNodeToInstallations)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

// mapInstallationObject wakes an installation when one of its labeled
// workloads, storage claims or operation Leases changes. The label and owner
// checks keep unrelated cluster activity out of the queue; a stale UID label
// cannot redirect an event to a recreated installation with the same name.
func (r *KubeOVNInstallationReconciler) mapInstallationObject(ctx context.Context, object client.Object) []reconcile.Request {
	if r == nil || r.Client == nil || object == nil || object.GetNamespace() == "" {
		return nil
	}
	labels := object.GetLabels()
	name, namespace := labels[apply.InstallationLabel], labels[apply.InstallationNamespaceLabel]
	if name == "" || namespace != object.GetNamespace() {
		for _, owner := range object.GetOwnerReferences() {
			if owner.APIVersion != operatorv1alpha1.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" || owner.Name == "" {
				continue
			}
			name = owner.Name
			namespace = object.GetNamespace()
			break
		}
	}
	if name == "" || namespace != object.GetNamespace() {
		return nil
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := r.Get(ctx, key, installation); err != nil {
		return nil
	}
	if uid := labels[apply.InstallationUIDLabel]; uid != "" && uid != string(installation.UID) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: key}}
}

// mapSecretToInstallations watches every Secret referenced by an installation's
// control-plane contract. Secret contents are never read by this mapper; a
// rotation merely causes the provider or member lifecycle contract to be
// re-evaluated against the new object identity. Member TLS rotation must wake
// the controller as well, otherwise a replaced certificate could remain
// outside the reconcile queue until the periodic resync.
func (r *KubeOVNInstallationReconciler) mapSecretToInstallations(ctx context.Context, object client.Object) []reconcile.Request {
	secret, ok := object.(*corev1.Secret)
	if !ok || secret == nil || secret.Namespace == "" || r == nil || r.Client == nil {
		return nil
	}
	installations := new(operatorv1alpha1.KubeOVNInstallationList)
	if err := r.List(ctx, installations, client.InNamespace(secret.Namespace)); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(installations.Items))
	for index := range installations.Items {
		installation := &installations.Items[index]
		if !installationReferencesSecret(installation, secret) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name}})
	}
	return requests
}

func installationReferencesSecret(installation *operatorv1alpha1.KubeOVNInstallation, secret *corev1.Secret) bool {
	if installation == nil || secret == nil || installation.Namespace != secret.Namespace {
		return false
	}
	if installation.Spec.HCP.Enabled && installation.Spec.HCP.TLSSecretRef != nil && installation.Spec.HCP.TLSSecretRef.Name == secret.Name {
		return true
	}
	for _, member := range installation.Spec.Central.MemberRequests {
		if member.TLSSecretRef != nil && member.TLSSecretRef.Name == secret.Name {
			return true
		}
	}
	return false
}

// mapNodeToInstallations wakes installations whose central placement, data
// plane placement, retained observed workload identity or explicit member
// request refers to the changed Node. Node status and schedulability changes
// are safety inputs for topology and runtime health, so waiting for the
// periodic reconcile would leave a stale decision window during a drain or
// replacement. Retained identities are checked before current selectors so a
// label removal cannot hide a node that is already hosting a managed workload.
func (r *KubeOVNInstallationReconciler) mapNodeToInstallations(ctx context.Context, object client.Object) []reconcile.Request {
	node, ok := object.(*corev1.Node)
	if !ok || node == nil || node.Name == "" || r.Client == nil {
		return nil
	}
	installations := new(operatorv1alpha1.KubeOVNInstallationList)
	if err := r.List(ctx, installations); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(installations.Items))
	for index := range installations.Items {
		installation := &installations.Items[index]
		if installationUsesNode(installation, node) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name}})
		}
	}
	slices.SortFunc(requests, func(a, b reconcile.Request) int {
		if value := strings.Compare(a.Namespace, b.Namespace); value != 0 {
			return value
		}
		return strings.Compare(a.Name, b.Name)
	})
	return requests
}

func installationUsesNode(installation *operatorv1alpha1.KubeOVNInstallation, node *corev1.Node) bool {
	if installation == nil || node == nil || installation.DeletionTimestamp != nil {
		return false
	}
	// Keep watching a node that hosted a previously observed workload even
	// after a label change makes it stop matching the desired placement. This
	// closes the event gap for cordon, drain, deletion and label/IP changes.
	for _, member := range installation.Status.Topology.Members {
		if member.Node == node.Name {
			return true
		}
	}
	for _, member := range installation.Status.Topology.Nodes {
		if member.Node == node.Name {
			return true
		}
	}
	for _, member := range installation.Spec.Central.MemberRequests {
		if member.NodeName == node.Name {
			return true
		}
	}
	if installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly && !installation.Spec.HCP.Enabled {
		selector := installation.Spec.Central.Placement.NodeSelector
		if len(selector) == 0 {
			selector = map[string]string{"node-role.kubernetes.io/control-plane": ""}
		}
		if labels.SelectorFromSet(selector).Matches(labels.Set(node.Labels)) {
			return true
		}
	}
	if installation.Spec.Mode != operatorv1alpha1.InstallationModeControlPlaneOnly {
		if labels.SelectorFromSet(installation.Spec.DataPlane.NodeSelector).Matches(labels.Set(node.Labels)) {
			return true
		}
	}
	return false
}

func (r *KubeOVNInstallationReconciler) Reconcile(ctx context.Context, req reconcile.Request) (result reconcile.Result, err error) {
	started := time.Now()
	result, err = r.reconcile(ctx, req)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	reconcileTotal.WithLabelValues(outcome).Inc()
	reconcileDuration.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
	return result, err
}

func (r *KubeOVNInstallationReconciler) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithValues("installation", req.NamespacedName)
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := r.Get(ctx, req.NamespacedName, installation); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !installation.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	now := time.Now
	if r.Clock != nil {
		now = r.Clock
	}
	plan := planner.Build(installation.Spec, installation.Status.ObservedVersion)
	if plan.Blocking != "" {
		if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
			return reconcile.Result{}, err
		}
		logger.Info("installation plan is blocked", "reason", plan.Blocking)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if installation.Spec.HCP.Enabled && r.HCP == nil && (r.Workload != nil || r.WorkloadFactory != nil) {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "HCPPeer", "HCP requires a peer provider before workload apply"
		if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
			return reconcile.Result{}, err
		}
		logger.Info("HCP installation is waiting for a peer provider")
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	var hcpRequest *hcp.PeerRequest
	if installation.Spec.HCP.Enabled && (r.Workload != nil || r.WorkloadFactory != nil) {
		request, requestErr := r.hcpPeerRequest(ctx, installation)
		if requestErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "HCPSecret", requestErr.Error()
			if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			logger.Info("HCP installation is waiting for its TLS Secret", "reason", requestErr.Error())
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		hcpRequest = &request
	}
	if err := validateMemberRequestLifecycle(installation); err != nil {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "LifecycleSelection", err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		logger.Info("member requests are waiting for an initial retained topology", "reason", err.Error())
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if upgradePaused(installation) {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "Paused", "upgrade is paused while the installation is degraded"
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		logger.Info("upgrade paused while installation is degraded")
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// Recover a durable membership operation before rendering the chart. Node
	// changes can make bootstrap rendering fail, while the operation's pinned
	// inventory remains the only safe path to convergence.
	if localRaft(installation) && r.Lifecycle != nil {
		if r.Leases == nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "LifecycleLease", "lifecycle recovery requires operation leases"
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
		}
		var recovery *lifecycle.Recovery
		recoveryErr := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
			return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
				var err error
				recovery, err = r.Lifecycle.Resume(centralCtx, installation, installation.Status.Topology.Operation)
				return err
			})
		})
		if recoveryErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", recoveryErr.Error()
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
		}
		if recovery != nil {
			operation := installation.Status.Topology.Operation
			if operation == nil || !operation.HandoffApplied {
				if err := validateLifecycleMemberRequest(installation, recovery.Request); err != nil {
					plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "Lifecycle", err.Error()
					if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
						return reconcile.Result{}, statusErr
					}
					return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
				}
			}
			recordLifecycleBackupStatus(installation, recovery)
			if installation.Status.Topology.Operation == nil {
				installation.Status.Topology.Operation = newOperationReference(recovery, installation.Status.Topology.Inventory)
			}
			if recovery.State.Phase != lifecycle.Complete {
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", fmt.Sprintf("durable central operation is %s", recovery.State.Phase)
				condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleInProgress", Message: plan.Blocking}
				if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
			}
			operation = installation.Status.Topology.Operation
			if operation == nil {
				return reconcile.Result{}, errors.New("completed lifecycle has no operation reference")
			}
			if recovery.Completion == nil && operation.CompletionDigest == "" {
				return reconcile.Result{}, errors.New("completed lifecycle has no immutable completion receipt")
			}
			if operation.CompletionDigest == "" && recovery.Completion != nil {
				operation.CompletionDigest = recovery.Completion.Digest
			}
			if operation.HandoffInventory == nil {
				if r.Topology == nil {
					return reconcile.Result{}, errors.New("completed lifecycle requires a topology handoff provider")
				}
				handoff, ok := r.Topology.(topology.HandoffProvider)
				if !ok || installation.Status.Topology.Inventory == nil {
					return reconcile.Result{}, errors.New("completed lifecycle cannot hand off without the retained topology provider")
				}
				var next *operatorv1alpha1.CentralTopologyReference
				handoffErr := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
					return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
						var err error
						next, err = handoff.Handoff(centralCtx, installation, installation.Status.Topology.Inventory)
						return err
					})
				})
				if handoffErr != nil {
					plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "TopologyHandoff", handoffErr.Error()
					if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
						return reconcile.Result{}, err
					}
					return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
				}
				if next == nil {
					return reconcile.Result{}, errors.New("topology handoff returned no reference")
				}
				operation.HandoffInventory = next
				operation.HandoffApplied = false
				installation.Status.Topology.Inventory = next
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "WorkloadHandoff", "central topology handed off; apply checkpoint authorization is pending"
				condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionTrue, Reason: "LifecycleComplete", Message: plan.Blocking}
				if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
			}
			if installation.Status.Topology.Inventory == nil || *installation.Status.Topology.Inventory != *operation.HandoffInventory {
				return reconcile.Result{}, errors.New("completed lifecycle handoff does not match the retained topology")
			}
			if !operation.HandoffApplied {
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "WorkloadHandoff", "lifecycle completion authorizes the next Chart apply checkpoint"
			}
		}
	}
	if installation.Status.ObservedVersion != "" && installation.Status.ObservedVersion != installation.Spec.Version {
		direction, directionErr := upgrade.CompareVersions(installation.Status.ObservedVersion, installation.Spec.Version)
		if directionErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "VersionContract", directionErr.Error()
			if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if direction == upgrade.VersionRollback {
			return r.reconcileRollback(ctx, installation, plan, now)
		}
		upgradeReady, upgradeErr := r.ensureUpgradeBackup(ctx, installation)
		if upgradeErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "Backup", upgradeErr.Error()
			if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if !upgradeReady {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "Backup", "verified upgrade backup pair is persisted; waiting for the next reconciliation before Chart apply"
			if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}
	if due, cleanup, cleanupErr := r.ensureBackupRetention(ctx, installation, now()); due {
		attempted := metav1.NewTime(now())
		if installation.Status.BackupRetention == nil {
			installation.Status.BackupRetention = new(operatorv1alpha1.BackupRetentionStatus)
		}
		status := installation.Status.BackupRetention
		status.LastAttempt = attempted
		status.Scanned = int32(cleanup.Scanned)
		status.Protected = int32(cleanup.Protected)
		status.Deleted = int32(cleanup.Deleted)
		if cleanupErr != nil {
			status.Reason = cleanupErr.Error()
			setCondition(&installation.Status.Conditions, metav1.Condition{Type: "BackupRetention", Status: metav1.ConditionFalse, Reason: "CleanupFailed", Message: cleanupErr.Error(), ObservedGeneration: installation.Generation, LastTransitionTime: attempted})
			if statusErr := r.Status().Update(ctx, installation); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		status.LastSuccess = attempted
		status.Reason = ""
		setCondition(&installation.Status.Conditions, metav1.Condition{Type: "BackupRetention", Status: metav1.ConditionTrue, Reason: "CleanupComplete", Message: fmt.Sprintf("retention scanned %d pair(s), deleted %d", cleanup.Scanned, cleanup.Deleted), ObservedGeneration: installation.Generation, LastTransitionTime: attempted})
		if statusErr := r.Status().Update(ctx, installation); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
	}
	// Retention is an independent policy. Run it before honoring a future
	// scheduled-backup deadline so a long capture interval cannot suppress
	// cleanup for the lifetime of the installation.
	if outcome, scheduleErr := r.ensureScheduledBackup(ctx, installation, now()); outcome.Due {
		installation.Status.Upgrade.Step = "BackupSchedule"
		if scheduleErr != nil {
			installation.Status.Phase = operatorv1alpha1.PhaseDegraded
			setCondition(&installation.Status.Conditions, metav1.Condition{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "BackupScheduleFailed", Message: scheduleErr.Error(), ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now())})
			setCondition(&installation.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "BackupScheduleFailed", Message: scheduleErr.Error(), ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now())})
			if statusErr := r.Status().Update(ctx, installation); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if err := r.Status().Update(ctx, installation); err != nil {
			return reconcile.Result{}, fmt.Errorf("persist scheduled backup checkpoint: %w", err)
		}
		return reconcile.Result{RequeueAfter: outcome.RequeueAfter}, nil
	} else if outcome.RequeueAfter > 0 {
		return reconcile.Result{RequeueAfter: outcome.RequeueAfter}, nil
	}
	if localRaft(installation) && r.Lifecycle != nil && (installation.Status.Topology.Operation == nil || installation.Status.Topology.Operation.HandoffApplied) && installation.Status.Topology.Inventory != nil && installation.Status.ApplyCheckpoint != nil {
		started, selectionErr := r.beginAutomaticLifecycle(ctx, installation, plan, now)
		if selectionErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "LifecycleSelection", selectionErr.Error()
			condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleSelectionBlocked", Message: selectionErr.Error()}
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
				return reconcile.Result{}, err
			}
			logger.Info("automatic lifecycle selection blocked", "reason", selectionErr.Error())
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if started {
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}
	renderSpec, retainedTopology, renderErr := r.lifecycleRenderSpec(ctx, installation)
	if renderErr != nil {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "Render", renderErr.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	renderer := r.Renderer
	if r.RendererFactory != nil {
		renderer = r.RendererFactory(installation)
	}
	if renderer == nil {
		renderer = chart.ValuesRenderer{}
	}
	rendered, err := renderer.Render(ctx, renderSpec)
	adoptionObjects := rendered.Objects
	if err == nil && retainedTopology != nil && len(rendered.Objects) > 0 {
		err = applyRetainedChartDeploymentReplicas(rendered.Objects, *retainedTopology)
	}
	if err == nil && len(rendered.Objects) > 0 {
		var probes []*unstructured.Unstructured
		probes, err = chart.NetworkProbeObjects(installation, rendered.Objects)
		rendered.Objects = append(rendered.Objects, probes...)
	}
	if err != nil {
		plan.Phase = operatorv1alpha1.PhaseDegraded
		plan.Step = "Render"
		plan.Blocking = err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	var upgradeObjects upgrade.ObjectSet
	upgradeRollout := upgradeOnlySpecChange(installation)
	if upgradeRollout {
		upgradeObjects, err = upgrade.Partition(rendered.Objects)
		if err != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "Render", err.Error()
			if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	workload := r.Workload
	if r.WorkloadFactory != nil {
		workload = r.WorkloadFactory(apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID})
	}
	var observation *apply.Observation
	var topologyCondition *metav1.Condition
	if workload == nil || len(rendered.Objects) == 0 {
		if err := r.ensurePlanConfigMap(ctx, installation, plan, rendered); err != nil {
			return reconcile.Result{}, err
		}
	} else {
		plan.Step = "AcquireLease"
		operation := func(operationCtx context.Context) error {
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := r.ensurePlanConfigMap(operationCtx, installation, plan, rendered); err != nil {
				return err
			}
			plan.Step = "ApplyCheckpoint"
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			// Capture the upgrade authorization before ensureApplyCheckpoint
			// records the target spec hash. Once that checkpoint advances, the
			// old-version hash is no longer available to infer authorization.
			upgradeAuthorized := upgradeAuthorizesApply(installation)
			upgradePhase := upgrade.RolloutPhase("")
			if upgradeAuthorized {
				upgradePhase = upgrade.ParsePhase(installation.Status.Upgrade.Step, upgradeObjects)
				plan.Step = string(upgradePhase)
			}
			if err := r.ensureApplyCheckpoint(operationCtx, installation, rendered); err != nil {
				return err
			}
			if localRaft(installation) && r.Topology != nil {
				plan.Step = "VerifyRetainedTopology"
				if err := r.ensureTopology(operationCtx, installation, false); err != nil {
					topologyCondition = &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: topologyFailureReason(err), Message: err.Error()}
					return err
				}
			}
			if installation.Spec.Adoption.Enabled {
				adopter, ok := workload.(apply.Adopter)
				if !ok {
					return errors.New("adoption requires a workload adopter")
				}
				plan.Step = "Adopt"
				if err := lease.CheckAll(operationCtx); err != nil {
					return err
				}
				if err := adopter.Adopt(operationCtx, adoptionObjects, installation.Spec.Adoption.Selector); err != nil {
					return err
				}
			}
			if upgradeAuthorized {
				plan.Step = "UpgradeCheckpoint"
				if err := r.beginUpgradeApply(operationCtx, installation); err != nil {
					return err
				}
			}
			plan.Step = "Apply"
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			objectsToApply := rendered.Objects
			if upgradeAuthorized {
				switch upgradePhase {
				case upgrade.RolloutCRDs:
					objectsToApply = upgradeObjects.CRDs
				case upgrade.RolloutCentral:
					objectsToApply = upgradeObjects.Central
				case upgrade.RolloutDataPlane:
					objectsToApply = upgradeObjects.DataPlane
				case upgrade.RolloutVerify:
					objectsToApply = nil
				}
			}
			if len(objectsToApply) > 0 {
				if err := workload.Apply(operationCtx, objectsToApply); err != nil {
					if upgradeAuthorized {
						return errors.Join(err, r.failUpgradeApply(operationCtx, installation, err.Error()))
					}
					return err
				}
			}
			if upgradeAuthorized {
				switch upgradePhase {
				case upgrade.RolloutCRDs:
					ready, err := r.upgradeCRDsReady(operationCtx, upgradeObjects.CRDs)
					if err != nil {
						return errors.Join(err, r.failUpgradeApply(operationCtx, installation, err.Error()))
					}
					if !ready {
						plan.Step = string(upgrade.RolloutCRDs)
						return nil
					}
					plan.Step = string(upgrade.NextPhase(upgradePhase, upgradeObjects))
					return nil
				case upgrade.RolloutCentral:
					ready, err := r.upgradeCentralReady(operationCtx, installation, workload, upgradeObjects.Central)
					if err != nil {
						return errors.Join(err, r.failUpgradeApply(operationCtx, installation, err.Error()))
					}
					if !ready {
						plan.Step = string(upgrade.RolloutCentral)
						return nil
					}
					plan.Step = string(upgrade.NextPhase(upgradePhase, upgradeObjects))
					return nil
				case upgrade.RolloutDataPlane:
					if err := r.markUpgradeCheckpointApplied(operationCtx, installation, now()); err != nil {
						return err
					}
					plan.Step = string(upgrade.RolloutVerify)
				case upgrade.RolloutVerify:
					plan.Step = string(upgrade.RolloutVerify)
				}
			}
			if lifecycleHandoffAuthorizesApply(installation) && installation.Status.Topology.Operation != nil {
				// Consume the lifecycle authorization when the Chart write is
				// accepted. Later observation failures must not make it reusable.
				if err := r.markLifecycleHandoffApplied(operationCtx, installation); err != nil {
					return err
				}
			}
			if upgradeAuthorized && (upgradePhase == upgrade.RolloutDataPlane || upgradePhase == upgrade.RolloutVerify) {
				plan.Step = string(upgrade.RolloutVerify)
			} else {
				plan.Step = "Observe"
			}
			if observer, ok := workload.(apply.Observer); ok {
				observed, err := observer.Observe(operationCtx, rendered.Objects)
				if err != nil {
					return err
				}
				observation = &observed
			}
			if observation != nil && observation.Ready && localRaft(installation) && r.Topology != nil {
				if !(upgradeAuthorized && (upgradePhase == upgrade.RolloutDataPlane || upgradePhase == upgrade.RolloutVerify)) {
					plan.Step = "RecordTopology"
				}
				condition := metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionTrue, Reason: "IdentityVerified", Message: "Live central identities match the retained topology"}
				if err := r.ensureTopology(operationCtx, installation, true); err != nil {
					condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, topologyFailureReason(err), err.Error()
				}
				topologyCondition = &condition
			}
			return nil
		}
		if r.Leases == nil {
			err = fmt.Errorf("workload management requires an operation lease manager")
		} else {
			err = r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
				if installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft && installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly {
					return r.Leases.With(operationCtx, installation, lease.Central, operation)
				}
				return operation(operationCtx)
			})
		}
		if err != nil {
			plan.Phase = operatorv1alpha1.PhaseDegraded
			plan.Blocking = err.Error()
			observation = nil
			logger.Info("installation operation blocked", "step", plan.Step, "reason", err.Error())
		}
	}
	// Health reads do not mutate workloads and must not extend the operation
	// Lease. Only a successful, complete rollout is eligible for verification.
	var ovnReport *health.OVNReport
	if err == nil && observation != nil && observation.Ready && r.OVN != nil {
		report, healthErr := r.OVN.Observe(ctx, installation)
		if healthErr != nil {
			report = health.OVNReport{Reason: "OVNObservationFailed", Message: healthErr.Error()}
		}
		ovnReport = &report
	}
	var hcpReport *hcp.PeerReport
	if err == nil && observation != nil && observation.Ready && installation.Spec.HCP.Enabled && r.HCP != nil {
		if hcpRequest == nil {
			request, requestErr := r.hcpPeerRequest(ctx, installation)
			if requestErr != nil {
				report := hcp.PeerReport{Reason: "TLSSecretUnavailable", Message: requestErr.Error()}
				hcpReport = &report
			} else {
				hcpRequest = &request
			}
		}
		if hcpRequest != nil {
			report, peerErr := r.HCP.Observe(ctx, *hcpRequest)
			if peerErr != nil {
				report = hcp.PeerReport{Reason: "HCPObservationFailed", Message: peerErr.Error()}
			} else if contractErr := hcp.ValidateReport(*hcpRequest, report); contractErr != nil {
				report.Ready = false
				report.Reason = "HCPContractInvalid"
				report.Message = contractErr.Error()
				report.TLSSecret = hcp.SecretIdentity{}
			}
			hcpReport = &report
		}
	}
	var runtimeReport *health.RuntimeReport
	if err == nil && observation != nil && observation.Ready && r.Runtime != nil {
		runtimeReport = new(r.Runtime.Observe(ctx, installation))
	}
	var networkReport *health.NetworkReport
	centralReady := ovnReport != nil && ovnReport.Ready
	if installation.Spec.HCP.Enabled && installation.Spec.HCP.Role == operatorv1alpha1.HCPRoleDataPlane {
		centralReady = hcpReport != nil && hcpReport.Ready
	}
	if centralReady && runtimeReport != nil && runtimeReport.Northd.Status == metav1.ConditionTrue && runtimeReport.DataPlane.Status == metav1.ConditionTrue && r.Network != nil {
		networkReport = new(r.Network.Observe(ctx, installation, runtimeReport.Nodes))
	}
	if err := r.updateStatus(ctx, installation, plan, now(), observation, ovnReport, runtimeReport, networkReport, topologyCondition, hcpReport); err != nil {
		return reconcile.Result{}, err
	}
	if errors.Is(err, lease.ErrHeld) {
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
}

// hcpPeerRequest binds provider observation to the current Secret object
// identity. The provider may read the Secret through its own client, but the
// controller passes only metadata so credentials cannot enter status, plans or
// rendered chart values. A replaced or rotated Secret therefore invalidates
// any report that was produced for an older generation.
func (r *KubeOVNInstallationReconciler) hcpPeerRequest(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (hcp.PeerRequest, error) {
	if r == nil || r.Client == nil || installation == nil {
		return hcp.PeerRequest{}, errors.New("HCP peer observation requires a Kubernetes client and installation")
	}
	secretRef := installation.Spec.HCP.TLSSecretRef
	if secretRef == nil || strings.TrimSpace(secretRef.Name) == "" {
		return hcp.PeerRequest{}, errors.New("HCP TLS Secret reference is empty")
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secret := new(corev1.Secret)
	key := client.ObjectKey{Namespace: installation.Namespace, Name: secretRef.Name}
	if err := reader.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return hcp.PeerRequest{}, fmt.Errorf("HCP TLS Secret %s/%s was not found", key.Namespace, key.Name)
		}
		return hcp.PeerRequest{}, fmt.Errorf("read HCP TLS Secret %s/%s: %w", key.Namespace, key.Name, err)
	}
	if secret.UID == "" {
		return hcp.PeerRequest{}, fmt.Errorf("HCP TLS Secret %s/%s has no stable UID", key.Namespace, key.Name)
	}
	return hcp.PeerRequest{
		Installation: types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name},
		Version:      installation.Spec.Version,
		Config:       installation.Spec.HCP,
		TLSSecret: hcp.SecretIdentity{
			Namespace:       secret.Namespace,
			Name:            secret.Name,
			UID:             secret.UID,
			ResourceVersion: secret.ResourceVersion,
		},
	}, nil
}

func (r *KubeOVNInstallationReconciler) beginAutomaticLifecycle(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now func() time.Time) (bool, error) {
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok {
		return false, errors.New("automatic lifecycle selection requires a retained topology provider")
	}
	if r.Leases == nil {
		return false, errors.New("automatic lifecycle selection requires operation leases")
	}
	var reference lifecycle.Reference
	var request *lifecycle.Request
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			retained, err := provider.Retained(centralCtx, installation)
			if err != nil {
				return err
			}
			replacement := installation.Spec.Central.Replicas == int32(len(retained.Members)) && hasMemberReplacementRequest(installation)
			if installation.Spec.Central.Replicas > int32(len(retained.Members)) || replacement {
				request, err = lifecycle.SelectAddition(centralCtx, r.Lifecycle.Reader(), installation, retained)
			} else {
				request, err = lifecycle.SelectRemoval(centralCtx, r.Lifecycle.Reader(), installation, retained)
			}
			if err != nil || request == nil {
				return err
			}
			replicaOnly, err := replicaOnlySpecChange(installation, len(retained.Members))
			if err != nil {
				return err
			}
			if !replicaOnly {
				return errors.New("automatic lifecycle selection requires the desired change to modify central replicas only")
			}
			// A lost Deployment endpoint is the one deliberate exception to the
			// retained-identity check: its old Pod cannot still be present. The
			// selector has already pinned the old UID, every survivor and the
			// approval; Begin performs a fresh NB/SB/quorum observation before any
			// mutation. A healthy retained Node with IP drift uses a separate
			// exact-target observation and rebinds the request to its fresh
			// addresses before Begin. All other transitions still require an
			// unchanged baseline.
			if request.AutomaticReplacement && request.Retirement != nil && !request.Retirement.EndpointMissing {
				replacementProvider, ok := r.Topology.(topology.ReplacementProvider)
				if !ok {
					return errors.New("automatic address replacement requires a drift-tolerant topology provider")
				}
				observed, err := replacementProvider.VerifyRetainedReplacement(centralCtx, installation, request.RemoveEndpoint)
				if err != nil {
					return fmt.Errorf("verify retained topology for automatic address replacement: %w", err)
				}
				updated, err := lifecycle.RebindAutomaticReplacement(*request, retained, observed)
				if err != nil {
					return fmt.Errorf("rebind automatic replacement addresses: %w", err)
				}
				request = &updated
			} else if request.Retirement == nil || !request.Retirement.EndpointMissing {
				verified, err := r.Topology.VerifyRetained(centralCtx, installation)
				if err != nil {
					return fmt.Errorf("verify retained topology before automatic selection: %w", err)
				}
				if verified == nil || installation.Status.Topology.Inventory == nil || *verified != *installation.Status.Topology.Inventory {
					return errors.New("automatic lifecycle selection requires a complete fresh topology matching the retained inventory")
				}
			}
			reference, err = r.Lifecycle.Begin(centralCtx, installation, *request)
			return err
		})
	})
	if err != nil {
		return false, err
	}
	if request == nil {
		return false, nil
	}
	installation.Status.Topology.Operation = operationReference(reference, installation.Status.Topology.Inventory)
	plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", "automatic central membership operation admitted; durable execution will resume before Chart apply"
	condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleInProgress", Message: plan.Blocking}
	if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
		return false, err
	}
	return true, nil
}

func hasMemberReplacementRequest(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || !installation.Spec.Central.DynamicMembership {
		return false
	}
	for _, member := range installation.Spec.Central.MemberRequests {
		if member.ReplaceEndpoint != "" || (member.RetireEndpoint == "" && member.RemovalApprovalRef != nil) {
			return true
		}
	}
	return false
}

func (r *KubeOVNInstallationReconciler) ensurePlanConfigMap(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, rendered chart.Rendered) error {
	planBytes, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("marshal plan: %w", err)
	}
	configMap := new(corev1.ConfigMap)
	name := planConfigMapPrefix + installation.Name
	configMap.Name = name
	configMap.Namespace = installation.Namespace
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Name = name
		configMap.Namespace = installation.Namespace
		if configMap.Labels == nil {
			configMap.Labels = map[string]string{}
		}
		configMap.Labels[apply.ManagedByLabel] = apply.ManagedByValue
		configMap.Labels[apply.InstallationLabel] = installation.Name
		configMap.Labels[apply.InstallationUIDLabel] = string(installation.UID)
		configMap.Labels[apply.InstallationNamespaceLabel] = installation.Namespace
		if err := controllerutil.SetControllerReference(installation, configMap, r.Scheme); err != nil {
			return err
		}
		configMap.Data = map[string]string{
			"plan.json":        string(planBytes),
			"values.yaml":      string(rendered.ValuesYAML),
			"chart.source":     rendered.Source,
			"chart.digest":     rendered.Digest,
			"install-mode":     plan.Values["installMode"],
			"central-mode":     plan.Values["centralMode"],
			"central-replicas": plan.Values["centralReplicas"],
			"version":          plan.Values["version"],
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure plan ConfigMap: %w", err)
	}
	return nil
}

func (r *KubeOVNInstallationReconciler) updateStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time, observation *apply.Observation, ovnReport *health.OVNReport, runtimeReport *health.RuntimeReport, networkReport *health.NetworkReport, topologyCondition *metav1.Condition, hcpReports ...*hcp.PeerReport) error {
	previous := installation.Status.DeepCopy()
	status := installation.Status.DeepCopy()
	var hcpReport *hcp.PeerReport
	if len(hcpReports) != 0 {
		hcpReport = hcpReports[0]
	}
	if status.ApplyCheckpoint == nil && plan.Step != "Rollback" {
		status.ObservedVersion = ""
	}
	status.Phase = plan.Phase
	status.ObservedGeneration = installation.Generation
	status.Topology.DesiredCentralReplicas = installation.Spec.Central.Replicas
	status.Topology.ReadyCentralReplicas = 0
	status.Topology.Members = nil
	status.Topology.Databases = nil
	status.Topology.NorthdProcesses = nil
	status.Topology.Nodes = nil
	status.Network = nil
	status.HCP = operatorv1alpha1.HCPStatus{}
	status.Upgrade.Step = plan.Step
	status.Upgrade.CanRollback = false
	if installation.Status.ObservedVersion == "" || installation.Status.ObservedVersion == installation.Spec.Version {
		status.Upgrade.Rollback = nil
	}
	readyReason, message := "PlanOnly", "The installation plan is valid; workload readiness has not been observed."
	workloadStatus := metav1.ConditionUnknown
	workloadReason := "NotObserved"
	workloadMessage := message
	degraded := metav1.ConditionFalse
	degradedReason := "PlanValid"
	ovnStatus, ovnReason, ovnMessage := metav1.ConditionUnknown, "NotObserved", "OVN health has not been observed for this rollout"
	northd := metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Northd has not been observed for this rollout"}
	dataPlane := metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Data-plane connections have not been observed for this rollout"}
	network := metav1.Condition{Type: "NetworkReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Network probes await healthy workloads, databases and runtime"}
	hcpCondition := metav1.Condition{Type: "HCPReady", Status: metav1.ConditionTrue, Reason: "NotRequired", Message: "This installation does not use hosted control-plane pairing"}
	if installation.Spec.HCP.Enabled {
		hcpCondition = metav1.Condition{Type: "HCPReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "HCP peer health has not been observed for this rollout"}
		if hcpReport != nil {
			status.HCP.Paired = hcpReport.Paired
			status.HCP.EndpointReady = hcpReport.EndpointReady
			status.HCP.PeerObservedVersion = hcpReport.Peer.Version
			status.HCP.TLSSecretUID = hcpReport.TLSSecret.UID
			status.HCP.TLSSecretResourceVersion = hcpReport.TLSSecret.ResourceVersion
			if hcpReport.Ready {
				hcpCondition.Status, hcpCondition.Reason, hcpCondition.Message = metav1.ConditionTrue, "PeerReady", "HCP peer is paired and its configured endpoints are healthy"
			} else {
				hcpCondition.Status = metav1.ConditionFalse
				hcpCondition.Reason = hcpReport.Reason
				hcpCondition.Message = hcpReport.Message
				if hcpCondition.Reason == "" {
					hcpCondition.Reason = "PeerNotReady"
				}
				if hcpCondition.Message == "" {
					hcpCondition.Message = "HCP peer is not ready"
				}
			}
		} else if plan.Step == "HCPPeer" && plan.Blocking != "" {
			hcpCondition.Status, hcpCondition.Reason, hcpCondition.Message = metav1.ConditionFalse, "PeerProviderUnavailable", plan.Blocking
		}
		if hcpCondition.Status == metav1.ConditionFalse {
			status.Phase = operatorv1alpha1.PhaseDegraded
			degraded, degradedReason = metav1.ConditionTrue, hcpCondition.Reason
			message = hcpCondition.Message
		}
	}
	topologyState := metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Complete central identity inventory has not been verified for this rollout"}
	if !localRaft(installation) {
		topologyState.Status, topologyState.Reason, topologyState.Message = metav1.ConditionTrue, "NotRequired", "Local RAFT topology inventory does not apply to this installation"
	} else if topologyCondition != nil {
		topologyState = *topologyCondition
	}
	readyStatus := metav1.ConditionFalse
	if observation != nil {
		status.Topology.ReadyCentralReplicas = observation.ReadyCentralReplicas
		workloadStatus = metav1.ConditionFalse
		workloadReason = "RolloutProgressing"
		readyReason, message = "WorkloadsProgressing", observation.Message
		workloadMessage = observation.Message
		if observation.Ready {
			workloadStatus, workloadReason = metav1.ConditionTrue, "RolloutComplete"
			status.ObservedVersion = installation.Spec.Version
			readyReason = "VerificationPending"
		}
	}
	if observation != nil && observation.Ready && status.Upgrade.Checkpoint != nil && status.Upgrade.Checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplying && status.Upgrade.Checkpoint.TargetVersion == installation.Spec.Version {
		status.Upgrade.Checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointApplied
		status.Upgrade.Checkpoint.Reason = "target rollout observed after an in-flight Chart write"
		status.Upgrade.Checkpoint.AppliedAt = metav1.NewTime(now.UTC())
		status.Upgrade.CheckpointAuthorized = false
	}
	if ovnReport != nil {
		ovnStatus, ovnReason, ovnMessage = metav1.ConditionFalse, ovnReport.Reason, ovnReport.Message
		status.Topology.Members = slices.Clone(ovnReport.Members)
		status.Topology.Databases = slices.Clone(ovnReport.Databases)
		for index := range status.Topology.Members {
			status.Topology.Members[index].LastObserved = metav1.NewTime(now)
		}
		if ovnReport.Ready {
			ovnStatus = metav1.ConditionTrue
			message = "Workloads and OVN databases are healthy; runtime and network verification are still required"
		} else {
			message = ovnMessage
			if ovnReason == "OVNDatabaseUnhealthy" || ovnReason == "OVNObservationFailed" {
				status.Phase = operatorv1alpha1.PhaseDegraded
				degraded, degradedReason = metav1.ConditionTrue, ovnReason
			}
		}
	}
	if runtimeReport != nil {
		northd, dataPlane = runtimeReport.Northd, runtimeReport.DataPlane
		status.Topology.NorthdProcesses = slices.Clone(runtimeReport.NorthdProcesses)
		status.Topology.Nodes = slices.Clone(runtimeReport.Nodes)
		for index := range status.Topology.NorthdProcesses {
			status.Topology.NorthdProcesses[index].LastObserved = metav1.NewTime(now)
		}
		for index := range status.Topology.Nodes {
			status.Topology.Nodes[index].LastObserved = metav1.NewTime(now)
		}
		for _, condition := range []metav1.Condition{northd, dataPlane} {
			if condition.Status == metav1.ConditionFalse {
				status.Phase = operatorv1alpha1.PhaseDegraded
				if degraded == metav1.ConditionFalse {
					degraded, degradedReason, message = metav1.ConditionTrue, condition.Reason, condition.Message
				}
			}
		}
		if ovnStatus == metav1.ConditionTrue && northd.Status == metav1.ConditionTrue && dataPlane.Status == metav1.ConditionTrue {
			message = "Workloads, OVN databases, northd and data-plane connections are healthy; network probes remain required"
		}
	}
	if plan.Blocking != "" {
		readyReason, message = "SafetyGateBlocked", plan.Blocking
		workloadMessage = message
		degraded, degradedReason = metav1.ConditionTrue, "SafetyGateBlocked"
	}
	if networkReport != nil {
		network = networkReport.Condition
		if network.Status == metav1.ConditionFalse {
			status.Phase = operatorv1alpha1.PhaseDegraded
			degraded, degradedReason, message = metav1.ConditionTrue, network.Reason, network.Message
		}
		if network.Status == metav1.ConditionTrue && networkReport.Evidence != nil {
			status.Network = new(*networkReport.Evidence)
			status.Network.AddressFamilies = slices.Clone(networkReport.Evidence.AddressFamilies)
			status.Network.LastObserved = metav1.NewTime(now)
		}
	}
	if topologyState.Status == metav1.ConditionFalse {
		status.Phase = operatorv1alpha1.PhaseDegraded
		degraded, degradedReason, readyReason, message = metav1.ConditionTrue, "TopologyUnverified", "TopologyUnverified", topologyState.Message
	}
	networkVerified := network.Status == metav1.ConditionTrue && (installation.Spec.Mode == operatorv1alpha1.InstallationModeControlPlaneOnly && network.Reason == "NotRequired" || status.Network != nil && status.Network.ObservedNodes > 0 && status.Network.SuccessfulProbes > 0 && len(status.Network.AddressFamilies) > 0)
	if topologyState.Status == metav1.ConditionTrue && hcpCondition.Status == metav1.ConditionTrue && plan.Blocking == "" && workloadStatus == metav1.ConditionTrue && ovnStatus == metav1.ConditionTrue && northd.Status == metav1.ConditionTrue && dataPlane.Status == metav1.ConditionTrue && networkVerified {
		readyStatus, readyReason, message = metav1.ConditionTrue, "VerificationComplete", "Workloads, OVN databases, runtime and configured network probes passed verification"
		status.Phase = operatorv1alpha1.PhaseReady
		status.Upgrade.Step = "Verified"
	}
	for _, condition := range []metav1.Condition{
		{Type: "Ready", Status: readyStatus, Reason: readyReason, Message: message},
		{Type: "Degraded", Status: degraded, Reason: degradedReason, Message: message},
		{Type: "WorkloadsReady", Status: workloadStatus, Reason: workloadReason, Message: workloadMessage},
		{Type: "OVNDBReady", Status: ovnStatus, Reason: ovnReason, Message: ovnMessage},
		northd, dataPlane,
		network, topologyState, hcpCondition,
	} {
		condition.ObservedGeneration = installation.Generation
		condition.LastTransitionTime = metav1.NewTime(now)
		setCondition(&status.Conditions, condition)
	}
	updated := installationWithStatus(installation, *status)
	if err := r.Status().Update(ctx, updated); err != nil {
		return err
	}
	r.observeStatus(installation, *previous, *status)
	installation.ResourceVersion = updated.ResourceVersion
	installation.Status = updated.Status
	return nil
}

func (r *KubeOVNInstallationReconciler) observeStatus(installation *operatorv1alpha1.KubeOVNInstallation, previous, current operatorv1alpha1.KubeOVNInstallationStatus) {
	if installation == nil {
		return
	}
	name := installation.Name
	namespace := installation.Namespace
	ready := conditionValue(current.Conditions, "Ready")
	installationReady.WithLabelValues(namespace, name).Set(ready)
	for _, condition := range current.Conditions {
		installationCondition.WithLabelValues(namespace, name, condition.Type).Set(conditionMetricValue(condition.Status))
	}
	if r.Recorder == nil {
		return
	}
	if previous.Phase != current.Phase {
		eventType := corev1.EventTypeNormal
		if current.Phase == operatorv1alpha1.PhaseDegraded {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Eventf(installation, eventType, "PhaseChanged", "installation phase changed from %q to %q", previous.Phase, current.Phase)
	}
	previousReady := findInstallationCondition(previous.Conditions, "Ready")
	currentReady := findInstallationCondition(current.Conditions, "Ready")
	if currentReady != nil && (previousReady == nil || previousReady.Status != currentReady.Status || previousReady.Reason != currentReady.Reason) {
		eventType := corev1.EventTypeNormal
		if currentReady.Status != metav1.ConditionTrue {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Eventf(installation, eventType, "ReadyChanged", "%s", currentReady.Message)
	}
}

func findInstallationCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}
	return nil
}

func conditionValue(conditions []metav1.Condition, conditionType string) float64 {
	condition := findInstallationCondition(conditions, conditionType)
	if condition != nil && condition.Status == metav1.ConditionTrue {
		return 1
	}
	return 0
}

func conditionMetricValue(status metav1.ConditionStatus) float64 {
	switch status {
	case metav1.ConditionTrue:
		return 1
	case metav1.ConditionFalse:
		return 0
	default:
		return -1
	}
}

func localRaft(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	return installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft && installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly && !installation.Spec.HCP.Enabled
}

func topologyFailureReason(err error) string {
	if _, ok := errors.AsType[*health.EndpointDriftError](err); ok {
		return "CentralEndpointDrift"
	}
	if node, ok := errors.AsType[*health.NodeLifecycleError](err); ok {
		if node.Unschedulable {
			return "CentralNodeDraining"
		}
		return "CentralNodeUnavailable"
	}
	return "TopologyUnverified"
}

func (r *KubeOVNInstallationReconciler) ensureTopology(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, initialize bool) error {
	var ref *operatorv1alpha1.CentralTopologyReference
	var observationErr error
	if initialize {
		ref, observationErr = r.Topology.Ensure(ctx, installation)
	} else {
		ref, observationErr = r.Topology.VerifyRetained(ctx, installation)
	}
	if ref == nil {
		if observationErr != nil {
			return observationErr
		}
		if !initialize && installation.Status.Topology.Inventory == nil {
			return nil
		}
		return errors.New("topology provider returned no retained identity reference")
	}
	if ref.Name == "" || ref.UID == "" || ref.Digest == "" {
		return errors.New("topology provider returned an incomplete reference")
	}
	if existing := installation.Status.Topology.Inventory; existing != nil {
		if *existing != *ref {
			return errors.New("retained topology reference changed without lifecycle authorization")
		}
		return observationErr
	}
	if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
		return err
	}
	installation.Status.Topology.Inventory = ref
	updated := installation.DeepCopy()
	if err := r.Status().Update(ctx, updated); err != nil {
		installation.Status.Topology.Inventory = nil
		return fmt.Errorf("persist central topology reference: %w", err)
	}
	installation.ResourceVersion = updated.ResourceVersion
	installation.Status = updated.Status
	return observationErr
}

func newOperationReference(recovery *lifecycle.Recovery, inventory *operatorv1alpha1.CentralTopologyReference) *operatorv1alpha1.CentralOperationReference {
	return operationReference(recovery.Reference, inventory)
}

func operationReference(reference lifecycle.Reference, inventory *operatorv1alpha1.CentralTopologyReference) *operatorv1alpha1.CentralOperationReference {
	result := &operatorv1alpha1.CentralOperationReference{JournalName: reference.Journal.Name, JournalUID: reference.Journal.UID, RequestName: reference.Request.Name, RequestUID: reference.Request.UID}
	if inventory != nil {
		result.Inventory = *inventory
	}
	return result
}

func replicaOnlySpecChange(installation *operatorv1alpha1.KubeOVNInstallation, retainedReplicas int) (bool, error) {
	if installation == nil || installation.Status.ApplyCheckpoint == nil || retainedReplicas < 1 {
		return false, errors.New("automatic lifecycle selection requires an existing apply checkpoint and retained member count")
	}
	currentHash, currentBaseHash, err := installationSpecHashes(installation.Spec)
	if err != nil {
		return false, fmt.Errorf("hash current installation spec: %w", err)
	}
	if currentHash == installation.Status.ApplyCheckpoint.SpecHash {
		return true, nil
	}
	spec := installation.Spec
	spec.Central.Replicas = int32(retainedReplicas)
	retainedHash, retainedBaseHash, err := installationSpecHashes(spec)
	if err != nil {
		return false, fmt.Errorf("hash prior installation spec: %w", err)
	}
	if retainedHash == installation.Status.ApplyCheckpoint.SpecHash {
		return true, nil
	}
	// Prepared member slots are lifecycle inputs. The first replica increase
	// commonly adds those slots at the same time as the desired replica count;
	// the initial Chart checkpoint predates them, so compare once with the
	// slots omitted. Once the handoff is accepted, the full current spec is
	// recorded in the checkpoint and the exact hash path above applies again.
	if len(spec.Central.MemberRequests) == 0 {
		return installation.Status.ApplyCheckpoint.BaseSpecHash != "" && currentBaseHash == installation.Status.ApplyCheckpoint.BaseSpecHash, nil
	}
	spec.Central.MemberRequests = nil
	_, baseHash, err := installationSpecHashes(spec)
	if err != nil {
		return false, fmt.Errorf("hash prior installation spec without member requests: %w", err)
	}
	if baseHash == installation.Status.ApplyCheckpoint.SpecHash {
		return true, nil
	}
	if installation.Status.ApplyCheckpoint.BaseSpecHash == "" {
		return false, nil
	}
	return currentBaseHash == installation.Status.ApplyCheckpoint.BaseSpecHash || retainedBaseHash == installation.Status.ApplyCheckpoint.BaseSpecHash, nil
}

func installationSpecHashes(spec operatorv1alpha1.KubeOVNInstallationSpec) (full, base string, err error) {
	data, err := stdjson.Marshal(spec, stdjson.Deterministic(true))
	if err != nil {
		return "", "", err
	}
	full = fmt.Sprintf("%x", sha256.Sum256(data))
	spec.Central.MemberRequests = nil
	data, err = stdjson.Marshal(spec, stdjson.Deterministic(true))
	if err != nil {
		return "", "", err
	}
	return full, fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func validateMemberRequestLifecycle(installation *operatorv1alpha1.KubeOVNInstallation) error {
	if installation == nil || len(installation.Spec.Central.MemberRequests) == 0 {
		return nil
	}
	if installation.Status.Topology.Inventory == nil {
		return errors.New("central.memberRequests are additions and require an existing retained topology; remove them for the initial installation and add them after the first topology is recorded")
	}
	return nil
}

func validateLifecycleMemberRequest(installation *operatorv1alpha1.KubeOVNInstallation, request lifecycle.Request) error {
	if installation == nil || request.Add == nil && request.RemoveEndpoint == "" {
		return nil
	}
	if request.Add == nil {
		for _, member := range installation.Spec.Central.MemberRequests {
			if member.RetireEndpoint == request.RemoveEndpoint {
				if !lifecycleMemberApprovalMatches(member, request) {
					return fmt.Errorf("retirement member request %q changed after operation admission; restore removalApprovalRef before retrying", member.Name)
				}
				return nil
			}
		}
		for _, member := range installation.Spec.Central.MemberRequests {
			if member.RetireEndpoint != "" {
				return fmt.Errorf("retirement member request changed after operation admission; restore retireEndpoint %q before retrying", request.RemoveEndpoint)
			}
		}
		return nil
	}
	plan := request.Add.Workload.Preparation
	var northboundAddress, southboundAddress string
	for _, database := range plan.Databases {
		switch database.Name {
		case "OVN_Northbound":
			northboundAddress = database.LocalAddress
		case "OVN_Southbound":
			southboundAddress = database.LocalAddress
		}
	}
	memberName := plan.MemberName
	for _, member := range installation.Spec.Central.MemberRequests {
		if member.Name != memberName {
			continue
		}
		autoReplacement := member.ReplaceEndpoint == "" && member.RetireEndpoint == "" && member.RemovalApprovalRef != nil
		memberTLSName := ""
		if member.TLSSecretRef != nil {
			memberTLSName = member.TLSSecretRef.Name
		}
		requestTLSName := ""
		if request.Add.Workload.TLSSecret != nil {
			requestTLSName = request.Add.Workload.TLSSecret.Name
		}
		if member.NodeName != request.Add.Workload.Node.Name || member.ClaimName != request.Add.Workload.Claim.Name || member.VolumeName != request.Add.Workload.Volume.Name || member.Image != request.Add.Workload.Image || member.NorthboundAddress != northboundAddress || member.SouthboundAddress != southboundAddress || member.NorthboundClientAddress != request.Add.NorthboundAddress || member.SouthboundClientAddress != request.Add.SouthboundAddress || memberTLSName != requestTLSName || request.AutomaticReplacement != autoReplacement || !autoReplacement && member.ReplaceEndpoint != request.RemoveEndpoint || !lifecycleMemberApprovalMatches(member, request) {
			return fmt.Errorf("lifecycle member request %q changed after operation admission; restore the pinned node, storage, image, addresses and TLS Secret reference before retrying", memberName)
		}
		return nil
	}
	return fmt.Errorf("lifecycle member request %q was removed after operation admission; restore the pinned request before retrying", memberName)
}

func lifecycleMemberApprovalMatches(member operatorv1alpha1.CentralMemberSpec, request lifecycle.Request) bool {
	name, uid := "", ""
	if member.RemovalApprovalRef != nil {
		name, uid = member.RemovalApprovalRef.Name, string(member.RemovalApprovalRef.UID)
	}
	if len(request.Databases) == 0 {
		// Synthetic planner tests and pre-admission callers may not carry the
		// database inventory yet. They cannot prove an approval reference, but
		// an omitted reference remains safely equivalent to the old contract.
		return member.RemovalApprovalRef == nil
	}
	if len(request.Databases) != 2 {
		return false
	}
	for _, database := range request.Databases {
		if database.Approval.Name != name || database.Approval.UID != uid {
			return false
		}
	}
	return true
}

// lifecycleRenderSpec returns the legal Raft input plus the retained topology
// used to patch the rendered Chart Deployment after template evaluation.
// Without that boundary, a successful addition would scale the old Deployment
// at the same time as the new StatefulSet and create duplicate RAFT members.
func (r *KubeOVNInstallationReconciler) lifecycleRenderSpec(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (operatorv1alpha1.KubeOVNInstallationSpec, *health.CentralTopology, error) {
	spec := installation.Spec
	if !localRaft(installation) || !spec.Central.DynamicMembership || installation.Status.Topology.Inventory == nil || r.Topology == nil {
		return spec, nil, nil
	}
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok || r.Leases == nil {
		return spec, nil, errors.New("dynamic membership rendering requires a retained topology provider and operation leases")
	}
	retained := new(health.CentralTopology)
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			var err error
			*retained, err = provider.Retained(centralCtx, installation)
			return err
		})
	})
	if err != nil {
		return spec, nil, fmt.Errorf("read retained topology for dynamic chart rendering: %w", err)
	}
	chartReplicas := int32(0)
	for _, member := range retained.Members {
		if member.Workload.Kind == "Deployment" {
			chartReplicas++
		}
	}
	if chartReplicas == 0 {
		return spec, nil, errors.New("dynamic membership requires at least one retained Chart Deployment member")
	}
	// Keep the legal Raft member count as the renderer input. The rendered
	// Deployment is patched separately to the retained member count so the
	// independently managed replacement members do not get duplicated.
	return spec, retained, nil
}

func applyRetainedChartDeploymentReplicas(objects []*unstructured.Unstructured, retained health.CentralTopology) error {
	deploymentReplicas := make(map[string]int64)
	for _, member := range retained.Members {
		if member.Workload.Kind != "Deployment" || member.Workload.Name == "" {
			continue
		}
		deploymentReplicas[member.Workload.Name]++
	}
	if len(deploymentReplicas) == 0 {
		return errors.New("dynamic membership requires at least one retained Chart Deployment member")
	}
	matched := make(map[string]struct{}, len(deploymentReplicas))
	for _, object := range objects {
		if object.GetKind() != "Deployment" {
			continue
		}
		replicas, ok := deploymentReplicas[object.GetName()]
		if !ok {
			continue
		}
		if err := unstructured.SetNestedField(object.Object, replicas, "spec", "replicas"); err != nil {
			return fmt.Errorf("set retained Chart Deployment %q replicas: %w", object.GetName(), err)
		}
		matched[object.GetName()] = struct{}{}
	}
	if len(matched) != len(deploymentReplicas) {
		missing := make([]string, 0, len(deploymentReplicas)-len(matched))
		for name := range deploymentReplicas {
			if _, ok := matched[name]; !ok {
				missing = append(missing, name)
			}
		}
		slices.Sort(missing)
		return fmt.Errorf("rendered Chart omitted retained Deployment(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

func recordLifecycleBackupStatus(installation *operatorv1alpha1.KubeOVNInstallation, recovery *lifecycle.Recovery) {
	if installation == nil || recovery == nil || len(recovery.Request.Databases) != 2 {
		return
	}
	refs := make(map[string]string, len(recovery.Request.Databases))
	latest := time.Time{}
	for index, database := range recovery.Request.Databases {
		if index >= len(recovery.State.Databases) || database.Database == "" {
			return
		}
		captured := recovery.State.Databases[index]
		if captured.BackupRef == "" || captured.CapturedAt.IsZero() {
			return
		}
		refs[database.Database] = captured.BackupRef
		if captured.CapturedAt.After(latest) {
			latest = captured.CapturedAt
		}
	}
	if len(refs) != 2 {
		return
	}
	data, err := stdjson.Marshal(refs, stdjson.Deterministic(true))
	if err != nil {
		return
	}
	backup := &operatorv1alpha1.BackupStatus{ID: fmt.Sprintf("sha256:%x", sha256.Sum256(data)), DatabaseRefs: refs, Destination: backupDestination(installation), Version: installation.Status.ObservedVersion, CompletedAt: metav1.NewTime(latest)}
	if installation.Status.Topology.Inventory != nil {
		backup.TopologyDigest = installation.Status.Topology.Inventory.Digest
	}
	installation.Status.LastBackup = backup
}

type scheduledBackupOutcome struct {
	Due          bool
	RequeueAfter time.Duration
}

const backupRetentionCheckInterval = 10 * time.Minute

func (r *KubeOVNInstallationReconciler) ensureBackupRetention(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, current time.Time) (bool, backup.RetentionResult, error) {
	if !localRaft(installation) || r.Lifecycle == nil {
		return false, backup.RetentionResult{}, nil
	}
	policy, err := backup.ParseRetention(installation.Spec.Upgrade.Backup.Retention.KeepLast, installation.Spec.Upgrade.Backup.Retention.MaxAge)
	if err != nil {
		return true, backup.RetentionResult{}, err
	}
	if policy.KeepLast == 0 && policy.MaxAge == 0 {
		return false, backup.RetentionResult{}, nil
	}
	if previous := installation.Status.BackupRetention; previous != nil && previous.Reason == "" && !previous.LastSuccess.IsZero() && current.Sub(previous.LastSuccess.Time) < backupRetentionCheckInterval {
		return false, backup.RetentionResult{}, nil
	}
	if r.Leases == nil {
		return true, backup.RetentionResult{}, errors.New("backup retention requires operation leases")
	}
	protected := protectedBackupReferences(installation)
	recoveryRefs, err := r.protectedRecoveryBackupReferences(ctx, installation)
	if err != nil {
		return true, backup.RetentionResult{}, err
	}
	protected = append(protected, recoveryRefs...)
	var active []string
	var cleanup backup.RetentionResult
	err = r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			if installation.Status.Topology.Operation != nil {
				active, err = r.Lifecycle.ProtectedBackupRefs(centralCtx, installation, installation.Status.Topology.Operation)
				if err != nil {
					return err
				}
			}
			protected = append(protected, active...)
			protected = slices.Compact(slices.Sorted(slices.Values(protected)))
			cleanup, err = r.Lifecycle.CleanupBackups(centralCtx, installation, policy, protected, current)
			return err
		})
	})
	return true, cleanup, err
}

// protectedRecoveryBackupReferences keeps a durable recovery plan's backup
// pair available until the request reaches a terminal phase. The request may
// be waiting for an external executor, so installation status alone cannot
// describe every pair that remains needed. Listing failure is returned to the
// caller and prevents destructive cleanup.
func (r *KubeOVNInstallationReconciler) protectedRecoveryBackupReferences(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) ([]string, error) {
	if r == nil || r.Client == nil || installation == nil {
		return nil, errors.New("backup retention cannot inspect recovery requests without a Kubernetes client")
	}
	requests := new(operatorv1alpha1.KubeOVNRecoveryRequestList)
	if err := r.List(ctx, requests, client.InNamespace(installation.Namespace)); err != nil {
		return nil, fmt.Errorf("list recovery requests for backup retention: %w", err)
	}
	refs := make([]string, 0, len(requests.Items))
	for index := range requests.Items {
		request := &requests.Items[index]
		if request.Spec.InstallationRef != installation.Name {
			continue
		}
		switch request.Status.Phase {
		case operatorv1alpha1.RecoveryPhaseSucceeded, operatorv1alpha1.RecoveryPhaseFailed, operatorv1alpha1.RecoveryPhaseExpired:
			continue
		}
		if request.Status.Plan != nil && request.Status.Plan.InstallationUID != "" && request.Status.Plan.InstallationUID != installation.UID {
			continue
		}
		reference := ""
		if request.Status.Plan != nil {
			reference = request.Status.Plan.BackupRef
		}
		if reference == "" {
			reference = request.Spec.BackupRef
		}
		if strings.TrimSpace(reference) != "" {
			refs = append(refs, reference)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(refs))), nil
}

func protectedBackupReferences(installation *operatorv1alpha1.KubeOVNInstallation) []string {
	if installation == nil {
		return nil
	}
	refs := make([]string, 0, 6)
	if last := installation.Status.LastBackup; last != nil {
		refs = append(refs, last.DatabaseRefs["OVN_Northbound"], last.DatabaseRefs["OVN_Southbound"])
	}
	if checkpoint := installation.Status.Upgrade.Checkpoint; checkpoint != nil {
		refs = append(refs, checkpoint.DatabaseRefs["OVN_Northbound"], checkpoint.DatabaseRefs["OVN_Southbound"])
	}
	refs = slices.DeleteFunc(refs, func(reference string) bool { return strings.TrimSpace(reference) == "" })
	return slices.Compact(slices.Sorted(slices.Values(refs)))
}

// ensureScheduledBackup captures one verified NB/SB pair only when the
// configured interval has elapsed. Unsupported installation shapes are left
// to their normal reconciliation path; an actually due capture is a safety
// gate and therefore reports adapter or lease failures to status.
func (r *KubeOVNInstallationReconciler) ensureScheduledBackup(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, now time.Time) (scheduledBackupOutcome, error) {
	interval, err := backup.ParseSchedule(installation.Spec.Upgrade.Backup.Schedule)
	if err != nil {
		return scheduledBackupOutcome{Due: true}, err
	}
	if interval == 0 || !localRaft(installation) || installation.Status.ApplyCheckpoint == nil || installation.Status.ObservedVersion == "" || installation.Status.ObservedVersion != installation.Spec.Version || installation.Status.Topology.Inventory == nil {
		return scheduledBackupOutcome{}, nil
	}
	if operation := installation.Status.Topology.Operation; operation != nil && !operation.HandoffApplied {
		return scheduledBackupOutcome{}, nil
	}
	last := installation.Status.LastBackup
	if last != nil && !last.CompletedAt.IsZero() {
		if remaining := last.CompletedAt.Time.Add(interval).Sub(now); remaining > 0 && last.Version == installation.Status.ObservedVersion && last.TopologyDigest == installation.Status.Topology.Inventory.Digest && strings.TrimSpace(last.Destination) == backupDestination(installation) {
			return scheduledBackupOutcome{RequeueAfter: remaining}, nil
		}
	}
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok {
		return scheduledBackupOutcome{Due: true}, errors.New("scheduled backup requires a retained topology provider")
	}
	if r.Leases == nil {
		return scheduledBackupOutcome{Due: true}, errors.New("scheduled backup requires operation leases")
	}
	ref := installation.Status.Topology.Inventory
	if ref.Name == "" || ref.UID == "" || ref.Digest == "" {
		return scheduledBackupOutcome{Due: true}, errors.New("scheduled backup requires a complete retained topology reference")
	}
	binding := lifecycle.BackupBinding{Destination: backupDestination(installation), Version: installation.Status.ObservedVersion, TopologyDigest: ref.Digest}
	capture := r.ScheduledBackup
	if capture == nil {
		if r.Lifecycle == nil {
			return scheduledBackupOutcome{Due: true}, errors.New("scheduled backup requires a lifecycle backup manager")
		}
		capture = r.Lifecycle.CaptureBackupPair
	}
	var captured *operatorv1alpha1.BackupStatus
	err = r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			retained, err := provider.Retained(centralCtx, installation)
			if err != nil {
				return fmt.Errorf("read retained topology for scheduled backup: %w", err)
			}
			if err := retained.Validate(); err != nil {
				return fmt.Errorf("validate retained topology for scheduled backup: %w", err)
			}
			captured, err = capture(centralCtx, installation, retained, binding)
			if err != nil {
				return fmt.Errorf("capture scheduled backup pair: %w", err)
			}
			if err := validateScheduledBackup(captured, binding); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return scheduledBackupOutcome{Due: true}, err
	}
	installation.Status.LastBackup = captured
	return scheduledBackupOutcome{Due: true, RequeueAfter: interval}, nil
}

func validateScheduledBackup(status *operatorv1alpha1.BackupStatus, binding lifecycle.BackupBinding) error {
	if status == nil || status.ID == "" || len(status.DatabaseRefs) != 2 || status.DatabaseRefs["OVN_Northbound"] == "" || status.DatabaseRefs["OVN_Southbound"] == "" || status.CompletedAt.IsZero() {
		return errors.New("scheduled backup returned an incomplete verified pair")
	}
	if !backupPairIDMatches(status) {
		return errors.New("scheduled backup pair ID does not match its database references")
	}
	if strings.TrimSpace(status.Destination) != binding.Destination || status.Version != binding.Version || status.TopologyDigest != binding.TopologyDigest {
		return errors.New("scheduled backup pair does not match the destination, observed version and retained topology")
	}
	return nil
}

func (r *KubeOVNInstallationReconciler) ensureUpgradeBackup(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (bool, error) {
	if !upgradeOnlySpecChange(installation) {
		return false, errors.New("upgrade backup authorization only covers a version-only change; separate member or workload changes require their lifecycle handoff")
	}
	if r.Lifecycle == nil {
		return false, errors.New("upgrade requires a lifecycle backup manager")
	}
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok {
		return false, errors.New("upgrade requires a retained topology provider")
	}
	if r.Leases == nil {
		return false, errors.New("upgrade backup verification requires operation leases")
	}
	ref := installation.Status.Topology.Inventory
	if ref == nil || ref.Name == "" || ref.UID == "" || ref.Digest == "" {
		return false, errors.New("upgrade requires a complete retained topology reference")
	}
	binding := lifecycle.BackupBinding{Destination: backupDestination(installation), Version: installation.Status.ObservedVersion, TopologyDigest: ref.Digest}
	targetVersion := installation.Spec.Version
	ready := false
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			retained, err := provider.Retained(centralCtx, installation)
			if err != nil {
				return fmt.Errorf("verify retained topology before upgrade backup: %w", err)
			}
			checkpoint := installation.Status.Upgrade.Checkpoint
			if upgradeCheckpointMatches(checkpoint, binding, targetVersion) {
				candidate := backupFromUpgradeCheckpoint(checkpoint)
				switch checkpoint.Phase {
				case operatorv1alpha1.UpgradeCheckpointApplying:
					// The target Chart write may have been accepted before the
					// process stopped. Retrying the same target is idempotent and
					// remains bound to this exact pair.
					ready = true
					return nil
				case operatorv1alpha1.UpgradeCheckpointApplied:
					if installation.Status.ObservedVersion == targetVersion {
						return nil
					}
					checkpoint = cloneUpgradeCheckpoint(checkpoint)
					checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointApplying
					checkpoint.Reason = "resuming an accepted upgrade whose target rollout is not yet observed"
					if err := r.persistUpgradeCheckpoint(centralCtx, installation, checkpoint, candidate, false); err != nil {
						return err
					}
					ready = true
					return nil
				case operatorv1alpha1.UpgradeCheckpointVerified:
					if err := r.Lifecycle.VerifyBackupPair(centralCtx, installation, retained, binding, candidate); err != nil {
						failure := r.failUpgradeCheckpoint(centralCtx, installation, fmt.Sprintf("verify retained upgrade backup pair: %v", err))
						return errors.Join(fmt.Errorf("verify retained upgrade backup pair: %w", err), failure)
					}
					if installation.Status.Upgrade.CheckpointAuthorized {
						ready = true
						return nil
					}
					if err := r.persistUpgradeCheckpoint(centralCtx, installation, checkpoint, candidate, true); err != nil {
						return err
					}
					// Persist authorization separately from capture/verification;
					// a restart can never infer it from an incomplete pair.
					return nil
				}
			}

			// Persist the in-flight phase before touching either database. This
			// makes a crash during the sequential native captures visible and
			// prevents a partial pair from becoming an upgrade authorization.
			pending := &operatorv1alpha1.UpgradeCheckpointStatus{
				SourceVersion:  binding.Version,
				TargetVersion:  targetVersion,
				Destination:    binding.Destination,
				TopologyDigest: binding.TopologyDigest,
				Phase:          operatorv1alpha1.UpgradeCheckpointCapturing,
			}
			if err := r.persistUpgradeCheckpoint(centralCtx, installation, pending, nil, false); err != nil {
				return err
			}
			candidate := matchingLastBackup(installation.Status.LastBackup, binding)
			if candidate != nil {
				if err := r.Lifecycle.VerifyBackupPair(centralCtx, installation, retained, binding, candidate); err != nil {
					failure := r.failUpgradeCheckpoint(centralCtx, installation, fmt.Sprintf("verify retained upgrade backup pair: %v", err))
					return errors.Join(fmt.Errorf("verify retained upgrade backup pair: %w", err), failure)
				}
			} else {
				candidate, err = r.Lifecycle.CaptureBackupPair(centralCtx, installation, retained, binding)
				if err != nil {
					failure := r.failUpgradeCheckpoint(centralCtx, installation, fmt.Sprintf("capture upgrade backup pair: %v", err))
					return errors.Join(fmt.Errorf("capture upgrade backup pair: %w", err), failure)
				}
			}
			if err := validateScheduledBackup(candidate, binding); err != nil {
				failure := r.failUpgradeCheckpoint(centralCtx, installation, err.Error())
				return errors.Join(err, failure)
			}
			verified := upgradeCheckpointFromBackup(candidate, targetVersion)
			if err := r.persistUpgradeCheckpoint(centralCtx, installation, verified, candidate, false); err != nil {
				return err
			}
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return ready, nil
}

// upgradeCRDsReady verifies the API server has established every CRD from the
// pinned target manifest before any target workload is rolled. Applying a CRD
// and immediately applying a workload in the same reconciliation would allow
// discovery and admission races to turn a valid upgrade into a partial one.
func (r *KubeOVNInstallationReconciler) upgradeCRDsReady(ctx context.Context, objects []*unstructured.Unstructured) (bool, error) {
	for _, object := range objects {
		if object == nil || object.GetKind() != "CustomResourceDefinition" {
			continue
		}
		crd := new(apiextensionsv1.CustomResourceDefinition)
		if err := r.Get(ctx, client.ObjectKey{Name: object.GetName()}, crd); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("read target CRD %q readiness: %w", object.GetName(), err)
		}
		established, accepted := false, false
		for _, condition := range crd.Status.Conditions {
			switch condition.Type {
			case apiextensionsv1.Established:
				established = condition.Status == apiextensionsv1.ConditionTrue
			case apiextensionsv1.NamesAccepted:
				accepted = condition.Status == apiextensionsv1.ConditionTrue
			}
		}
		if !established || !accepted {
			return false, nil
		}
	}
	return true, nil
}

// upgradeCentralReady gates the data-plane phase on both Kubernetes rollout
// and live central health. A workload observer alone cannot prove that OVN
// databases or northd have accepted the new central image/configuration.
func (r *KubeOVNInstallationReconciler) upgradeCentralReady(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, workload apply.ApplyClient, objects []*unstructured.Unstructured) (bool, error) {
	if len(objects) == 0 {
		return true, nil
	}
	observer, ok := workload.(apply.Observer)
	if !ok {
		return false, errors.New("central-first upgrade requires a workload observer")
	}
	observation, err := observer.Observe(ctx, objects)
	if err != nil {
		return false, fmt.Errorf("observe central rollout: %w", err)
	}
	if !observation.Ready {
		return false, nil
	}
	if r.OVN == nil || r.Runtime == nil {
		return false, errors.New("central-first upgrade requires OVN and runtime readiness providers")
	}
	ovnReport, err := r.OVN.Observe(ctx, installation)
	if err != nil {
		return false, fmt.Errorf("observe OVN readiness after central rollout: %w", err)
	}
	if !ovnReport.Ready {
		return false, nil
	}
	runtimeReport := r.Runtime.Observe(ctx, installation)
	if runtimeReport.Northd.Status != metav1.ConditionTrue {
		return false, nil
	}
	return true, nil
}

func matchingLastBackup(status *operatorv1alpha1.BackupStatus, binding lifecycle.BackupBinding) *operatorv1alpha1.BackupStatus {
	if status == nil || strings.TrimSpace(status.Destination) != binding.Destination || status.Version != binding.Version || status.TopologyDigest != binding.TopologyDigest {
		return nil
	}
	result := new(operatorv1alpha1.BackupStatus)
	status.DeepCopyInto(result)
	return result
}

func upgradeCheckpointFromBackup(status *operatorv1alpha1.BackupStatus, targetVersion string) *operatorv1alpha1.UpgradeCheckpointStatus {
	if status == nil {
		return nil
	}
	return &operatorv1alpha1.UpgradeCheckpointStatus{
		ID: status.ID, DatabaseRefs: maps.Clone(status.DatabaseRefs), SourceVersion: status.Version,
		TargetVersion: targetVersion, Destination: status.Destination, TopologyDigest: status.TopologyDigest,
		CapturedAt: status.CompletedAt, Phase: operatorv1alpha1.UpgradeCheckpointVerified,
	}
}

func backupFromUpgradeCheckpoint(checkpoint *operatorv1alpha1.UpgradeCheckpointStatus) *operatorv1alpha1.BackupStatus {
	if checkpoint == nil {
		return nil
	}
	return &operatorv1alpha1.BackupStatus{
		ID: checkpoint.ID, DatabaseRefs: maps.Clone(checkpoint.DatabaseRefs), Destination: checkpoint.Destination,
		Version: checkpoint.SourceVersion, TopologyDigest: checkpoint.TopologyDigest, CompletedAt: checkpoint.CapturedAt,
	}
}

func cloneUpgradeCheckpoint(checkpoint *operatorv1alpha1.UpgradeCheckpointStatus) *operatorv1alpha1.UpgradeCheckpointStatus {
	if checkpoint == nil {
		return nil
	}
	result := new(operatorv1alpha1.UpgradeCheckpointStatus)
	*result = *checkpoint
	result.DatabaseRefs = maps.Clone(checkpoint.DatabaseRefs)
	return result
}

func upgradeCheckpointMatches(checkpoint *operatorv1alpha1.UpgradeCheckpointStatus, binding lifecycle.BackupBinding, targetVersion string) bool {
	if checkpoint == nil || checkpoint.ID == "" || len(checkpoint.DatabaseRefs) != 2 || checkpoint.SourceVersion != binding.Version || checkpoint.TargetVersion != targetVersion || strings.TrimSpace(checkpoint.Destination) != binding.Destination || checkpoint.TopologyDigest != binding.TopologyDigest || checkpoint.CapturedAt.IsZero() {
		return false
	}
	return (checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointVerified || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplying || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplied) && backupPairIDMatches(&operatorv1alpha1.BackupStatus{ID: checkpoint.ID, DatabaseRefs: checkpoint.DatabaseRefs})
}

func (r *KubeOVNInstallationReconciler) persistUpgradeCheckpoint(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, checkpoint *operatorv1alpha1.UpgradeCheckpointStatus, backup *operatorv1alpha1.BackupStatus, authorized bool) error {
	previous := installation.Status.DeepCopy()
	installation.Status.Upgrade.Checkpoint = cloneUpgradeCheckpoint(checkpoint)
	installation.Status.Upgrade.CheckpointAuthorized = authorized
	installation.Status.Upgrade.BackupID = ""
	installation.Status.Upgrade.TargetVersion = ""
	if checkpoint != nil {
		installation.Status.Upgrade.BackupID = checkpoint.ID
		installation.Status.Upgrade.TargetVersion = checkpoint.TargetVersion
	}
	if backup != nil {
		installation.Status.LastBackup = new(operatorv1alpha1.BackupStatus)
		backup.DeepCopyInto(installation.Status.LastBackup)
	}
	if err := r.Status().Update(ctx, installation); err != nil {
		installation.Status = *previous
		return fmt.Errorf("persist upgrade checkpoint: %w", err)
	}
	return nil
}

func (r *KubeOVNInstallationReconciler) failUpgradeCheckpoint(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, reason string) error {
	checkpoint := cloneUpgradeCheckpoint(installation.Status.Upgrade.Checkpoint)
	if checkpoint == nil {
		checkpoint = &operatorv1alpha1.UpgradeCheckpointStatus{TargetVersion: installation.Spec.Version}
	}
	checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointFailed
	checkpoint.Reason = reason
	return r.persistUpgradeCheckpoint(ctx, installation, checkpoint, nil, false)
}

func upgradeOnlySpecChange(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || installation.Status.ApplyCheckpoint == nil || installation.Status.ObservedVersion == "" || installation.Status.ObservedVersion == installation.Spec.Version {
		return false
	}
	currentJSON, err := stdjson.Marshal(installation.Spec, stdjson.Deterministic(true))
	if err != nil {
		return false
	}
	currentHash := fmt.Sprintf("%x", sha256.Sum256(currentJSON))
	if currentHash == installation.Status.ApplyCheckpoint.SpecHash {
		checkpoint := installation.Status.Upgrade.Checkpoint
		return checkpoint != nil && checkpoint.SourceVersion == installation.Status.ObservedVersion && checkpoint.TargetVersion == installation.Spec.Version && installation.Status.Topology.Inventory != nil && checkpoint.TopologyDigest == installation.Status.Topology.Inventory.Digest
	}
	spec := installation.Spec
	spec.Version = installation.Status.ObservedVersion
	data, err := stdjson.Marshal(spec, stdjson.Deterministic(true))
	return err == nil && fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash
}

func installationDegraded(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil {
		return false
	}
	condition := findInstallationCondition(installation.Status.Conditions, "Degraded")
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func upgradePaused(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || installation.Status.ObservedVersion == "" || installation.Status.ObservedVersion == installation.Spec.Version || !installation.Spec.Upgrade.PauseOnDegraded || !installationDegraded(installation) {
		return false
	}
	direction, err := upgrade.CompareVersions(installation.Status.ObservedVersion, installation.Spec.Version)
	return err == nil && direction == upgrade.VersionUpgrade
}

func (r *KubeOVNInstallationReconciler) reconcileRollback(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now func() time.Time) (reconcile.Result, error) {
	current := now().UTC()
	blocked := func(reason string) (reconcile.Result, error) {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseUpgrading, "Rollback", reason
		installation.Status.Upgrade.Rollback = rollbackBlockedStatus(installation, current, reason)
		installation.Status.Upgrade.CanRollback = false
		if err := r.persistRollbackStatus(ctx, installation, plan, current); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if !installation.Spec.Upgrade.AllowRollback {
		return blocked(fmt.Sprintf("version rollback from %s to %s requires spec.upgrade.allowRollback=true", installation.Status.ObservedVersion, installation.Spec.Version))
	}
	if r.Rollback == nil {
		return blocked(fmt.Sprintf("version rollback from %s to %s is blocked until a version-aware database restore and workload rollback executor is configured", installation.Status.ObservedVersion, installation.Spec.Version))
	}
	if err := r.Rollback.Ready(); err != nil {
		return blocked(fmt.Sprintf("version rollback from %s to %s is not ready: %v", installation.Status.ObservedVersion, installation.Spec.Version, err))
	}
	capabilities, ok := r.Rollback.(rollbackCapabilities)
	if !ok {
		return blocked("version rollback executor does not expose restore, workload and fencing capability evidence")
	}
	restoreReady, workloadReady, fenceReady, capabilityErr := capabilities.Capabilities()
	if capabilityErr != nil {
		return blocked(fmt.Sprintf("version rollback capabilities are unavailable: %v", capabilityErr))
	}
	fenceReady = fenceReady && r.Leases != nil
	rollbackPlan, contract, pair, err := r.rollbackAdmission(installation, current)
	if err != nil {
		return blocked(err.Error())
	}
	contract.RestoreExecutorReady, contract.WorkloadRollbackReady, contract.FenceReady = restoreReady, workloadReady, fenceReady
	if err := contract.Validate(); err != nil {
		return blocked(err.Error())
	}
	rollback := installation.Status.Upgrade.Rollback
	if rollback == nil || rollback.SourceVersion != rollbackPlan.SourceVersion || rollback.TargetVersion != rollbackPlan.TargetVersion || rollback.BackupID != rollbackPlan.BackupID || rollback.TopologyDigest != rollbackPlan.TopologyDigest || rollback.ChartDigest != rollbackPlan.ChartDigest || !sameRecoveryApprovalRef(rollback.ApprovalRef, rollbackPlan.ApprovalRef) || rollback.Phase == operatorv1alpha1.RollbackPhaseBlocked {
		rollback = &operatorv1alpha1.RollbackStatus{Phase: operatorv1alpha1.RollbackPhaseQuiescing, SourceVersion: rollbackPlan.SourceVersion, TargetVersion: rollbackPlan.TargetVersion, BackupID: rollbackPlan.BackupID, TopologyDigest: rollbackPlan.TopologyDigest, ChartDigest: rollbackPlan.ChartDigest, ApprovalRef: rollbackPlan.ApprovalRef, StartedAt: metav1.NewTime(current), RPOSeconds: maxDurationSeconds(current.Sub(pair.CompletedAt.Time))}
	}
	if rollback.Phase == operatorv1alpha1.RollbackPhaseFailed {
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if rollback.Phase == operatorv1alpha1.RollbackPhaseSucceeded {
		return reconcile.Result{}, nil
	}
	installation.Status.Upgrade.Rollback = rollback
	installation.Status.Upgrade.CanRollback = true
	if err := r.persistRollbackStatus(ctx, installation, planner.Plan{Phase: operatorv1alpha1.PhaseUpgrading, Step: "Rollback"}, current); err != nil {
		return reconcile.Result{}, err
	}
	rollback = installation.Status.Upgrade.Rollback
	next, stepErr := r.runRollbackStep(ctx, installation, rollbackPlan, rollback.Phase)
	if stepErr != nil {
		if errors.Is(stepErr, ErrRollbackInProgress) {
			rollback.Reason = "rollback operation accepted; waiting for workload and OVN verification"
			if err := r.persistRollbackStatus(ctx, installation, planner.Plan{Phase: operatorv1alpha1.PhaseUpgrading, Step: "Rollback"}, current); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
		rollback.Phase, rollback.Reason = operatorv1alpha1.RollbackPhaseFailed, stepErr.Error()
		installation.Status.Upgrade.CanRollback = false
		if err := r.persistRollbackStatus(ctx, installation, planner.Plan{Phase: operatorv1alpha1.PhaseUpgrading, Step: "Rollback", Blocking: stepErr.Error()}, current); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	rollback.Phase = next
	rollback.Reason = "rollback phase advanced"
	if next == operatorv1alpha1.RollbackPhaseSucceeded {
		rollback.CompletedAt = metav1.NewTime(current)
		rollback.RTOSeconds = maxDurationSeconds(current.Sub(rollback.StartedAt.Time))
		rollback.Reason = "target Chart, both databases and verification gates converged"
		installation.Status.ObservedVersion = rollback.TargetVersion
		installation.Status.ApplyCheckpoint = nil
		installation.Status.Upgrade.CanRollback = false
	}
	if err := r.persistRollbackStatus(ctx, installation, planner.Plan{Phase: operatorv1alpha1.PhaseUpgrading, Step: "Rollback"}, current); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *KubeOVNInstallationReconciler) rollbackAdmission(installation *operatorv1alpha1.KubeOVNInstallation, now time.Time) (RollbackPlan, upgrade.RollbackContract, *operatorv1alpha1.BackupStatus, error) {
	checkpoint := installation.Status.Upgrade.Checkpoint
	if checkpoint == nil || checkpoint.SourceVersion != installation.Spec.Version || checkpoint.TargetVersion != installation.Status.ObservedVersion || checkpoint.ID == "" || len(checkpoint.DatabaseRefs) != 2 || checkpoint.DatabaseRefs["OVN_Northbound"] == "" || checkpoint.DatabaseRefs["OVN_Southbound"] == "" || checkpoint.TopologyDigest == "" || checkpoint.CapturedAt.IsZero() || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointCapturing || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointFailed {
		return RollbackPlan{}, upgrade.RollbackContract{}, nil, fmt.Errorf("version rollback from %s to %s requires a verified target-version NB/SB backup pair", installation.Status.ObservedVersion, installation.Spec.Version)
	}
	pair := backupFromUpgradeCheckpoint(checkpoint)
	if pair == nil || !backupPairIDMatches(pair) {
		return RollbackPlan{}, upgrade.RollbackContract{}, nil, errors.New("version rollback backup checkpoint is incomplete")
	}
	plan := RollbackPlan{InstallationNamespace: installation.Namespace, InstallationName: installation.Name, InstallationUID: installation.UID, InstallationGeneration: installation.Generation, SourceVersion: installation.Status.ObservedVersion, TargetVersion: installation.Spec.Version, BackupID: pair.ID, DatabaseRefs: maps.Clone(pair.DatabaseRefs), Destination: pair.Destination, TopologyDigest: pair.TopologyDigest, ChartDigest: installation.Spec.Chart.Digest, ApprovalRef: installation.Spec.Upgrade.RollbackApprovalRef, Mode: "rollback"}
	operationToken, tokenErr := rollbackOperationToken(plan)
	if tokenErr != nil {
		return RollbackPlan{}, upgrade.RollbackContract{}, nil, tokenErr
	}
	plan.OperationToken = operationToken
	contract := upgrade.RollbackContract{FromVersion: plan.SourceVersion, ToVersion: plan.TargetVersion, FromCentralMode: string(installation.Spec.Central.Mode), ToCentralMode: string(installation.Spec.Central.Mode), AllowRollback: installation.Spec.Upgrade.AllowRollback, BackupID: plan.BackupID, DatabaseRefs: maps.Clone(plan.DatabaseRefs), TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest}
	if pair.CompletedAt.After(now) {
		return RollbackPlan{}, upgrade.RollbackContract{}, nil, errors.New("version rollback backup checkpoint has a future capture time")
	}
	return plan, contract, pair, nil
}

func (r *KubeOVNInstallationReconciler) runRollbackStep(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan RollbackPlan, phase operatorv1alpha1.RollbackPhase) (operatorv1alpha1.RollbackPhase, error) {
	if r.Leases == nil {
		return phase, errors.New("rollback requires installation and central operation leases")
	}
	var next operatorv1alpha1.RollbackPhase
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			var err error
			next, err = r.Rollback.Step(centralCtx, plan, phase)
			return err
		})
	})
	return next, err
}

func (r *KubeOVNInstallationReconciler) persistRollbackStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time) error {
	status := installation.Status.DeepCopy()
	status.Phase = plan.Phase
	status.Upgrade.Step = plan.Step
	status.ObservedGeneration = installation.Generation
	if status.Upgrade.Rollback != nil {
		status.Upgrade.Rollback.Reason = strings.TrimSpace(status.Upgrade.Rollback.Reason)
	}
	message := "version rollback is gated by a durable target-version backup pair and fenced execution"
	if plan.Blocking != "" {
		message = plan.Blocking
	}
	setCondition(&status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "RollbackInProgress", Message: message, ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
	setCondition(&status.Conditions, metav1.Condition{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "RollbackGated", Message: message, ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
	updated := installationWithStatus(installation, *status)
	if err := r.Status().Update(ctx, updated); err != nil {
		return err
	}
	installation.ResourceVersion = updated.ResourceVersion
	installation.Status = updated.Status
	return nil
}

func maxDurationSeconds(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return int64(duration / time.Second)
}

func rollbackBlockedStatus(installation *operatorv1alpha1.KubeOVNInstallation, now time.Time, reason string) *operatorv1alpha1.RollbackStatus {
	status := &operatorv1alpha1.RollbackStatus{
		Phase:         operatorv1alpha1.RollbackPhaseBlocked,
		SourceVersion: installation.Status.ObservedVersion,
		TargetVersion: installation.Spec.Version,
		ApprovalRef:   installation.Spec.Upgrade.RollbackApprovalRef,
		Reason:        reason,
	}
	backup := installation.Status.LastBackup
	checkpoint := installation.Status.Upgrade.Checkpoint
	if checkpoint != nil && checkpoint.SourceVersion == installation.Spec.Version && checkpoint.TargetVersion == status.SourceVersion && rollbackCheckpointUsable(checkpoint.Phase) && !checkpoint.CapturedAt.IsZero() {
		backup = backupFromUpgradeCheckpoint(checkpoint)
	}
	if backup == nil || !backupPairIDMatches(backup) || backup.Version == "" || backup.Version != installation.Spec.Version && backup.Version != status.SourceVersion || backup.TopologyDigest == "" || installation.Status.Topology.Inventory == nil || backup.TopologyDigest != installation.Status.Topology.Inventory.Digest {
		return status
	}
	status.BackupID = backup.ID
	status.TopologyDigest = backup.TopologyDigest
	status.ChartDigest = installation.Spec.Chart.Digest
	if !backup.CompletedAt.IsZero() {
		age := now.Sub(backup.CompletedAt.Time)
		if age > 0 {
			status.RPOSeconds = int64(age / time.Second)
		}
	}
	return status
}

func (r *KubeOVNInstallationReconciler) ensureApplyCheckpoint(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, rendered chart.Rendered) error {
	specHash, baseSpecHash, err := installationSpecHashes(installation.Spec)
	if err != nil {
		return err
	}
	manifestJSON, err := stdjson.Marshal(rendered.Objects, stdjson.Deterministic(true))
	if err != nil {
		return err
	}
	checkpoint := operatorv1alpha1.ApplyCheckpointStatus{
		SpecHash:     specHash,
		BaseSpecHash: baseSpecHash,
		ManifestHash: fmt.Sprintf("%x", sha256.Sum256(manifestJSON)),
		Version:      installation.Spec.Version,
	}
	if existing := installation.Status.ApplyCheckpoint; existing != nil {
		if *existing != checkpoint {
			// Older checkpoints predate BaseSpecHash. Add the derived field in
			// place when every existing input still matches, so a restart does
			// not turn this compatibility migration into an unauthorized apply.
			if existing.BaseSpecHash == "" && existing.SpecHash == checkpoint.SpecHash && existing.ManifestHash == checkpoint.ManifestHash && existing.Version == checkpoint.Version {
				previous := *existing
				installation.Status.ApplyCheckpoint = &checkpoint
				updated := installation.DeepCopy()
				if err := r.Status().Update(ctx, updated); err != nil {
					installation.Status.ApplyCheckpoint = &previous
					return fmt.Errorf("persist apply checkpoint baseline: %w", err)
				}
				installation.ResourceVersion = updated.ResourceVersion
				installation.Status = updated.Status
				return nil
			}
			if !lifecycleHandoffAuthorizesApply(installation) && !upgradeAuthorizesApply(installation) {
				return fmt.Errorf("installation inputs changed after the first apply checkpoint; a lifecycle executor is required")
			}
			previous := *existing
			installation.Status.ApplyCheckpoint = &checkpoint
			updated := installation.DeepCopy()
			if err := r.Status().Update(ctx, updated); err != nil {
				installation.Status.ApplyCheckpoint = &previous
				return fmt.Errorf("persist lifecycle-authorized apply checkpoint: %w", err)
			}
			installation.ResourceVersion = updated.ResourceVersion
			installation.Status = updated.Status
			return nil
		}
		return nil
	}
	installation.Status.ApplyCheckpoint = &checkpoint
	// Persist before any workload write so a restart or partial apply cannot
	// turn a topology/version change into a fresh installation.
	updated := installation.DeepCopy()
	if err := r.Status().Update(ctx, updated); err != nil {
		installation.Status.ApplyCheckpoint = nil
		return fmt.Errorf("persist apply checkpoint: %w", err)
	}
	installation.ResourceVersion = updated.ResourceVersion
	installation.Status = updated.Status
	return nil
}

func (r *KubeOVNInstallationReconciler) markLifecycleHandoffApplied(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) error {
	if !lifecycleHandoffAuthorizesApply(installation) || installation.Status.Topology.Operation == nil {
		return nil
	}
	updated := installation.DeepCopy()
	updated.Status.Topology.Operation.HandoffApplied = true
	if err := r.Status().Update(ctx, updated); err != nil {
		return fmt.Errorf("persist lifecycle handoff consumption: %w", err)
	}
	installation.ResourceVersion = updated.ResourceVersion
	installation.Status = updated.Status
	return nil
}

func (r *KubeOVNInstallationReconciler) beginUpgradeApply(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) error {
	checkpoint := cloneUpgradeCheckpoint(installation.Status.Upgrade.Checkpoint)
	if checkpoint == nil || !upgradeCheckpointMatches(checkpoint, lifecycle.BackupBinding{Destination: checkpoint.Destination, Version: checkpoint.SourceVersion, TopologyDigest: checkpoint.TopologyDigest}, checkpoint.TargetVersion) {
		return errors.New("upgrade apply requires a complete durable checkpoint")
	}
	checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointApplying
	checkpoint.Reason = "Chart apply is authorized by the verified NB/SB pair"
	return r.persistUpgradeCheckpoint(ctx, installation, checkpoint, backupFromUpgradeCheckpoint(checkpoint), false)
}

func (r *KubeOVNInstallationReconciler) markUpgradeCheckpointApplied(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, now time.Time) error {
	checkpoint := cloneUpgradeCheckpoint(installation.Status.Upgrade.Checkpoint)
	if checkpoint == nil || checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointApplying {
		return errors.New("upgrade apply did not have an Applying checkpoint")
	}
	checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointApplied
	checkpoint.Reason = "Chart write accepted; target rollout remains subject to observation"
	checkpoint.AppliedAt = metav1.NewTime(now.UTC())
	return r.persistUpgradeCheckpoint(ctx, installation, checkpoint, backupFromUpgradeCheckpoint(checkpoint), false)
}

func (r *KubeOVNInstallationReconciler) failUpgradeApply(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, reason string) error {
	checkpoint := cloneUpgradeCheckpoint(installation.Status.Upgrade.Checkpoint)
	if checkpoint == nil {
		return errors.New("cannot record upgrade apply failure without a checkpoint")
	}
	checkpoint.Phase = operatorv1alpha1.UpgradeCheckpointFailed
	checkpoint.Reason = reason
	return r.persistUpgradeCheckpoint(ctx, installation, checkpoint, backupFromUpgradeCheckpoint(checkpoint), false)
}

func lifecycleHandoffAuthorizesApply(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || installation.Status.Topology.Operation == nil || installation.Status.Topology.Operation.HandoffApplied || installation.Status.Topology.Operation.CompletionDigest == "" || installation.Status.Topology.Operation.HandoffInventory == nil || installation.Status.Topology.Inventory == nil {
		return false
	}
	return *installation.Status.Topology.Operation.HandoffInventory == *installation.Status.Topology.Inventory
}

func upgradeAuthorizesApply(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || installation.Status.ApplyCheckpoint == nil || installation.Status.ObservedVersion == "" || installation.Status.ObservedVersion == installation.Spec.Version || !upgradeOnlySpecChange(installation) {
		return false
	}
	ref := installation.Status.Topology.Inventory
	if ref == nil {
		return false
	}
	binding := lifecycle.BackupBinding{Destination: backupDestination(installation), Version: installation.Status.ObservedVersion, TopologyDigest: ref.Digest}
	checkpoint := installation.Status.Upgrade.Checkpoint
	if !upgradeCheckpointMatches(checkpoint, binding, installation.Spec.Version) || checkpoint.ID == "" || len(checkpoint.DatabaseRefs) != 2 {
		return false
	}
	backup := backupFromUpgradeCheckpoint(checkpoint)
	if validateScheduledBackup(backup, binding) != nil || (installation.Status.ApplyCheckpoint.Version != installation.Status.ObservedVersion && installation.Status.ApplyCheckpoint.Version != installation.Spec.Version) {
		return false
	}
	return checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplying || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointVerified && installation.Status.Upgrade.CheckpointAuthorized
}

func backupDestination(installation *operatorv1alpha1.KubeOVNInstallation) string {
	if installation == nil {
		return ""
	}
	return strings.TrimSpace(installation.Spec.Upgrade.Backup.Destination)
}

func installationWithStatus(in *operatorv1alpha1.KubeOVNInstallation, status operatorv1alpha1.KubeOVNInstallationStatus) *operatorv1alpha1.KubeOVNInstallation {
	copy := in.DeepCopy()
	copy.Status = status
	return copy
}

func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	for index := range *conditions {
		if (*conditions)[index].Type == condition.Type {
			if (*conditions)[index].Status == condition.Status && (*conditions)[index].Reason == condition.Reason && (*conditions)[index].Message == condition.Message {
				condition.LastTransitionTime = (*conditions)[index].LastTransitionTime
			}
			(*conditions)[index] = condition
			return
		}
	}
	*conditions = append(*conditions, condition)
}

// RecoveryPlan is the immutable, identity-bound input supplied to an explicit
// recovery executor. The reconciler never constructs one from an approver
// string alone and never performs a database wipe itself.
type RecoveryPlan struct {
	RequestNamespace       string
	RequestName            string
	RequestUID             types.UID
	InstallationNamespace  string
	InstallationName       string
	InstallationUID        types.UID
	InstallationGeneration int64
	RequestGeneration      int64
	Reason                 string
	Mode                   operatorv1alpha1.RecoveryMode
	SourceVersion          string
	TargetVersion          string
	ChartDigest            string
	TargetMemberRef        string
	TargetMemberDigest     string
	TopologyDigest         string
	BackupRef              string
	DatabaseRefs           map[string]string
	ApprovalRef            *operatorv1alpha1.RecoveryApprovalReference
	Approver               string
	ExpiresAt              time.Time
	Digest                 string
}

// RecoveryExecutor is an explicit integration boundary for central recovery.
// The normal operator injects the non-destructive rejoin adapter; plan-only
// mode leaves it nil, so requests are validated and durably queued without a
// database or storage mutation.
type RecoveryExecutor interface {
	Execute(context.Context, RecoveryPlan) error
}

type RecoveryRequestReconciler struct {
	client.Client
	Scheme   *apiruntime.Scheme
	Clock    func() time.Time
	Executor RecoveryExecutor
}

func (r *RecoveryRequestReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).For(&operatorv1alpha1.KubeOVNRecoveryRequest{}).Complete(r)
}

func (r *RecoveryRequestReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := r.Get(ctx, req.NamespacedName, request); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !request.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	now := time.Now().UTC()
	if r.Clock != nil {
		now = r.Clock().UTC()
	}
	if request.Status.Phase == operatorv1alpha1.RecoveryPhaseSucceeded || request.Status.Phase == operatorv1alpha1.RecoveryPhaseFailed || request.Status.Phase == operatorv1alpha1.RecoveryPhaseExpired {
		return reconcile.Result{}, nil
	}
	installationName := strings.TrimSpace(request.Spec.InstallationRef)
	if installationName == "" {
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, "InvalidRequest", "spec.installationRef is required", now, false)
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := r.Get(ctx, client.ObjectKey{Namespace: request.Namespace, Name: installationName}, installation); err != nil {
		reason := "InstallationLookupFailed"
		message := fmt.Sprintf("cannot read referenced installation: %v", err)
		if apierrors.IsNotFound(err) {
			reason = "InstallationNotFound"
			message = fmt.Sprintf("referenced installation %q was not found", installationName)
		}
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, reason, message, now, false)
	}
	if request.Status.Plan != nil && request.Status.Plan.RequestGeneration != request.Generation {
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, "PlanChanged", "RecoveryRequest spec changed after its immutable plan was recorded", now, false)
	}
	plan, reason, err := buildRecoveryPlan(request, installation, now)
	if err != nil {
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, reason, err.Error(), now, reason == "Expired")
	}
	planStatus := recoveryPlanStatus(plan)
	if request.Status.Plan != nil && !sameRecoveryPlan(request.Status.Plan, planStatus) {
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, "PlanChanged", "installation identity or RecoveryRequest inputs changed after the immutable plan was recorded", now, false)
	}
	if request.Status.Plan == nil {
		request.Status.Plan = planStatus
	}
	if r.Executor == nil {
		if err := r.setRecoveryAwaiting(ctx, request, now); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{}, nil
	}
	if request.Status.Phase != operatorv1alpha1.RecoveryPhaseExecuting {
		if err := r.setRecoveryExecuting(ctx, request, now); err != nil {
			return reconcile.Result{}, err
		}
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	if err := r.Executor.Execute(ctx, plan); err != nil {
		// A destructive executor persists its phase from a fresh object so a
		// restart can resume it. Refresh before writing the outer request phase
		// to avoid turning that status update into a stale-resource conflict.
		latest := new(operatorv1alpha1.KubeOVNRecoveryRequest)
		if refreshErr := r.Get(ctx, client.ObjectKeyFromObject(request), latest); refreshErr == nil {
			request = latest
		}
		if errors.Is(err, ErrRecoveryInProgress) {
			if progressErr := r.setRecoveryInProgress(ctx, request, now); progressErr != nil {
				return reconcile.Result{}, progressErr
			}
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, "ExecutionFailed", err.Error(), now, false)
	}
	latest := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if refreshErr := r.Get(ctx, client.ObjectKeyFromObject(request), latest); refreshErr == nil {
		request = latest
	}
	completed := metav1.NewTime(now)
	request.Status.Phase = operatorv1alpha1.RecoveryPhaseSucceeded
	request.Status.CompletedAt = &completed
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "Accepted", metav1.ConditionTrue, "ExecutionComplete", "recovery executor completed the explicitly admitted plan", now))
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "ExecutionReady", metav1.ConditionTrue, "ExecutionComplete", "recovery executor completed the explicitly admitted plan", now))
	if err := r.updateRecoveryStatus(ctx, request); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func buildRecoveryPlan(request *operatorv1alpha1.KubeOVNRecoveryRequest, installation *operatorv1alpha1.KubeOVNInstallation, now time.Time) (RecoveryPlan, string, error) {
	if request == nil || installation == nil {
		return RecoveryPlan{}, "InvalidRequest", errors.New("recovery plan requires a request and installation")
	}
	if request.Name == "" || request.Namespace == "" || request.UID == "" || request.Generation < 1 {
		return RecoveryPlan{}, "InvalidRequest", errors.New("recovery request requires a stable UID, namespace, name and generation")
	}
	if installation.UID == "" || installation.Generation < 1 {
		return RecoveryPlan{}, "InstallationIdentityInvalid", errors.New("referenced installation has no stable UID or generation")
	}
	if request.Namespace != installation.Namespace || strings.TrimSpace(request.Spec.InstallationRef) != installation.Name {
		return RecoveryPlan{}, "InstallationMismatch", errors.New("recovery request does not reference the installation in its namespace")
	}
	reason := strings.TrimSpace(request.Spec.Reason)
	if reason == "" || len(reason) > 1024 {
		return RecoveryPlan{}, "InvalidRequest", errors.New("spec.reason must contain 1 to 1024 non-space characters")
	}
	approver := strings.TrimSpace(request.Spec.ApprovedBy)
	if approver == "" || len(approver) > 256 {
		return RecoveryPlan{}, "InvalidRequest", errors.New("spec.approvedBy must contain 1 to 256 non-space characters")
	}
	targetMemberRef, targetMemberDigest, topologyDigest, err := recoveryTarget(request, installation)
	if err != nil {
		return RecoveryPlan{}, "TargetInvalid", err
	}
	switch request.Spec.Mode {
	case operatorv1alpha1.RecoveryModeRejoin:
		if targetMemberRef == "" {
			return RecoveryPlan{}, "TargetRequired", errors.New("rejoin requires spec.targetMemberRef naming an explicit central.memberRequests entry")
		}
		if request.Spec.ApprovalRef != nil {
			return RecoveryPlan{}, "ApprovalUnsupported", errors.New("rejoin recovery does not accept a destructive recovery approval")
		}
	case operatorv1alpha1.RecoveryModeReconvert, operatorv1alpha1.RecoveryModeFreshBootstrap:
		if strings.TrimSpace(request.Spec.BackupRef) == "" {
			return RecoveryPlan{}, "BackupRequired", fmt.Errorf("mode %q requires spec.backupRef bound to a verified backup pair", request.Spec.Mode)
		}
	default:
		return RecoveryPlan{}, "InvalidRequest", fmt.Errorf("unsupported recovery mode %q", request.Spec.Mode)
	}
	expiresAt := request.Spec.ExpiresAt.UTC()
	if expiresAt.IsZero() || !expiresAt.After(now) {
		return RecoveryPlan{}, "Expired", errors.New("recovery request has expired")
	}
	backupRef := strings.TrimSpace(request.Spec.BackupRef)
	var verifiedBackup *operatorv1alpha1.BackupStatus
	if backupRef != "" {
		verifiedBackup = verifiedRecoveryBackup(installation, backupRef)
		if verifiedBackup == nil {
			return RecoveryPlan{}, "BackupMismatch", errors.New("backupRef does not match the installation's current verified backup pair")
		}
	}
	if installation.Spec.HCP.Enabled {
		return RecoveryPlan{}, "HCPUnsupported", errors.New("local recovery requests cannot target an HCP installation")
	}
	plan := RecoveryPlan{
		RequestNamespace: request.Namespace, RequestName: request.Name, RequestUID: request.UID,
		InstallationNamespace: installation.Namespace, InstallationName: installation.Name, InstallationUID: installation.UID,
		InstallationGeneration: installation.Generation, RequestGeneration: request.Generation,
		Reason: reason, Mode: request.Spec.Mode, SourceVersion: installation.Status.ObservedVersion, TargetVersion: installation.Spec.Version,
		ChartDigest: installation.Spec.Chart.Digest, TargetMemberRef: targetMemberRef, TargetMemberDigest: targetMemberDigest,
		TopologyDigest: topologyDigest, BackupRef: backupRef, ApprovalRef: request.Spec.ApprovalRef, Approver: approver, ExpiresAt: expiresAt,
	}
	if verifiedBackup != nil {
		plan.DatabaseRefs = maps.Clone(verifiedBackup.DatabaseRefs)
	}
	plan.Digest = recoveryPlanDigest(plan)
	return plan, "", nil
}

// recoveryTarget resolves the explicit member slot at admission time. A
// digest is retained in the immutable plan so a later edit cannot redirect an
// executor to a different Node, storage identity or endpoint.
func recoveryTarget(request *operatorv1alpha1.KubeOVNRecoveryRequest, installation *operatorv1alpha1.KubeOVNInstallation) (string, string, string, error) {
	if request == nil || installation == nil {
		return "", "", "", errors.New("recovery target requires a request and installation")
	}
	ref := strings.TrimSpace(request.Spec.TargetMemberRef)
	if ref == "" {
		if request.Spec.Mode == operatorv1alpha1.RecoveryModeReconvert || request.Spec.Mode == operatorv1alpha1.RecoveryModeFreshBootstrap {
			if installation.Status.Topology.Inventory == nil || installation.Status.Topology.Inventory.Digest == "" {
				return "", "", "", errors.New("destructive recovery requires a retained topology binding")
			}
			return "", "", installation.Status.Topology.Inventory.Digest, nil
		}
		return "", "", "", nil
	}
	if request.Spec.Mode != operatorv1alpha1.RecoveryModeRejoin {
		return "", "", "", errors.New("spec.targetMemberRef is only supported for rejoin recovery")
	}
	if installation.Spec.Central.Mode != operatorv1alpha1.CentralModeRaft || installation.Spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly || !installation.Spec.Central.DynamicMembership {
		return "", "", "", errors.New("rejoin target requires a local raft installation with dynamic membership enabled")
	}
	retained := installation.Status.Topology.Inventory
	if retained == nil || retained.Digest == "" {
		return "", "", "", errors.New("rejoin target requires a retained central topology")
	}
	var target *operatorv1alpha1.CentralMemberSpec
	for index := range installation.Spec.Central.MemberRequests {
		member := &installation.Spec.Central.MemberRequests[index]
		if member.Name == ref {
			if target != nil {
				return "", "", "", fmt.Errorf("rejoin target member %q is ambiguous", ref)
			}
			target = member
		}
	}
	if target == nil {
		return "", "", "", fmt.Errorf("rejoin target member %q is not present in central.memberRequests", ref)
	}
	if target.RetireEndpoint != "" {
		return "", "", "", errors.New("rejoin target cannot be a retirement-only member request")
	}
	if err := validateRecoveryTargetMember(*target); err != nil {
		return "", "", "", fmt.Errorf("validate rejoin target %q: %w", ref, err)
	}
	data, err := stdjson.Marshal(*target, stdjson.Deterministic(true))
	if err != nil {
		return "", "", "", fmt.Errorf("marshal rejoin target %q: %w", ref, err)
	}
	return ref, fmt.Sprintf("sha256:%x", sha256.Sum256(data)), retained.Digest, nil
}

func validateRecoveryTargetMember(member operatorv1alpha1.CentralMemberSpec) error {
	if len(kvalidation.IsDNS1123Label(member.Name)) != 0 {
		return errors.New("target member name must be a DNS label")
	}
	if member.ReplaceEndpoint != "" && len(kvalidation.IsDNS1123Subdomain(member.ReplaceEndpoint)) != 0 {
		return errors.New("target member replaceEndpoint must be a DNS subdomain")
	}
	if member.RetireEndpoint != "" && len(kvalidation.IsDNS1123Subdomain(member.RetireEndpoint)) != 0 {
		return errors.New("target member retireEndpoint must be a DNS subdomain")
	}
	if member.ReplaceEndpoint != "" && member.RetireEndpoint != "" {
		return errors.New("target member cannot set both replaceEndpoint and retireEndpoint")
	}
	for _, value := range []string{member.NodeName, member.ClaimName, member.VolumeName, member.Image, member.NorthboundAddress, member.SouthboundAddress, member.NorthboundClientAddress, member.SouthboundClientAddress} {
		if strings.TrimSpace(value) == "" {
			return errors.New("target member requires node, storage, image and all NB/SB addresses")
		}
	}
	image, digest, found := strings.Cut(member.Image, "@sha256:")
	if !found || image == "" || len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" {
		return errors.New("target member image must be pinned to a sha256 digest")
	}
	if err := validateMemberTLSContract(member); err != nil {
		return err
	}
	return nil
}

func validateMemberTLSContract(member operatorv1alpha1.CentralMemberSpec) error {
	if member.TLSSecretRef != nil && len(kvalidation.IsDNS1123Subdomain(member.TLSSecretRef.Name)) != 0 {
		return errors.New("target member tlsSecretRef.name must be a DNS subdomain")
	}
	protocol := ""
	for _, address := range []string{member.NorthboundAddress, member.SouthboundAddress, member.NorthboundClientAddress, member.SouthboundClientAddress} {
		current, endpoint, found := strings.Cut(address, ":")
		if !found || endpoint == "" || current != "tcp" && current != "ssl" {
			return errors.New("target member NB/SB addresses must use tcp or ssl")
		}
		if protocol == "" {
			protocol = current
		} else if protocol != current {
			return errors.New("target member NB/SB addresses must use one protocol")
		}
	}
	if (protocol == "ssl") != (member.TLSSecretRef != nil) {
		return errors.New("target member listener protocol and tlsSecretRef must agree")
	}
	return nil
}

func verifiedRecoveryBackup(installation *operatorv1alpha1.KubeOVNInstallation, reference string) *operatorv1alpha1.BackupStatus {
	if installation == nil || strings.TrimSpace(reference) == "" || installation.Status.ObservedVersion == "" || installation.Status.Topology.Inventory == nil || installation.Status.Topology.Inventory.Digest == "" {
		return nil
	}
	candidates := []*operatorv1alpha1.BackupStatus{installation.Status.LastBackup}
	checkpoint := installation.Status.Upgrade.Checkpoint
	if checkpoint != nil && (checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointVerified || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplying || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplied) {
		candidates = append(candidates, backupFromUpgradeCheckpoint(checkpoint))
	}
	for _, backup := range candidates {
		if backup == nil || backup.ID != reference || backup.Version == "" || backup.Version != installation.Status.ObservedVersion || strings.TrimSpace(backup.Destination) != backupDestination(installation) || backup.TopologyDigest != installation.Status.Topology.Inventory.Digest || backup.CompletedAt.IsZero() {
			continue
		}
		if !backupPairIDMatches(backup) {
			continue
		}
		copy := new(operatorv1alpha1.BackupStatus)
		backup.DeepCopyInto(copy)
		return copy
	}
	return nil
}

// backupPairIDMatches verifies the content-addressed binding between the
// durable pair ID and both database references before a recovery plan can use
// the pair. Status fields are untrusted input after an API-server restore or
// manual edit; non-empty references alone do not prove they belong to the
// recorded pair.
func backupPairIDMatches(status *operatorv1alpha1.BackupStatus) bool {
	if status == nil || len(status.DatabaseRefs) != 2 || status.DatabaseRefs["OVN_Northbound"] == "" || status.DatabaseRefs["OVN_Southbound"] == "" {
		return false
	}
	refs, err := stdjson.Marshal(status.DatabaseRefs, stdjson.Deterministic(true))
	return err == nil && status.ID == fmt.Sprintf("sha256:%x", sha256.Sum256(refs))
}

func matchesVerifiedBackup(installation *operatorv1alpha1.KubeOVNInstallation, reference string) bool {
	return verifiedRecoveryBackup(installation, reference) != nil
}

func recoveryPlanDigest(plan RecoveryPlan) string {
	data, err := stdjson.Marshal(struct {
		RequestNamespace       string
		RequestName            string
		RequestUID             types.UID
		InstallationNamespace  string
		InstallationName       string
		InstallationUID        types.UID
		InstallationGeneration int64
		RequestGeneration      int64
		Reason                 string
		Mode                   operatorv1alpha1.RecoveryMode
		SourceVersion          string
		TargetVersion          string
		ChartDigest            string
		TargetMemberRef        string
		TargetMemberDigest     string
		TopologyDigest         string
		BackupRef              string
		DatabaseRefs           map[string]string
		ApprovalRef            *operatorv1alpha1.RecoveryApprovalReference
		Approver               string
		ExpiresAt              string
	}{
		RequestNamespace: plan.RequestNamespace, RequestName: plan.RequestName, RequestUID: plan.RequestUID,
		InstallationNamespace: plan.InstallationNamespace, InstallationName: plan.InstallationName, InstallationUID: plan.InstallationUID,
		InstallationGeneration: plan.InstallationGeneration, RequestGeneration: plan.RequestGeneration,
		Reason: plan.Reason, Mode: plan.Mode, SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, ChartDigest: plan.ChartDigest,
		TargetMemberRef: plan.TargetMemberRef, TargetMemberDigest: plan.TargetMemberDigest, TopologyDigest: plan.TopologyDigest,
		BackupRef: plan.BackupRef, DatabaseRefs: plan.DatabaseRefs, ApprovalRef: plan.ApprovalRef, Approver: plan.Approver,
		ExpiresAt: plan.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, stdjson.Deterministic(true))
	if err != nil {
		return ""
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func recoveryPlanStatus(plan RecoveryPlan) *operatorv1alpha1.RecoveryPlanStatus {
	return &operatorv1alpha1.RecoveryPlanStatus{Digest: plan.Digest, InstallationUID: plan.InstallationUID, InstallationGeneration: plan.InstallationGeneration, RequestGeneration: plan.RequestGeneration, Mode: plan.Mode, SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, ChartDigest: plan.ChartDigest, TargetMemberRef: plan.TargetMemberRef, TargetMemberDigest: plan.TargetMemberDigest, TopologyDigest: plan.TopologyDigest, BackupRef: plan.BackupRef, DatabaseRefs: maps.Clone(plan.DatabaseRefs), ApprovalRef: plan.ApprovalRef, ExpiresAt: metav1.NewTime(plan.ExpiresAt)}
}

func sameRecoveryPlan(left, right *operatorv1alpha1.RecoveryPlanStatus) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Digest == right.Digest && left.InstallationUID == right.InstallationUID && left.InstallationGeneration == right.InstallationGeneration && left.RequestGeneration == right.RequestGeneration && left.Mode == right.Mode && left.SourceVersion == right.SourceVersion && left.TargetVersion == right.TargetVersion && left.ChartDigest == right.ChartDigest && left.TargetMemberRef == right.TargetMemberRef && left.TargetMemberDigest == right.TargetMemberDigest && left.TopologyDigest == right.TopologyDigest && left.BackupRef == right.BackupRef && maps.Equal(left.DatabaseRefs, right.DatabaseRefs) && sameRecoveryApprovalRef(left.ApprovalRef, right.ApprovalRef) && left.ExpiresAt.Time.Equal(right.ExpiresAt.Time)
}

func sameRecoveryApprovalRef(left, right *operatorv1alpha1.RecoveryApprovalReference) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func recoveryCondition(generation int64, conditionType string, status metav1.ConditionStatus, reason, message string, now time.Time) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(now)}
}

func (r *RecoveryRequestReconciler) setRecoveryAwaiting(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, now time.Time) error {
	request.Status.Phase = operatorv1alpha1.RecoveryPhaseAwaitingExecution
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "Accepted", metav1.ConditionTrue, "PlanValidated", "recovery plan is identity-bound; approvedBy is self-reported and does not authorize execution", now))
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "ExecutionReady", metav1.ConditionFalse, "ExternalExecutorRequired", "no explicit central recovery executor is configured; no database or storage mutation was attempted", now))
	return r.updateRecoveryStatus(ctx, request)
}

func (r *RecoveryRequestReconciler) setRecoveryExecuting(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, now time.Time) error {
	request.Status.Phase = operatorv1alpha1.RecoveryPhaseExecuting
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "Accepted", metav1.ConditionTrue, "PlanValidated", "recovery plan is identity-bound; approvedBy is self-reported and does not authorize execution", now))
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "ExecutionReady", metav1.ConditionTrue, "ExecutionStarted", "explicit recovery executor was admitted to execute the persisted plan", now))
	return r.updateRecoveryStatus(ctx, request)
}

func (r *RecoveryRequestReconciler) setRecoveryInProgress(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, now time.Time) error {
	request.Status.Phase = operatorv1alpha1.RecoveryPhaseExecuting
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "Accepted", metav1.ConditionTrue, "PlanValidated", "recovery plan is identity-bound; approvedBy is self-reported and does not authorize execution", now))
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "ExecutionReady", metav1.ConditionTrue, "ExecutionInProgress", "the durable rejoin operation is advancing and will be retried until both databases and the workload converge", now))
	return r.updateRecoveryStatus(ctx, request)
}

func (r *RecoveryRequestReconciler) setRecoveryFailure(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, reason, message string, now time.Time, expired bool) error {
	if expired {
		request.Status.Phase = operatorv1alpha1.RecoveryPhaseExpired
	} else {
		request.Status.Phase = operatorv1alpha1.RecoveryPhaseFailed
	}
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "Accepted", metav1.ConditionFalse, reason, message, now))
	setCondition(&request.Status.Conditions, recoveryCondition(request.Generation, "ExecutionReady", metav1.ConditionFalse, reason, message, now))
	return r.updateRecoveryStatus(ctx, request)
}

func (r *RecoveryRequestReconciler) updateRecoveryStatus(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest) error {
	if request == nil {
		return errors.New("recovery status update requires a request")
	}
	current := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := r.Get(ctx, client.ObjectKeyFromObject(request), current); err == nil && reflect.DeepEqual(current.Status, request.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return err
	}
	return nil
}

var _ reconcile.Reconciler = (*KubeOVNInstallationReconciler)(nil)
var _ reconcile.Reconciler = (*RecoveryRequestReconciler)(nil)
