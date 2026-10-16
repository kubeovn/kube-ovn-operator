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
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	planConfigMapPrefix = "kube-ovn-operator-plan-"
)

type KubeOVNInstallationReconciler struct {
	client.Client
	Scheme          *apiruntime.Scheme
	Clock           func() time.Time
	Renderer        chart.Renderer
	RendererFactory func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer
	Workload        apply.ApplyClient
	Leases          lease.Runner
	OVN             health.OVNProvider
	Runtime         health.RuntimeProvider
	Network         health.NetworkProvider
	Topology        topology.Provider
	Lifecycle       lifecycleCoordinator
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
}

func (r *KubeOVNInstallationReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		For(&operatorv1alpha1.KubeOVNInstallation{}).
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

func (r *KubeOVNInstallationReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
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
	if err := validateMemberRequestLifecycle(installation); err != nil {
		plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "LifecycleSelection", err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		logger.Info("member requests are waiting for an initial retained topology", "reason", err.Error())
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
	renderSpec, renderErr := r.lifecycleRenderSpec(ctx, installation)
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
			if err := r.ensureApplyCheckpoint(operationCtx, installation, rendered); err != nil {
				return err
			}
			if localRaft(installation) && r.Topology != nil {
				plan.Step = "VerifyRetainedTopology"
				if err := r.ensureTopology(operationCtx, installation, false); err != nil {
					topologyCondition = &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "TopologyUnverified", Message: err.Error()}
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
			upgradeAuthorized := upgradeAuthorizesApply(installation)
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
			if err := workload.Apply(operationCtx, rendered.Objects); err != nil {
				if upgradeAuthorized {
					return errors.Join(err, r.failUpgradeApply(operationCtx, installation, err.Error()))
				}
				return err
			}
			if upgradeAuthorized {
				// Persist the accepted target before observing it. If the process
				// exits after Chart Apply, the next reconciliation can retry this
				// idempotent target from the durable Applying phase rather than
				// treating the verified backup as a fresh authorization.
				if err := r.markUpgradeCheckpointApplied(operationCtx, installation, now()); err != nil {
					return err
				}
			}
			if lifecycleHandoffAuthorizesApply(installation) && installation.Status.Topology.Operation != nil {
				// Consume the lifecycle authorization when the Chart write is
				// accepted. Later observation failures must not make it reusable.
				installation.Status.Topology.Operation.HandoffApplied = true
			}
			plan.Step = "Observe"
			if observer, ok := workload.(apply.Observer); ok {
				observed, err := observer.Observe(operationCtx, rendered.Objects)
				if err != nil {
					return err
				}
				observation = &observed
			}
			if observation != nil && observation.Ready && localRaft(installation) && r.Topology != nil {
				plan.Step = "RecordTopology"
				condition := metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionTrue, Reason: "IdentityVerified", Message: "Live central identities match the retained topology"}
				if err := r.ensureTopology(operationCtx, installation, true); err != nil {
					condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, "TopologyUnverified", err.Error()
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
	var runtimeReport *health.RuntimeReport
	if err == nil && observation != nil && observation.Ready && r.Runtime != nil {
		runtimeReport = new(r.Runtime.Observe(ctx, installation))
	}
	var networkReport *health.NetworkReport
	if ovnReport != nil && ovnReport.Ready && runtimeReport != nil && runtimeReport.Northd.Status == metav1.ConditionTrue && runtimeReport.DataPlane.Status == metav1.ConditionTrue && r.Network != nil {
		networkReport = new(r.Network.Observe(ctx, installation, runtimeReport.Nodes))
	}
	if err := r.updateStatus(ctx, installation, plan, now(), observation, ovnReport, runtimeReport, networkReport, topologyCondition); err != nil {
		return reconcile.Result{}, err
	}
	if errors.Is(err, lease.ErrHeld) {
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
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
			verified, err := r.Topology.VerifyRetained(centralCtx, installation)
			if err != nil {
				return fmt.Errorf("verify retained topology before automatic selection: %w", err)
			}
			if verified == nil || installation.Status.Topology.Inventory == nil || *verified != *installation.Status.Topology.Inventory {
				return errors.New("automatic lifecycle selection requires a complete fresh topology matching the retained inventory")
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
		if member.ReplaceEndpoint != "" {
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

func (r *KubeOVNInstallationReconciler) updateStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time, observation *apply.Observation, ovnReport *health.OVNReport, runtimeReport *health.RuntimeReport, networkReport *health.NetworkReport, topologyCondition *metav1.Condition) error {
	status := installation.Status.DeepCopy()
	if status.ApplyCheckpoint == nil {
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
	if topologyState.Status == metav1.ConditionTrue && plan.Blocking == "" && workloadStatus == metav1.ConditionTrue && ovnStatus == metav1.ConditionTrue && northd.Status == metav1.ConditionTrue && dataPlane.Status == metav1.ConditionTrue && networkVerified {
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
		network, topologyState,
	} {
		condition.ObservedGeneration = installation.Generation
		condition.LastTransitionTime = metav1.NewTime(now)
		setCondition(&status.Conditions, condition)
	}
	return r.Status().Update(ctx, installationWithStatus(installation, *status))
}

func localRaft(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	return installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft && installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly && !installation.Spec.HCP.Enabled
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
	if err := r.Status().Update(ctx, installation); err != nil {
		installation.Status.Topology.Inventory = nil
		return fmt.Errorf("persist central topology reference: %w", err)
	}
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
	data, err := stdjson.Marshal(installation.Spec, stdjson.Deterministic(true))
	if err != nil {
		return false, fmt.Errorf("hash current installation spec: %w", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash {
		return true, nil
	}
	spec := installation.Spec
	spec.Central.Replicas = int32(retainedReplicas)
	data, err = stdjson.Marshal(spec, stdjson.Deterministic(true))
	if err != nil {
		return false, fmt.Errorf("hash prior installation spec: %w", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash {
		return true, nil
	}
	// Prepared member slots are lifecycle inputs. The first replica increase
	// commonly adds those slots at the same time as the desired replica count;
	// the initial Chart checkpoint predates them, so compare once with the
	// slots omitted. Once the handoff is accepted, the full current spec is
	// recorded in the checkpoint and the exact hash path above applies again.
	if len(spec.Central.MemberRequests) == 0 {
		return false, nil
	}
	spec.Central.MemberRequests = nil
	data, err = stdjson.Marshal(spec, stdjson.Deterministic(true))
	if err != nil {
		return false, fmt.Errorf("hash prior installation spec without member requests: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash, nil
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
		if member.NodeName != request.Add.Workload.Node.Name || member.ClaimName != request.Add.Workload.Claim.Name || member.VolumeName != request.Add.Workload.Volume.Name || member.Image != request.Add.Workload.Image || member.NorthboundAddress != northboundAddress || member.SouthboundAddress != southboundAddress || member.NorthboundClientAddress != request.Add.NorthboundAddress || member.SouthboundClientAddress != request.Add.SouthboundAddress || member.ReplaceEndpoint != request.RemoveEndpoint {
			return fmt.Errorf("lifecycle member request %q changed after operation admission; restore the pinned node, storage, image and NB/SB addresses before retrying", memberName)
		}
		return nil
	}
	return fmt.Errorf("lifecycle member request %q was removed after operation admission; restore the pinned request before retrying", memberName)
}

// lifecycleRenderSpec keeps the original Chart Deployment at its retained
// member count while independently managed additions or removals converge.
// Without this boundary, a successful addition would scale the old Deployment
// at the same time as the new StatefulSet and create duplicate RAFT members.
func (r *KubeOVNInstallationReconciler) lifecycleRenderSpec(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (operatorv1alpha1.KubeOVNInstallationSpec, error) {
	spec := installation.Spec
	if !localRaft(installation) || !spec.Central.DynamicMembership || installation.Status.Topology.Inventory == nil || r.Topology == nil {
		return spec, nil
	}
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok || r.Leases == nil {
		return spec, errors.New("dynamic membership rendering requires a retained topology provider and operation leases")
	}
	var retained health.CentralTopology
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			var err error
			retained, err = provider.Retained(centralCtx, installation)
			return err
		})
	})
	if err != nil {
		return spec, fmt.Errorf("read retained topology for dynamic chart rendering: %w", err)
	}
	chartReplicas := int32(0)
	for _, member := range retained.Members {
		if member.Workload.Kind == "Deployment" {
			chartReplicas++
		}
	}
	if chartReplicas == 0 {
		return spec, errors.New("dynamic membership requires at least one retained Chart Deployment member")
	}
	spec.Central.Replicas = chartReplicas
	return spec, nil
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
	refs, err := stdjson.Marshal(status.DatabaseRefs, stdjson.Deterministic(true))
	if err != nil || status.ID != fmt.Sprintf("sha256:%x", sha256.Sum256(refs)) {
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
	if checkpoint == nil || checkpoint.ID == "" || len(checkpoint.DatabaseRefs) != 2 || checkpoint.SourceVersion != binding.Version || checkpoint.TargetVersion != targetVersion || strings.TrimSpace(checkpoint.Destination) != binding.Destination || checkpoint.TopologyDigest != binding.TopologyDigest {
		return false
	}
	return checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointVerified || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplying || checkpoint.Phase == operatorv1alpha1.UpgradeCheckpointApplied
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
	spec := installation.Spec
	spec.Version = installation.Status.ObservedVersion
	data, err := stdjson.Marshal(spec, stdjson.Deterministic(true))
	return err == nil && fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash
}

func (r *KubeOVNInstallationReconciler) ensureApplyCheckpoint(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, rendered chart.Rendered) error {
	specJSON, err := stdjson.Marshal(installation.Spec, stdjson.Deterministic(true))
	if err != nil {
		return err
	}
	manifestJSON, err := stdjson.Marshal(rendered.Objects, stdjson.Deterministic(true))
	if err != nil {
		return err
	}
	checkpoint := operatorv1alpha1.ApplyCheckpointStatus{
		SpecHash:     fmt.Sprintf("%x", sha256.Sum256(specJSON)),
		ManifestHash: fmt.Sprintf("%x", sha256.Sum256(manifestJSON)),
		Version:      installation.Spec.Version,
	}
	if existing := installation.Status.ApplyCheckpoint; existing != nil {
		if *existing != checkpoint {
			if !lifecycleHandoffAuthorizesApply(installation) && !upgradeAuthorizesApply(installation) {
				return fmt.Errorf("installation inputs changed after the first apply checkpoint; a lifecycle executor is required")
			}
			previous := *existing
			installation.Status.ApplyCheckpoint = &checkpoint
			if err := r.Status().Update(ctx, installation); err != nil {
				installation.Status.ApplyCheckpoint = &previous
				return fmt.Errorf("persist lifecycle-authorized apply checkpoint: %w", err)
			}
			return nil
		}
		return nil
	}
	installation.Status.ApplyCheckpoint = &checkpoint
	// Persist before any workload write so a restart or partial apply cannot
	// turn a topology/version change into a fresh installation.
	if err := r.Status().Update(ctx, installation); err != nil {
		installation.Status.ApplyCheckpoint = nil
		return fmt.Errorf("persist apply checkpoint: %w", err)
	}
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
	if validateScheduledBackup(backup, binding) != nil || installation.Status.ApplyCheckpoint.Version != installation.Status.ObservedVersion {
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
	BackupRef              string
	Approver               string
	ExpiresAt              time.Time
	Digest                 string
}

// RecoveryExecutor is an explicit integration boundary for a future central
// admin adapter. The default operator leaves it nil, so a RecoveryRequest can
// be validated and durably queued without mutating a database or storage.
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
		return reconcile.Result{}, r.setRecoveryFailure(ctx, request, "ExecutionFailed", err.Error(), now, false)
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
	switch request.Spec.Mode {
	case operatorv1alpha1.RecoveryModeRejoin:
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
	if backupRef != "" {
		if !matchesVerifiedBackup(installation, backupRef) {
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
		Reason: reason, Mode: request.Spec.Mode, BackupRef: backupRef, Approver: approver, ExpiresAt: expiresAt,
	}
	plan.Digest = recoveryPlanDigest(plan)
	return plan, "", nil
}

func matchesVerifiedBackup(installation *operatorv1alpha1.KubeOVNInstallation, reference string) bool {
	if installation == nil || strings.TrimSpace(reference) == "" || installation.Status.LastBackup == nil {
		return false
	}
	backup := installation.Status.LastBackup
	if backup.ID != reference || backup.ID == "" || backup.Version == "" || backup.Version != installation.Status.ObservedVersion || backup.TopologyDigest == "" || installation.Status.Topology.Inventory == nil || backup.TopologyDigest != installation.Status.Topology.Inventory.Digest || backup.CompletedAt.IsZero() {
		return false
	}
	return len(backup.DatabaseRefs) == 2 && backup.DatabaseRefs["OVN_Northbound"] != "" && backup.DatabaseRefs["OVN_Southbound"] != ""
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
		BackupRef              string
		Approver               string
		ExpiresAt              string
	}{
		RequestNamespace: plan.RequestNamespace, RequestName: plan.RequestName, RequestUID: plan.RequestUID,
		InstallationNamespace: plan.InstallationNamespace, InstallationName: plan.InstallationName, InstallationUID: plan.InstallationUID,
		InstallationGeneration: plan.InstallationGeneration, RequestGeneration: plan.RequestGeneration,
		Reason: plan.Reason, Mode: plan.Mode, BackupRef: plan.BackupRef, Approver: plan.Approver,
		ExpiresAt: plan.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, stdjson.Deterministic(true))
	if err != nil {
		return ""
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func recoveryPlanStatus(plan RecoveryPlan) *operatorv1alpha1.RecoveryPlanStatus {
	return &operatorv1alpha1.RecoveryPlanStatus{Digest: plan.Digest, InstallationUID: plan.InstallationUID, InstallationGeneration: plan.InstallationGeneration, RequestGeneration: plan.RequestGeneration, Mode: plan.Mode, BackupRef: plan.BackupRef, ExpiresAt: metav1.NewTime(plan.ExpiresAt)}
}

func sameRecoveryPlan(left, right *operatorv1alpha1.RecoveryPlanStatus) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Digest == right.Digest && left.InstallationUID == right.InstallationUID && left.InstallationGeneration == right.InstallationGeneration && left.RequestGeneration == right.RequestGeneration && left.Mode == right.Mode && left.BackupRef == right.BackupRef && left.ExpiresAt.Time.Equal(right.ExpiresAt.Time)
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
