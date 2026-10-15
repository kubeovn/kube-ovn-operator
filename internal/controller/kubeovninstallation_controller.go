package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
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
	WorkloadFactory func(apply.InstallationIdentity) apply.ApplyClient
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
		if err := r.updateStatus(ctx, installation, plan, now(), nil); err != nil {
			return reconcile.Result{}, err
		}
		logger.Info("installation plan is blocked", "reason", plan.Blocking)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	renderer := r.Renderer
	if r.RendererFactory != nil {
		renderer = r.RendererFactory(installation)
	}
	if renderer == nil {
		renderer = chart.ValuesRenderer{}
	}
	rendered, err := renderer.Render(ctx, installation.Spec)
	if err != nil {
		plan.Phase = operatorv1alpha1.PhaseDegraded
		plan.Step = "Render"
		plan.Blocking = err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	workload := r.Workload
	if r.WorkloadFactory != nil {
		workload = r.WorkloadFactory(apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID})
	}
	var observation *apply.Observation
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
			plan.Step = "Apply"
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := workload.Apply(operationCtx, rendered.Objects); err != nil {
				return err
			}
			plan.Step = "Observe"
			if observer, ok := workload.(apply.Observer); ok {
				observed, err := observer.Observe(operationCtx, rendered.Objects)
				if err != nil {
					return err
				}
				observation = &observed
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
	if err := r.updateStatus(ctx, installation, plan, now(), observation); err != nil {
		return reconcile.Result{}, err
	}
	if errors.Is(err, lease.ErrHeld) {
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
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

func (r *KubeOVNInstallationReconciler) updateStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time, observation *apply.Observation) error {
	status := installation.Status.DeepCopy()
	if status.ApplyCheckpoint == nil {
		status.ObservedVersion = ""
	}
	status.Phase = plan.Phase
	status.ObservedGeneration = installation.Generation
	status.Topology.DesiredCentralReplicas = installation.Spec.Central.Replicas
	status.Topology.ReadyCentralReplicas = 0
	status.HCP = operatorv1alpha1.HCPStatus{}
	status.Upgrade.Step = plan.Step
	status.Upgrade.CanRollback = false
	readyReason, message := "PlanOnly", "The installation plan is valid; workload readiness has not been observed."
	workloadStatus := metav1.ConditionUnknown
	workloadReason := "NotObserved"
	degraded := metav1.ConditionFalse
	degradedReason := "PlanValid"
	if observation != nil {
		status.Topology.ReadyCentralReplicas = observation.ReadyCentralReplicas
		workloadStatus = metav1.ConditionFalse
		workloadReason = "RolloutProgressing"
		readyReason, message = "WorkloadsProgressing", observation.Message
		if observation.Ready {
			workloadStatus, workloadReason = metav1.ConditionTrue, "RolloutComplete"
			status.ObservedVersion = installation.Spec.Version
			readyReason = "VerificationPending"
		}
	}
	if plan.Blocking != "" {
		readyReason, message = "SafetyGateBlocked", plan.Blocking
		degraded, degradedReason = metav1.ConditionTrue, "SafetyGateBlocked"
	}
	for _, condition := range []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionFalse, Reason: readyReason, Message: message},
		{Type: "Degraded", Status: degraded, Reason: degradedReason, Message: message},
		{Type: "WorkloadsReady", Status: workloadStatus, Reason: workloadReason, Message: message},
	} {
		condition.ObservedGeneration = installation.Generation
		condition.LastTransitionTime = metav1.NewTime(now)
		setCondition(&status.Conditions, condition)
	}
	return r.Status().Update(ctx, installationWithStatus(installation, *status))
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
			return fmt.Errorf("installation inputs changed after the first apply checkpoint; a lifecycle executor is required")
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

type RecoveryRequestReconciler struct {
	client.Client
	Scheme *apiruntime.Scheme
}

func (r *RecoveryRequestReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).For(&operatorv1alpha1.KubeOVNRecoveryRequest{}).Complete(r)
}

func (r *RecoveryRequestReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := r.Get(ctx, req.NamespacedName, request); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	condition := metav1.Condition{Type: "Accepted", Status: metav1.ConditionFalse, Reason: "AwaitingSafetyReview", Message: "Recovery requests are recorded for an explicit central admin adapter; no database is wiped automatically.", ObservedGeneration: request.Generation, LastTransitionTime: metav1.Now()}
	request.Status.Phase = "Pending"
	request.Status.Conditions = []metav1.Condition{condition}
	if err := r.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

var _ reconcile.Reconciler = (*KubeOVNInstallationReconciler)(nil)
var _ reconcile.Reconciler = (*RecoveryRequestReconciler)(nil)
