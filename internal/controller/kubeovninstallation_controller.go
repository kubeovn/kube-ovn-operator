package controller

import (
	"context"
	"fmt"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	installationLeasePrefix = "kube-ovn-operator-installation-"
	membershipLeasePrefix   = "kube-ovn-operator-central-"
	planConfigMapPrefix     = "kube-ovn-operator-plan-"
)

type KubeOVNInstallationReconciler struct {
	client.Client
	Scheme          *apiruntime.Scheme
	Clock           func() time.Time
	Renderer        chart.Renderer
	Workload        apply.ApplyClient
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
	now := time.Now
	if r.Clock != nil {
		now = r.Clock
	}
	plan := planner.Build(installation.Spec, installation.Status.ObservedVersion)
	if plan.Blocking != "" {
		if err := r.updateStatus(ctx, installation, plan, now()); err != nil {
			return reconcile.Result{}, err
		}
		logger.Info("installation plan is blocked", "reason", plan.Blocking)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	renderer := r.Renderer
	if renderer == nil {
		renderer = chart.ValuesRenderer{}
	}
	rendered, err := renderer.Render(ctx, installation.Spec)
	if err != nil {
		plan.Phase = operatorv1alpha1.PhaseDegraded
		plan.Step = "Render"
		plan.Blocking = err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now()); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.ensureLease(ctx, installation, installationLeasePrefix); err != nil {
		return reconcile.Result{}, err
	}
	if installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft {
		if err := r.ensureLease(ctx, installation, membershipLeasePrefix); err != nil {
			return reconcile.Result{}, err
		}
	}
	if err := r.ensurePlanConfigMap(ctx, installation, plan, rendered); err != nil {
		return reconcile.Result{}, err
	}
	workload := r.Workload
	if r.WorkloadFactory != nil {
		workload = r.WorkloadFactory(apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID})
	}
	if workload != nil && len(rendered.Objects) != 0 {
		if err := workload.Apply(ctx, rendered.Objects); err != nil {
			plan.Phase = operatorv1alpha1.PhaseDegraded
			plan.Step = "Apply"
			plan.Blocking = err.Error()
			if statusErr := r.updateStatus(ctx, installation, plan, now()); statusErr != nil {
				return reconcile.Result{}, statusErr
			}
			logger.Info("installation workload apply is blocked", "reason", err.Error())
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	if err := r.updateStatus(ctx, installation, plan, now()); err != nil {
		return reconcile.Result{}, err
	}
	logger.Info("installation plan reconciled", "phase", plan.Phase, "step", plan.Step)
	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *KubeOVNInstallationReconciler) ensureLease(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, prefix string) error {
	lease := new(coordinationv1.Lease)
	name := prefix + installation.Name
	key := types.NamespacedName{Name: name, Namespace: installation.Namespace}
	lease.Name = name
	lease.Namespace = installation.Namespace
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, lease, func() error {
		lease.Name = name
		lease.Namespace = installation.Namespace
		if err := controllerutil.SetControllerReference(installation, lease, r.Scheme); err != nil {
			return err
		}
		holder := string(installation.UID)
		if holder == "" {
			holder = installation.Name
		}
		lease.Spec.HolderIdentity = &holder
		duration := int32(90)
		lease.Spec.LeaseDurationSeconds = &duration
		renewed := metav1.NewMicroTime(time.Now())
		lease.Spec.RenewTime = &renewed
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure lease %s: %w", key.String(), err)
	}
	return nil
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

func (r *KubeOVNInstallationReconciler) updateStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time) error {
	status := installation.Status.DeepCopy()
	status.Phase = plan.Phase
	status.ObservedGeneration = installation.Generation
	status.Topology.DesiredCentralReplicas = installation.Spec.Central.Replicas
	status.Upgrade.Step = plan.Step
	status.Upgrade.CanRollback = installation.Spec.Upgrade.AllowRollback
	if plan.Blocking == "" {
		status.ObservedVersion = installation.Spec.Version
		status.Topology.ReadyCentralReplicas = installation.Spec.Central.Replicas
		status.HCP.Paired = installation.Spec.HCP.Enabled
		status.HCP.EndpointReady = !installation.Spec.HCP.Enabled || (installation.Spec.HCP.NBEndpoint != "" && installation.Spec.HCP.SBEndpoint != "")
		setCondition(&status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "PlanReady", Message: "The installation plan passed validation and is serialized for the workload adapter.", ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
		setCondition(&status.Conditions, metav1.Condition{Type: "Degraded", Status: metav1.ConditionFalse, Reason: "PlanValid", Message: "No safety gate is currently blocking reconciliation.", ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
	} else {
		setCondition(&status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "SafetyGateBlocked", Message: plan.Blocking, ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
		setCondition(&status.Conditions, metav1.Condition{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "SafetyGateBlocked", Message: plan.Blocking, ObservedGeneration: installation.Generation, LastTransitionTime: metav1.NewTime(now)})
	}
	if err := r.Status().Update(ctx, installationWithStatus(installation, *status)); err != nil {
		if apierrors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("update installation status: %w", err)
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
