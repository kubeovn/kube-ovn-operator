package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OfflineRebuildWorkloadBuilder is the narrow boundary between recovery
// admission and the Kubernetes objects needed by a privileged rebuild Job.
// Implementations must return a complete, validated WorkloadSpec. A builder
// must never fill missing storage, backup, or ownership identities with
// placeholders.
type OfflineRebuildWorkloadBuilder interface {
	Build(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error)
}

// RecoveryJobExecutor creates and observes one immutable offline rebuild Job.
// It intentionally does not discover storage identities or backup PVCs. Until
// a production builder can provide those values, execution remains blocked
// before any Job is created.
type RecoveryJobExecutor struct {
	Client  client.Client
	Builder OfflineRebuildWorkloadBuilder
}

func (e *RecoveryJobExecutor) Execute(ctx context.Context, plan RecoveryPlan) error {
	if e == nil || e.Client == nil || e.Builder == nil {
		return errors.New("offline rebuild Job executor is not fully configured")
	}
	if plan.Mode == operatorv1alpha1.RecoveryModeRejoin {
		return errors.New("offline rebuild Job executor does not handle rejoin recovery")
	}
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.RequestNamespace, Name: plan.RequestName}, request); err != nil {
		return fmt.Errorf("read offline rebuild request: %w", err)
	}
	if request.UID != plan.RequestUID || request.Generation != plan.RequestGeneration || request.Status.Plan == nil || request.Status.Plan.Digest != plan.Digest {
		return errors.New("offline rebuild request identity or immutable plan changed")
	}
	spec, err := e.Builder.Build(ctx, plan)
	if err != nil {
		return fmt.Errorf("build offline rebuild workload: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("validate offline rebuild workload: %w", err)
	}
	if spec.PlanDigest != plan.Digest || spec.InstallationUID != string(plan.InstallationUID) {
		return errors.New("offline rebuild workload is bound to a different installation or plan")
	}
	owner := recoveryRequestOwner(request)
	desired, err := spec.Job(&owner)
	if err != nil {
		return fmt.Errorf("render offline rebuild Job: %w", err)
	}
	key := client.ObjectKeyFromObject(desired)
	current := new(batchv1.Job)
	err = e.Client.Get(ctx, key, current)
	switch {
	case apierrors.IsNotFound(err):
		if err := e.Client.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create offline rebuild Job: %w", err)
		}
		if err := e.Client.Get(ctx, key, current); err != nil {
			return fmt.Errorf("read created offline rebuild Job: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read offline rebuild Job: %w", err)
	}
	if err := validateRecoveryJob(current, desired, request.Status.Execution); err != nil {
		return err
	}
	if err := pinRecoveryJobStatus(ctx, e.Client, request, current); err != nil {
		return err
	}
	if condition, ok := jobCondition(current, batchv1.JobFailed); ok {
		return fmt.Errorf("offline rebuild Job failed: %s", strings.TrimSpace(condition.Message))
	}
	if _, ok := jobCondition(current, batchv1.JobComplete); ok {
		return nil
	}
	return ErrRecoveryInProgress
}

func recoveryRequestOwner(request *operatorv1alpha1.KubeOVNRecoveryRequest) metav1.OwnerReference {
	controller, blockDeletion := true, true
	return metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNRecoveryRequest", Name: request.Name, UID: request.UID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}
}

func validateRecoveryJob(current, desired *batchv1.Job, execution *operatorv1alpha1.RecoveryExecutionStatus) error {
	if current == nil || desired == nil || current.Namespace != desired.Namespace || current.Name != desired.Name {
		return errors.New("offline rebuild Job identity is invalid")
	}
	if current.Labels[recovery.RebuildWorkloadLabel] != "true" || current.Annotations[recovery.RebuildPlanDigestAnnotation] != desired.Annotations[recovery.RebuildPlanDigestAnnotation] || current.Annotations[recovery.RebuildOperationAnnotation] != desired.Annotations[recovery.RebuildOperationAnnotation] {
		return errors.New("offline rebuild Job metadata does not match the admitted workload")
	}
	if len(desired.OwnerReferences) != 1 || !hasRecoveryRequestOwner(current, desired.OwnerReferences[0]) {
		return errors.New("offline rebuild Job owner identity does not match the recovery request")
	}
	if execution != nil {
		if execution.JobName != "" && execution.JobName != current.Name {
			return errors.New("offline rebuild Job name changed after admission")
		}
		if execution.JobUID != "" && execution.JobUID != current.UID {
			return errors.New("offline rebuild Job UID changed after admission")
		}
		if execution.JobPlanDigest != "" && execution.JobPlanDigest != desired.Annotations[recovery.RebuildPlanDigestAnnotation] {
			return errors.New("offline rebuild Job plan digest changed after admission")
		}
	}
	return nil
}

func hasRecoveryRequestOwner(job *batchv1.Job, expected metav1.OwnerReference) bool {
	for _, owner := range job.OwnerReferences {
		if owner.APIVersion == expected.APIVersion && owner.Kind == expected.Kind && owner.Name == expected.Name && owner.UID == expected.UID && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func pinRecoveryJobStatus(ctx context.Context, kube client.Client, request *operatorv1alpha1.KubeOVNRecoveryRequest, job *batchv1.Job) error {
	if request.Status.Execution == nil {
		request.Status.Execution = new(operatorv1alpha1.RecoveryExecutionStatus)
	}
	execution := request.Status.Execution
	execution.JobName = job.Name
	execution.JobUID = job.UID
	execution.JobPlanDigest = job.Annotations[recovery.RebuildPlanDigestAnnotation]
	switch {
	case jobConditionPresent(job, batchv1.JobComplete):
		execution.JobPhase = string(batchv1.JobComplete)
	case jobConditionPresent(job, batchv1.JobFailed):
		execution.JobPhase = string(batchv1.JobFailed)
	default:
		execution.JobPhase = "Running"
	}
	if condition, ok := jobCondition(job, batchv1.JobFailed); ok {
		execution.JobReason = strings.TrimSpace(condition.Message)
	}
	if err := kube.Status().Update(ctx, request); err != nil && !apierrors.IsConflict(err) {
		return fmt.Errorf("persist offline rebuild Job identity: %w", err)
	}
	return nil
}

func jobConditionPresent(job *batchv1.Job, conditionType batchv1.JobConditionType) bool {
	_, ok := jobCondition(job, conditionType)
	return ok
}

func jobCondition(job *batchv1.Job, conditionType batchv1.JobConditionType) (batchv1.JobCondition, bool) {
	if job == nil {
		return batchv1.JobCondition{}, false
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return condition, true
		}
	}
	return batchv1.JobCondition{}, false
}

type offlineRebuildWorkloadBuilderFunc func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error)

func (f offlineRebuildWorkloadBuilderFunc) Build(ctx context.Context, plan RecoveryPlan) (recovery.WorkloadSpec, error) {
	return f(ctx, plan)
}

var _ RecoveryExecutor = (*RecoveryJobExecutor)(nil)
var _ OfflineRebuildWorkloadBuilder = offlineRebuildWorkloadBuilderFunc(nil)
