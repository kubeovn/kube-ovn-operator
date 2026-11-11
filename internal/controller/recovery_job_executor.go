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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
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

// ErrRecoveryWorkloadNotReady means the admitted plan is intact but an
// external workload artifact has not become available yet. The request
// controller must keep the request Executing and retry instead of converting
// this transient readiness gap into a terminal failure.
var ErrRecoveryWorkloadNotReady = errors.New("offline recovery workload is not ready")

// RecoveryJobExecutor creates and observes one immutable offline rebuild Job.
// It intentionally does not discover storage identities or backup PVCs. Until
// a production builder can provide those values, execution remains blocked
// before any Job is created.
type RecoveryJobExecutor struct {
	// Client must read directly from the API server for request and Job identity checks.
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
	if request.DeletionTimestamp != nil || request.UID != plan.RequestUID || request.Generation != plan.RequestGeneration || request.Status.Plan == nil || request.Status.Plan.Digest != plan.Digest {
		return errors.New("offline rebuild request identity or immutable plan changed")
	}
	spec, err := e.Builder.Build(ctx, plan)
	if err != nil {
		return fmt.Errorf("build offline rebuild workload: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return fmt.Errorf("validate offline rebuild workload: %w", err)
	}
	if spec.Namespace != request.Namespace || spec.PlanDigest != plan.Digest || spec.InstallationUID != string(plan.InstallationUID) {
		return errors.New("offline rebuild workload is bound to a different namespace, installation or plan")
	}
	owner := recoveryRequestOwner(request)
	if err := e.validateRecoveryConfigMaps(ctx, request, spec); err != nil {
		return err
	}
	desired, err := spec.Job(&owner)
	if err != nil {
		return fmt.Errorf("render offline rebuild Job: %w", err)
	}
	execution := request.Status.Execution
	if execution != nil {
		if execution.JobName != "" && execution.JobName != desired.Name {
			return errors.New("offline rebuild Job name changed after admission")
		}
		if execution.JobPlanDigest != "" && execution.JobPlanDigest != spec.PlanDigest {
			return errors.New("offline rebuild Job plan digest changed after admission")
		}
	}
	key := client.ObjectKeyFromObject(desired)
	current := new(batchv1.Job)
	err = e.Client.Get(ctx, key, current)
	switch {
	case apierrors.IsNotFound(err):
		if execution != nil && (execution.JobName != "" || execution.JobUID != "") {
			return errors.New("pinned offline rebuild Job no longer exists")
		}
		current = desired.DeepCopy()
		current.Spec.Suspend = new(true)
		if err := e.Client.Create(ctx, current); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return ErrRecoveryInProgress
			}
			return fmt.Errorf("create offline rebuild Job: %w", err)
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
	if recoveryJobSuspended(current) {
		// An optimistic update prevents activating a replacement or a Job
		// that changed since its spec and UID were checked.
		current.Spec.Suspend = new(false)
		if err := e.Client.Update(ctx, current); err != nil {
			// A lost response may mean activation already succeeded. Read
			// the pinned Job again instead of declaring recovery failed.
			return fmt.Errorf("%w: activate pinned offline rebuild Job: %w", ErrRecoveryInProgress, err)
		}
		return ErrRecoveryInProgress
	}
	if condition, ok := jobCondition(current, batchv1.JobFailed); ok {
		return fmt.Errorf("offline rebuild Job failed: %s", strings.TrimSpace(condition.Message))
	}
	if _, ok := jobCondition(current, batchv1.JobComplete); ok {
		return nil
	}
	return ErrRecoveryInProgress
}

func (e *RecoveryJobExecutor) validateRecoveryConfigMaps(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, spec recovery.WorkloadSpec) error {
	for name, artifact := range map[string]struct {
		key        string
		annotation string
	}{
		spec.PlanConfigMapName:       {key: "plan.json", annotation: recovery.RebuildPlanDigestAnnotation},
		spec.IsolationProofConfigMap: {key: "proof.json", annotation: recovery.RebuildPlanDigestAnnotation},
	} {
		configMap := new(corev1.ConfigMap)
		if err := e.Client.Get(ctx, client.ObjectKey{Namespace: spec.Namespace, Name: name}, configMap); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: offline rebuild ConfigMap %q is not available", ErrRecoveryWorkloadNotReady, name)
			}
			return fmt.Errorf("read offline rebuild %s ConfigMap %q: %w", artifact.annotation, name, err)
		}
		if configMap.DeletionTimestamp != nil || configMap.Immutable == nil || !*configMap.Immutable {
			return fmt.Errorf("offline rebuild ConfigMap %q must be immutable and not deleting", name)
		}
		if configMap.Annotations[artifact.annotation] != spec.PlanDigest || strings.TrimSpace(configMap.Data[artifact.key]) == "" {
			return fmt.Errorf("offline rebuild ConfigMap %q is not bound to plan %s", name, spec.PlanDigest)
		}
		if !hasRecoveryRequestOwnerConfigMap(configMap, request) {
			return fmt.Errorf("offline rebuild ConfigMap %q owner identity does not match the recovery request", name)
		}
	}
	return nil
}

func hasRecoveryRequestOwnerConfigMap(configMap *corev1.ConfigMap, request *operatorv1alpha1.KubeOVNRecoveryRequest) bool {
	for _, owner := range configMap.OwnerReferences {
		if owner.APIVersion == operatorv1alpha1.GroupVersion.String() && owner.Kind == "KubeOVNRecoveryRequest" && owner.Name == request.Name && owner.UID == request.UID && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func recoveryRequestOwner(request *operatorv1alpha1.KubeOVNRecoveryRequest) metav1.OwnerReference {
	controller, blockDeletion := true, true
	return metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNRecoveryRequest", Name: request.Name, UID: request.UID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}
}

func validateRecoveryJob(current, desired *batchv1.Job, execution *operatorv1alpha1.RecoveryExecutionStatus) error {
	if current == nil || desired == nil || current.Namespace != desired.Namespace || current.Name != desired.Name {
		return errors.New("offline rebuild Job identity is invalid")
	}
	for k, v := range desired.Labels {
		if current.Labels[k] != v {
			return errors.New("offline rebuild Job labels do not match the admitted workload")
		}
	}
	for k, v := range desired.Annotations {
		if current.Annotations[k] != v {
			return errors.New("offline rebuild Job annotations do not match the admitted workload")
		}
	}
	if len(desired.OwnerReferences) != 1 || !hasRecoveryRequestOwner(current, desired.OwnerReferences[0]) {
		return errors.New("offline rebuild Job owner identity does not match the recovery request")
	}
	if current.UID == "" {
		return errors.New("offline rebuild Job is missing its API-assigned UID")
	}
	pinned := execution != nil && execution.JobUID != ""
	if pinned && execution.JobUID != current.UID {
		return errors.New("offline rebuild Job UID changed after admission")
	}
	if recoveryJobSuspended(current) {
		desired = desired.DeepCopy()
		desired.Spec.Suspend = new(true)
	} else if !pinned {
		return errors.New("offline rebuild Job is already active without a pinned UID")
	}
	if current.DeletionTimestamp != nil || !matchesRecoveryJobSpec(desired, current) {
		return errors.New("offline rebuild Job is deleting or its spec differs from the admitted workload")
	}
	return nil
}

// Accept only Kubernetes' Job defaults and generated selector/labels. The
// rest of the spec, including the Pod template, must match the builder output.
func matchesRecoveryJobSpec(desired, current *batchv1.Job) bool {
	expected := desired.Spec.DeepCopy()
	actual := &current.Spec
	if expected.CompletionMode == nil && actual.CompletionMode != nil && *actual.CompletionMode == batchv1.NonIndexedCompletion {
		expected.CompletionMode = actual.CompletionMode
	}
	if expected.Suspend == nil && actual.Suspend != nil && !*actual.Suspend {
		expected.Suspend = actual.Suspend
	}
	if expected.ManualSelector == nil && actual.ManualSelector != nil && !*actual.ManualSelector {
		expected.ManualSelector = actual.ManualSelector
	}
	if expected.PodReplacementPolicy == nil && actual.PodReplacementPolicy != nil && *actual.PodReplacementPolicy == batchv1.TerminatingOrFailed {
		expected.PodReplacementPolicy = actual.PodReplacementPolicy
	}
	if expected.Template.Spec.SchedulerName == "" && actual.Template.Spec.SchedulerName == corev1.DefaultSchedulerName {
		expected.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
	}
	// The API converts the deprecated serviceAccount field to and from
	// serviceAccountName when serializing PodSpecs.
	if expected.Template.Spec.DeprecatedServiceAccount == "" && actual.Template.Spec.DeprecatedServiceAccount == expected.Template.Spec.ServiceAccountName {
		expected.Template.Spec.DeprecatedServiceAccount = expected.Template.Spec.ServiceAccountName
	}
	if expected.Selector == nil && actual.Selector != nil && current.UID != "" {
		expected.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{batchv1.ControllerUidLabel: string(current.UID)}}
		for key, value := range map[string]string{
			batchv1.ControllerUidLabel: string(current.UID), "controller-uid": string(current.UID),
			batchv1.JobNameLabel: current.Name, "job-name": current.Name,
		} {
			expected.Template.Labels[key] = value
		}
	}
	return apiequality.Semantic.DeepEqual(expected, actual)
}

func hasRecoveryRequestOwner(job *batchv1.Job, expected metav1.OwnerReference) bool {
	for _, owner := range job.OwnerReferences {
		if owner.APIVersion == expected.APIVersion && owner.Kind == expected.Kind && owner.Name == expected.Name && owner.UID == expected.UID && owner.Controller != nil && *owner.Controller {
			return true
		}
	}
	return false
}

func recoveryJobSuspended(job *batchv1.Job) bool {
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

func pinRecoveryJobStatus(ctx context.Context, kube client.Client, request *operatorv1alpha1.KubeOVNRecoveryRequest, job *batchv1.Job) error {
	if request.Status.Execution == nil {
		request.Status.Execution = new(operatorv1alpha1.RecoveryExecutionStatus)
	}
	execution := request.Status.Execution
	jobPhase := "Running"
	switch {
	case recoveryJobSuspended(job):
		jobPhase = "Suspended"
	case jobConditionPresent(job, batchv1.JobComplete):
		jobPhase = string(batchv1.JobComplete)
	case jobConditionPresent(job, batchv1.JobFailed):
		jobPhase = string(batchv1.JobFailed)
	}
	jobReason := ""
	if condition, ok := jobCondition(job, batchv1.JobFailed); ok {
		jobReason = strings.TrimSpace(condition.Message)
	}
	jobDigest := job.Annotations[recovery.RebuildPlanDigestAnnotation]
	if execution.JobName == job.Name && execution.JobUID == job.UID && execution.JobPlanDigest == jobDigest && execution.JobPhase == jobPhase && execution.JobReason == jobReason {
		return nil
	}
	execution.JobName, execution.JobUID, execution.JobPlanDigest = job.Name, job.UID, jobDigest
	execution.JobPhase, execution.JobReason = jobPhase, jobReason
	if err := kube.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return fmt.Errorf("%w: persist offline rebuild Job identity: %w", ErrRecoveryInProgress, err)
		}
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
