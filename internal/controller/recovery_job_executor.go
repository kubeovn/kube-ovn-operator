package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	if err := e.validateRecoveryStorage(ctx, spec); err != nil {
		return err
	}
	inputDigest := recoveryWorkloadInputIdentityDigest(spec)
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
		if execution.JobInputIdentityDigest != "" && execution.JobInputIdentityDigest != inputDigest {
			return errors.New("offline rebuild Job storage or artifact identity changed after admission")
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
	if err := pinRecoveryJobStatus(ctx, e.Client, request, current, inputDigest); err != nil {
		return err
	}
	if recoveryJobSuspended(current) {
		// An optimistic update prevents activating a replacement or a Job
		// that changed since its spec and UID were checked.
		current.Spec.Suspend = new(false)
		if err := e.Client.Update(ctx, current); err != nil {
			return recoveryJobActivationError(err)
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

func recoveryJobActivationError(err error) error {
	if apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsUnauthorized(err) || apierrors.IsMethodNotSupported(err) || apierrors.IsBadRequest(err) || apierrors.IsNotAcceptable(err) || apierrors.IsUnsupportedMediaType(err) || apierrors.IsRequestEntityTooLargeError(err) {
		return fmt.Errorf("activate pinned offline rebuild Job was rejected: %w", err)
	}
	// A lost response, conflict, throttling, or transport failure may mean
	// activation already succeeded. Read the pinned Job again on retry.
	return fmt.Errorf("%w: activate pinned offline rebuild Job: %w", ErrRecoveryInProgress, err)
}

func (e *RecoveryJobExecutor) validateRecoveryConfigMaps(ctx context.Context, request *operatorv1alpha1.KubeOVNRecoveryRequest, spec recovery.WorkloadSpec) error {
	for name, artifact := range map[string]struct {
		key        string
		annotation string
		canonical  string
		uid        types.UID
		dataDigest string
	}{
		spec.PlanConfigMapName:       {key: "plan.json", annotation: recovery.RebuildPlanDigestAnnotation, canonical: recovery.RebuildCanonicalPlanDigestAnnotation, uid: spec.PlanConfigMapUID, dataDigest: spec.PlanConfigMapDataDigest},
		spec.IsolationProofConfigMap: {key: "proof.json", annotation: recovery.RebuildPlanDigestAnnotation, canonical: recovery.RebuildCanonicalPlanDigestAnnotation, uid: spec.IsolationProofConfigMapUID, dataDigest: spec.IsolationProofDataDigest},
	} {
		configMap := new(corev1.ConfigMap)
		if err := e.Client.Get(ctx, client.ObjectKey{Namespace: spec.Namespace, Name: name}, configMap); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: offline rebuild ConfigMap %q is not available", ErrRecoveryWorkloadNotReady, name)
			}
			return fmt.Errorf("read offline rebuild %s ConfigMap %q: %w", artifact.annotation, name, err)
		}
		if configMap.DeletionTimestamp != nil || configMap.UID != artifact.uid || configMap.Immutable == nil || !*configMap.Immutable {
			return fmt.Errorf("offline rebuild ConfigMap %q must be immutable and not deleting", name)
		}
		if configMap.Annotations[artifact.annotation] != spec.PlanDigest || configMap.Annotations[artifact.canonical] != spec.CanonicalPlanDigest || strings.TrimSpace(configMap.Data[artifact.key]) == "" || recovery.ConfigMapDataDigest(configMap.Data[artifact.key]) != artifact.dataDigest {
			return fmt.Errorf("offline rebuild ConfigMap %q is not bound to plan %s", name, spec.PlanDigest)
		}
		if !hasRecoveryRequestOwnerConfigMap(configMap, request) {
			return fmt.Errorf("offline rebuild ConfigMap %q owner identity does not match the recovery request", name)
		}
	}
	return nil
}

func (e *RecoveryJobExecutor) validateRecoveryStorage(ctx context.Context, spec recovery.WorkloadSpec) error {
	volumes := append([]recovery.WorkloadVolume{{Name: "backup", Claim: spec.BackupClaim, ClaimUID: spec.BackupClaimUID, PVUID: spec.BackupPVUID}}, spec.Target)
	volumes = append(volumes, spec.Retained...)
	for _, volume := range volumes {
		pvc := new(corev1.PersistentVolumeClaim)
		if err := e.Client.Get(ctx, client.ObjectKey{Namespace: spec.Namespace, Name: volume.Claim}, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: offline rebuild PVC %q is not available", ErrRecoveryWorkloadNotReady, volume.Claim)
			}
			return fmt.Errorf("read offline rebuild PVC %q: %w", volume.Claim, err)
		}
		if pvc.DeletionTimestamp != nil || pvc.UID != volume.ClaimUID || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
			return fmt.Errorf("offline rebuild PVC %q identity or binding changed", volume.Claim)
		}
		pv := new(corev1.PersistentVolume)
		if err := e.Client.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%w: offline rebuild PV %q is not available", ErrRecoveryWorkloadNotReady, pvc.Spec.VolumeName)
			}
			return fmt.Errorf("read offline rebuild PV %q: %w", pvc.Spec.VolumeName, err)
		}
		if pv.DeletionTimestamp != nil || pv.UID != volume.PVUID || pv.Status.Phase != corev1.VolumeBound || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != spec.Namespace || pv.Spec.ClaimRef.Name != volume.Claim || pv.Spec.ClaimRef.UID != pvc.UID {
			return fmt.Errorf("offline rebuild PV %q identity or claim binding changed", pv.Name)
		}
	}
	return nil
}

func hasRecoveryRequestOwnerConfigMap(configMap *corev1.ConfigMap, request *operatorv1alpha1.KubeOVNRecoveryRequest) bool {
	if len(configMap.OwnerReferences) != 1 {
		return false
	}
	return ownerReferenceEqual(configMap.OwnerReferences[0], recoveryRequestOwner(request))
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
	if len(desired.OwnerReferences) != 1 || len(current.OwnerReferences) != 1 || !ownerReferenceEqual(current.OwnerReferences[0], desired.OwnerReferences[0]) {
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

func ownerReferenceEqual(actual, expected metav1.OwnerReference) bool {
	if actual.APIVersion != expected.APIVersion || actual.Kind != expected.Kind || actual.Name != expected.Name || actual.UID != expected.UID {
		return false
	}
	return boolPointerEqual(actual.Controller, expected.Controller) && boolPointerEqual(actual.BlockOwnerDeletion, expected.BlockOwnerDeletion)
}

func boolPointerEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
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

func recoveryJobSuspended(job *batchv1.Job) bool {
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

func pinRecoveryJobStatus(ctx context.Context, kube client.Client, request *operatorv1alpha1.KubeOVNRecoveryRequest, job *batchv1.Job, inputDigest string) error {
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
	if execution.JobName == job.Name && execution.JobUID == job.UID && execution.JobPlanDigest == jobDigest && execution.JobInputIdentityDigest == inputDigest && execution.JobPhase == jobPhase && execution.JobReason == jobReason {
		return nil
	}
	execution.JobName, execution.JobUID, execution.JobPlanDigest = job.Name, job.UID, jobDigest
	execution.JobInputIdentityDigest = inputDigest
	execution.JobPhase, execution.JobReason = jobPhase, jobReason
	if err := kube.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return fmt.Errorf("%w: persist offline rebuild Job identity: %w", ErrRecoveryInProgress, err)
		}
		return fmt.Errorf("persist offline rebuild Job identity: %w", err)
	}
	return nil
}

func recoveryWorkloadInputIdentityDigest(spec recovery.WorkloadSpec) string {
	identities := []string{
		"configmap/plan=" + string(spec.PlanConfigMapUID),
		"configmap/plan-data=" + spec.PlanConfigMapDataDigest,
		"configmap/canonical-plan=" + spec.CanonicalPlanDigest,
		"configmap/proof=" + string(spec.IsolationProofConfigMapUID),
		"configmap/proof-data=" + spec.IsolationProofDataDigest,
		"pvc/backup=" + string(spec.BackupClaimUID),
		"pv/backup=" + string(spec.BackupPVUID),
		"pvc/target=" + string(spec.Target.ClaimUID),
		"pv/target=" + string(spec.Target.PVUID),
		"storage/target=" + fmt.Sprintf("%+v", spec.TargetStorage),
	}
	for _, volume := range spec.Retained {
		identities = append(identities, "pvc/retained/"+volume.Name+"="+string(volume.ClaimUID), "pv/retained/"+volume.Name+"="+string(volume.PVUID))
		identities = append(identities, "storage/retained/"+volume.Name+"="+fmt.Sprintf("%+v", spec.RetainedStorage[volume.Name]))
	}
	sort.Strings(identities)
	hash := sha256.Sum256([]byte(strings.Join(identities, "\n")))
	return fmt.Sprintf("sha256:%x", hash[:])
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
