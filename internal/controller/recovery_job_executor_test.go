package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func recoveryJobTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestRecoveryJobExecutorTreatsMissingArtifactsAsNotReady(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).WithObjects(request).Build()
	builder := offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })
	executor := &RecoveryJobExecutor{Client: kube, Builder: builder}
	err := executor.Execute(t.Context(), plan)
	if !errors.Is(err, ErrRecoveryWorkloadNotReady) {
		t.Fatalf("missing recovery artifact was not retryable: %v", err)
	}
	job := new(batchv1.Job)
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: request.Namespace, Name: spec.Name}, job); err == nil {
		t.Fatal("executor created a Job before recovery artifacts were ready")
	}
}

func TestRecoveryJobExecutorCreatesAndObservesOnePinnedJob(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	planConfigMap, proofConfigMap := recoveryArtifactConfigMaps(request, plan.Digest)
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).WithObjects(append([]client.Object{request, planConfigMap, proofConfigMap}, recoveryStorageObjects(spec)...)...).WithInterceptorFuncs(interceptor.Funcs{Create: assignRecoveryJobUID}).Build()
	builder := offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })
	executor := &RecoveryJobExecutor{Client: kube, Builder: builder}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatalf("new Job did not remain in progress: %v", err)
	}
	job := new(batchv1.Job)
	jobKey := client.ObjectKey{Namespace: request.Namespace, Name: spec.Name}
	if err := kube.Get(t.Context(), jobKey, job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatalf("active Job did not remain in progress: %v", err)
	}
	stored := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Execution == nil || stored.Status.Execution.JobName != job.Name || stored.Status.Execution.JobUID != job.UID || stored.Status.Execution.JobPlanDigest != plan.Digest || stored.Status.Execution.JobInputIdentityDigest != recoveryWorkloadInputIdentityDigest(spec) {
		t.Fatalf("Job identity was not durably pinned: %+v", stored.Status.Execution)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := kube.Status().Update(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != nil {
		t.Fatalf("completed Job was not accepted: %v", err)
	}
}

func TestRecoveryJobExecutorRejectsRecreatedJob(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	planConfigMap, proofConfigMap := recoveryArtifactConfigMaps(request, plan.Digest)
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).WithObjects(append([]client.Object{request, planConfigMap, proofConfigMap}, recoveryStorageObjects(spec)...)...).WithInterceptorFuncs(interceptor.Funcs{Create: assignRecoveryJobUID}).Build()
	builder := offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })
	executor := &RecoveryJobExecutor{Client: kube, Builder: builder}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatal(err)
	}
	job := new(batchv1.Job)
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: request.Namespace, Name: spec.Name}, job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatal(err)
	}
	// A different builder result must be rejected before it creates a second Job.
	executor.Builder = offlineRebuildWorkloadBuilderFunc(func(ctx context.Context, plan RecoveryPlan) (recovery.WorkloadSpec, error) {
		spec, err := builder.Build(ctx, plan)
		spec.Name += "-new"
		return spec, err
	})
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "derived from the plan digest") {
		t.Fatalf("changed Job name was not rejected: %v", err)
	}
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: job.Namespace, Name: job.Name + "-new"}, new(batchv1.Job)); !apierrors.IsNotFound(err) {
		t.Fatalf("changed builder output created a second Job: %v", err)
	}
	executor.Builder = builder
	if err := kube.Delete(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Fatalf("missing pinned Job was not rejected: %v", err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(job), new(batchv1.Job)); !apierrors.IsNotFound(err) {
		t.Fatalf("executor recreated the missing pinned Job: %v", err)
	}
	recreated := job.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = "replacement-job"
	if err := kube.Create(t.Context(), recreated); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("recreated Job was not rejected: %v", err)
	}
}

func recoveryArtifactConfigMaps(request *operatorv1alpha1.KubeOVNRecoveryRequest, digest string) (*corev1.ConfigMap, *corev1.ConfigMap) {
	owner := recoveryRequestOwner(request)
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rebuild-plan", Namespace: request.Namespace, UID: "plan-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: digest}}, Immutable: new(true), Data: map[string]string{"plan.json": "{}"}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rebuild-proof", Namespace: request.Namespace, UID: "proof-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: digest}}, Immutable: new(true), Data: map[string]string{"proof.json": "{}"}}
}

func recoveryStorageObjects(spec recovery.WorkloadSpec) []client.Object {
	volumes := append([]recovery.WorkloadVolume{{Name: "backup", Claim: spec.BackupClaim, ClaimUID: spec.BackupClaimUID, PVUID: spec.BackupPVUID}}, spec.Target)
	volumes = append(volumes, spec.Retained...)
	objects := make([]client.Object, 0, len(volumes)*2)
	for _, volume := range volumes {
		pvName := "pv-" + volume.Claim
		objects = append(objects,
			&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName, UID: volume.PVUID}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Namespace: spec.Namespace, Name: volume.Claim, UID: volume.ClaimUID}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: volume.Claim, Namespace: spec.Namespace, UID: volume.ClaimUID}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pvName}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		)
	}
	return objects
}

func TestRecoveryJobExecutorRejectsUnsafeHandoff(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*corev1.ConfigMap, *batchv1.Job) []client.Object
		want string
	}{
		{name: "mutable plan", edit: func(plan *corev1.ConfigMap, _ *batchv1.Job) []client.Object {
			plan.Immutable = new(false)
			return nil
		}, want: "must be immutable"},
		{name: "wrong plan content", edit: func(plan *corev1.ConfigMap, _ *batchv1.Job) []client.Object {
			plan.Data["plan.json"] = `{"wrong":true}`
			return nil
		}, want: "not bound to plan"},
		{name: "foreign ConfigMap owner", edit: func(plan *corev1.ConfigMap, _ *batchv1.Job) []client.Object {
			plan.OwnerReferences = append(plan.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "foreign", UID: "foreign"})
			return nil
		}, want: "owner identity"},
		{name: "wrong image", edit: func(_ *corev1.ConfigMap, job *batchv1.Job) []client.Object {
			job.Spec.Template.Spec.Containers[0].Image = "other:latest"
			return []client.Object{job}
		}, want: "spec differs"},
		{name: "wrong PVC", edit: func(_ *corev1.ConfigMap, job *batchv1.Job) []client.Object {
			job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "other-pvc"
			return []client.Object{job}
		}, want: "spec differs"},
		{name: "extra sidecar", edit: func(_ *corev1.ConfigMap, job *batchv1.Job) []client.Object {
			job.Spec.Template.Spec.Containers = append(job.Spec.Template.Spec.Containers, corev1.Container{Name: "other", Image: "other:latest"})
			return []client.Object{job}
		}, want: "spec differs"},
		{name: "foreign owner", edit: func(_ *corev1.ConfigMap, job *batchv1.Job) []client.Object {
			job.OwnerReferences = append(job.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "foreign", UID: "foreign"})
			return []client.Object{job}
		}, want: "owner identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, plan, spec := recoveryJobFixture()
			planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
			job, err := spec.Job(new(recoveryRequestOwner(request)))
			if err != nil {
				t.Fatal(err)
			}
			job.UID = "existing-job"
			job.Spec.Suspend = new(true)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			extra := tc.edit(planConfigMap, job)
			kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).
				WithObjects(append(append([]client.Object{request, planConfigMap, proof}, recoveryStorageObjects(spec)...), extra...)...).Build()
			executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) {
				return spec, nil
			})}
			if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unsafe handoff was not rejected: %v", err)
			}
			jobs := new(batchv1.JobList)
			if err := kube.List(t.Context(), jobs); err != nil || len(jobs.Items) != len(extra) {
				t.Fatalf("unexpected Jobs after rejection: %d, %v", len(jobs.Items), err)
			}
			if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), request); err != nil || request.Status.Execution != nil {
				t.Fatalf("unsafe Job was pinned: %+v, %v", request.Status.Execution, err)
			}
		})
	}
}

func TestRecoveryJobExecutorRejectsReplacedInputIdentity(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
	objects := append([]client.Object{request, planConfigMap, proof}, recoveryStorageObjects(spec)...)
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).
		WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{Create: assignRecoveryJobUID}).Build()
	executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatal(err)
	}
	if err := kube.Delete(t.Context(), planConfigMap); err != nil {
		t.Fatal(err)
	}
	replacedPlan := planConfigMap.DeepCopy()
	replacedPlan.ResourceVersion = ""
	replacedPlan.UID = "replacement-plan"
	if err := kube.Create(t.Context(), replacedPlan); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "must be immutable") {
		t.Fatalf("replaced ConfigMap was accepted: %v", err)
	}
	if err := kube.Delete(t.Context(), replacedPlan); err != nil {
		t.Fatal(err)
	}
	originalPlan := planConfigMap.DeepCopy()
	originalPlan.ResourceVersion = ""
	if err := kube.Create(t.Context(), originalPlan); err != nil {
		t.Fatal(err)
	}
	pvc := new(corev1.PersistentVolumeClaim)
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: spec.Namespace, Name: spec.Target.Claim}, pvc); err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(t.Context(), pvc); err != nil {
		t.Fatal(err)
	}
	replacedPVC := pvc.DeepCopy()
	replacedPVC.ResourceVersion = ""
	replacedPVC.UID = "replacement-target"
	if err := kube.Create(t.Context(), replacedPVC); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "identity or binding changed") {
		t.Fatalf("replaced PVC was accepted: %v", err)
	}
}

func TestRecoveryJobExecutorRejectsUnboundOrDeletingStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
	}{
		{name: "deleting PVC", edit: func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
			now := metav1.Now()
			pvc.DeletionTimestamp = &now
			pvc.Finalizers = []string{"test.kubeovn.io/hold"}
		}},
		{name: "unbound PV", edit: func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
			pv.Status.Phase = corev1.VolumeReleased
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, plan, spec := recoveryJobFixture()
			planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
			storage := recoveryStorageObjects(spec)
			pvc := storage[3].(*corev1.PersistentVolumeClaim)
			pv := storage[2].(*corev1.PersistentVolume)
			tc.edit(pvc, pv)
			kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).
				WithObjects(append([]client.Object{request, planConfigMap, proof}, storage...)...).Build()
			executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })}
			if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "changed") {
				t.Fatalf("unsafe storage was accepted: %v", err)
			}
		})
	}
}

func TestRecoveryJobExecutorWaitsForTerminalJobIdentityToBePersisted(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
	job, err := spec.Job(new(recoveryRequestOwner(request)))
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "completed-job"
	request.Status.Execution = &operatorv1alpha1.RecoveryExecutionStatus{JobName: job.Name, JobUID: job.UID, JobPlanDigest: plan.Digest}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	conflict := true
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).WithObjects(append([]client.Object{request, planConfigMap, proof, job}, recoveryStorageObjects(spec)...)...).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, kube client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if conflict {
				conflict = false
				return apierrors.NewConflict(operatorv1alpha1.GroupVersion.WithResource("kubeovnrecoveryrequests").GroupResource(), obj.GetName(), errors.New("concurrent status change"))
			}
			return kube.SubResource(sub).Update(ctx, obj, opts...)
		}}).Build()
	executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) {
		return spec, nil
	})}
	if err := executor.Execute(t.Context(), plan); !errors.Is(err, ErrRecoveryInProgress) {
		t.Fatalf("terminal Job was accepted before pinning its identity: %v", err)
	}
	if err := executor.Execute(t.Context(), plan); err != nil {
		t.Fatalf("terminal Job was not accepted after retry: %v", err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), request); err != nil || request.Status.Execution == nil || request.Status.Execution.JobUID != job.UID {
		t.Fatalf("terminal Job UID was not persisted: %+v, %v", request.Status.Execution, err)
	}
}

func recoveryJobFixture() (*operatorv1alpha1.KubeOVNRecoveryRequest, RecoveryPlan, recovery.WorkloadSpec) {
	request := recoveryRequest(time.Unix(100, 0).UTC())
	request.Spec.Mode = operatorv1alpha1.RecoveryModeFreshBootstrap
	digest := "sha256:" + strings.Repeat("a", 64)
	request.Status.Plan = &operatorv1alpha1.RecoveryPlanStatus{Digest: digest, InstallationUID: "installation-uid", RequestGeneration: request.Generation}
	plan := RecoveryPlan{RequestNamespace: request.Namespace, RequestName: request.Name, RequestUID: request.UID, RequestGeneration: request.Generation, InstallationUID: "installation-uid", Mode: request.Spec.Mode, Digest: digest}
	spec := recovery.WorkloadSpec{Namespace: request.Namespace, Name: recovery.DeterministicRebuildJobName(digest), InstallationUID: string(plan.InstallationUID), OperationID: "123e4567-e89b-12d3-a456-426614174000", PlanDigest: digest, MemberName: "member-recovery", NodeName: "node-recovery", Image: "example/central@sha256:" + strings.Repeat("b", 64), ServiceAccountName: "recovery", PlanConfigMapName: "rebuild-plan", PlanConfigMapUID: "plan-uid", PlanConfigMapDataDigest: recovery.ConfigMapDataDigest("{}"), IsolationProofConfigMap: "rebuild-proof", IsolationProofConfigMapUID: "proof-uid", IsolationProofDataDigest: recovery.ConfigMapDataDigest("{}"), BackupClaim: "backup", BackupClaimUID: "backup-uid", BackupPVUID: "backup-pv-uid", Target: recovery.WorkloadVolume{Name: "target", Claim: "target", ClaimUID: "target-uid", PVUID: "target-pv-uid"}, Retained: []recovery.WorkloadVolume{{Name: "retained", Claim: "retained", ClaimUID: "retained-uid", PVUID: "retained-pv-uid", ReadOnly: true}}}
	return request, plan, spec
}

func assignRecoveryJobUID(ctx context.Context, kube client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
	if job, ok := obj.(*batchv1.Job); ok && job.UID == "" {
		job.UID = "job-uid"
	}
	return kube.Create(ctx, obj, opts...)
}

func TestRecoveryJobExecutorActivatesOnlyAfterPin(t *testing.T) {
	request, plan, spec := recoveryJobFixture()
	planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
	conflict := true
	kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).
		WithObjects(append([]client.Object{request, planConfigMap, proof}, recoveryStorageObjects(spec)...)...).WithInterceptorFuncs(interceptor.Funcs{
		Create: assignRecoveryJobUID,
		SubResourceUpdate: func(ctx context.Context, kube client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if conflict {
				conflict = false
				return apierrors.NewConflict(operatorv1alpha1.GroupVersion.WithResource("kubeovnrecoveryrequests").GroupResource(), obj.GetName(), errors.New("concurrent status change"))
			}
			return kube.SubResource(sub).Update(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, kube client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if job, ok := obj.(*batchv1.Job); ok && !recoveryJobSuspended(job) {
				stored := new(operatorv1alpha1.KubeOVNRecoveryRequest)
				if err := kube.Get(ctx, client.ObjectKeyFromObject(request), stored); err != nil {
					t.Fatal(err)
				}
				if stored.Status.Execution == nil || stored.Status.Execution.JobUID != job.UID {
					t.Fatal("Job was activated before its UID was persisted")
				}
			}
			if err := kube.Update(ctx, obj, opts...); err != nil {
				return err
			}
			return apierrors.NewTimeoutError("activation response lost", 0)
		},
	}).Build()
	executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) {
		return spec, nil
	})}
	key := client.ObjectKey{Namespace: spec.Namespace, Name: spec.Name}
	if err := executor.Execute(t.Context(), plan); !errors.Is(err, ErrRecoveryInProgress) {
		t.Fatalf("status conflict did not leave the operation in progress: %v", err)
	}
	job := new(batchv1.Job)
	if err := kube.Get(t.Context(), key, job); err != nil || !recoveryJobSuspended(job) {
		t.Fatalf("Job did not remain suspended across conflict: %v", err)
	}
	uid := job.UID
	if err := executor.Execute(t.Context(), plan); !errors.Is(err, ErrRecoveryInProgress) {
		t.Fatalf("retry did not activate the pinned Job: %v", err)
	}
	if err := kube.Get(t.Context(), key, job); err != nil || job.UID != uid || recoveryJobSuspended(job) {
		t.Fatalf("retry did not resume the same Job: %v", err)
	}
}

func TestRecoveryJobExecutorDoesNotRetryDeterministicActivationRejection(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  func(string) error
	}{
		{name: "forbidden", err: func(name string) error {
			return apierrors.NewForbidden(batchv1.Resource("jobs"), name, errors.New("admission policy denied suspended Job update"))
		}},
		{name: "invalid", err: func(name string) error {
			return apierrors.NewInvalid(batchv1.SchemeGroupVersion.WithKind("Job").GroupKind(), name, nil)
		}},
		{name: "bad request", err: func(string) error { return apierrors.NewBadRequest("job suspend update is malformed") }},
		{name: "not acceptable", err: func(name string) error {
			return apierrors.NewGenericServerResponse(406, "update", batchv1.SchemeGroupVersion.WithResource("jobs").GroupResource(), name, "not acceptable", 0, false)
		}},
		{name: "unsupported media type", err: func(name string) error {
			return apierrors.NewGenericServerResponse(415, "update", batchv1.SchemeGroupVersion.WithResource("jobs").GroupResource(), name, "unsupported media type", 0, false)
		}},
		{name: "request entity too large", err: func(string) error {
			return apierrors.NewRequestEntityTooLargeError("request entity too large")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, plan, spec := recoveryJobFixture()
			planConfigMap, proof := recoveryArtifactConfigMaps(request, plan.Digest)
			kube := fake.NewClientBuilder().WithScheme(recoveryJobTestScheme(t)).WithStatusSubresource(request).
				WithObjects(append([]client.Object{request, planConfigMap, proof}, recoveryStorageObjects(spec)...)...).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: assignRecoveryJobUID,
					Update: func(ctx context.Context, kube client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						if job, ok := obj.(*batchv1.Job); ok && !recoveryJobSuspended(job) {
							return tc.err(job.Name)
						}
						return kube.Update(ctx, obj, opts...)
					},
				}).Build()
			executor := &RecoveryJobExecutor{Client: kube, Builder: offlineRebuildWorkloadBuilderFunc(func(context.Context, RecoveryPlan) (recovery.WorkloadSpec, error) { return spec, nil })}
			err := executor.Execute(t.Context(), plan)
			if err == nil || errors.Is(err, ErrRecoveryInProgress) {
				t.Fatalf("deterministic activation rejection was treated as retryable: %v", err)
			}
			job := new(batchv1.Job)
			if getErr := kube.Get(t.Context(), client.ObjectKey{Namespace: spec.Namespace, Name: spec.Name}, job); getErr != nil {
				t.Fatal(getErr)
			}
			if !recoveryJobSuspended(job) {
				t.Fatal("rejected Job was not left suspended")
			}
		})
	}
}
