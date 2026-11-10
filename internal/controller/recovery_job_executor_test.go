package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRecoveryJobExecutorCreatesAndObservesOnePinnedJob(t *testing.T) {
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
	installation := recoveryInstallation()
	request := recoveryRequest(time.Unix(100, 0).UTC())
	request.Spec.Mode = operatorv1alpha1.RecoveryModeFreshBootstrap
	planDigest := "sha256:" + strings.Repeat("a", 64)
	request.Status.Plan = &operatorv1alpha1.RecoveryPlanStatus{Digest: planDigest, InstallationUID: installation.UID, RequestGeneration: request.Generation}
	planConfigMap, proofConfigMap := recoveryArtifactConfigMaps(request, planDigest)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(request).WithObjects(request, planConfigMap, proofConfigMap).Build()
	plan := RecoveryPlan{RequestNamespace: request.Namespace, RequestName: request.Name, RequestUID: request.UID, RequestGeneration: request.Generation, InstallationUID: installation.UID, Mode: request.Spec.Mode, Digest: planDigest}
	builder := offlineRebuildWorkloadBuilderFunc(func(_ context.Context, _ RecoveryPlan) (recovery.WorkloadSpec, error) {
		return recovery.WorkloadSpec{Namespace: request.Namespace, Name: "rebuild-member-recovery", InstallationUID: string(installation.UID), OperationID: "123e4567-e89b-12d3-a456-426614174000", PlanDigest: planDigest, MemberName: "member-recovery", NodeName: "node-recovery", Image: "example/central@sha256:" + strings.Repeat("b", 64), ServiceAccountName: "recovery", PlanConfigMapName: "rebuild-plan", IsolationProofConfigMap: "rebuild-proof", BackupClaim: "backup", Target: recovery.WorkloadVolume{Name: "target", Claim: "target"}, Retained: []recovery.WorkloadVolume{{Name: "retained", Claim: "retained", ReadOnly: true}}}, nil
	})
	executor := &RecoveryJobExecutor{Client: kube, Builder: builder}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatalf("new Job did not remain in progress: %v", err)
	}
	job := new(batchv1.Job)
	jobKey := client.ObjectKey{Namespace: request.Namespace, Name: "rebuild-member-recovery"}
	if err := kube.Get(t.Context(), jobKey, job); err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("job-uid")
	if err := kube.Update(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatalf("active Job did not remain in progress: %v", err)
	}
	stored := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), stored); err != nil {
		t.Fatal(err)
	}
	if stored.Status.Execution == nil || stored.Status.Execution.JobName != job.Name || stored.Status.Execution.JobUID != job.UID || stored.Status.Execution.JobPlanDigest != planDigest {
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
	request := recoveryRequest(time.Unix(100, 0).UTC())
	request.Spec.Mode = operatorv1alpha1.RecoveryModeFreshBootstrap
	planDigest := "sha256:" + strings.Repeat("c", 64)
	request.Status.Plan = &operatorv1alpha1.RecoveryPlanStatus{Digest: planDigest, InstallationUID: "installation-uid", RequestGeneration: request.Generation}
	planConfigMap, proofConfigMap := recoveryArtifactConfigMaps(request, planDigest)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(request).WithObjects(request, planConfigMap, proofConfigMap).Build()
	plan := RecoveryPlan{RequestNamespace: request.Namespace, RequestName: request.Name, RequestUID: request.UID, RequestGeneration: request.Generation, InstallationUID: "installation-uid", Mode: request.Spec.Mode, Digest: planDigest}
	builder := offlineRebuildWorkloadBuilderFunc(func(_ context.Context, _ RecoveryPlan) (recovery.WorkloadSpec, error) {
		return recovery.WorkloadSpec{Namespace: request.Namespace, Name: "rebuild-member-recovery", InstallationUID: "installation-uid", OperationID: "123e4567-e89b-12d3-a456-426614174000", PlanDigest: planDigest, MemberName: "member-recovery", NodeName: "node-recovery", Image: "example/central@sha256:" + strings.Repeat("d", 64), ServiceAccountName: "recovery", PlanConfigMapName: "rebuild-plan", IsolationProofConfigMap: "rebuild-proof", BackupClaim: "backup", Target: recovery.WorkloadVolume{Name: "target", Claim: "target"}, Retained: []recovery.WorkloadVolume{{Name: "retained", Claim: "retained", ReadOnly: true}}}, nil
	})
	executor := &RecoveryJobExecutor{Client: kube, Builder: builder}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatal(err)
	}
	job := new(batchv1.Job)
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: request.Namespace, Name: "rebuild-member-recovery"}, job); err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("original-job")
	if err := kube.Update(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != ErrRecoveryInProgress {
		t.Fatal(err)
	}
	if err := kube.Delete(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	recreated := job.DeepCopy()
	recreated.ResourceVersion = ""
	recreated.UID = types.UID("replacement-job")
	if err := kube.Create(t.Context(), recreated); err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("recreated Job was not rejected: %v", err)
	}
}

func recoveryArtifactConfigMaps(request *operatorv1alpha1.KubeOVNRecoveryRequest, digest string) (*corev1.ConfigMap, *corev1.ConfigMap) {
	controller := true
	owner := metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNRecoveryRequest", Name: request.Name, UID: request.UID, Controller: &controller}
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rebuild-plan", Namespace: request.Namespace, OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: digest}}, Data: map[string]string{"plan.json": "{}"}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "rebuild-proof", Namespace: request.Namespace, OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: digest}}, Data: map[string]string{"proof.json": "{}"}}
}
