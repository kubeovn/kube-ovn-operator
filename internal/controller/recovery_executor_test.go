package controller

import (
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateRecoveryApprovalForPlanAllowsOnlyInitialQuiescingAuditGap(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	plan := RecoveryPlan{
		RequestNamespace: "test", RequestName: "recovery", RequestUID: "request-uid", RequestGeneration: 3,
		InstallationName: "ovn", InstallationUID: "installation-uid", InstallationGeneration: 2,
		Mode: operatorv1alpha1.RecoveryModeReconvert, SourceVersion: "v1.16.0", TargetVersion: "v1.17.0",
		BackupRef: "sha256:" + strings.Repeat("a", 64), TopologyDigest: "sha256:" + strings.Repeat("b", 64), ChartDigest: "sha256:" + strings.Repeat("c", 64),
		ApprovalRef: &operatorv1alpha1.RecoveryApprovalReference{Name: "recovery-approval", UID: types.UID("approval-uid")},
	}
	record := &operatorv1alpha1.KubeOVNRecoveryApproval{
		ObjectMeta: metav1.ObjectMeta{Name: plan.ApprovalRef.Name, Namespace: plan.RequestNamespace, UID: plan.ApprovalRef.UID, Generation: 1, CreationTimestamp: metav1.NewTime(now.Add(-time.Minute))},
		Spec: operatorv1alpha1.RecoveryApprovalSpec{
			InstallationRef: operatorv1alpha1.InstallationReference{Name: plan.InstallationName, UID: plan.InstallationUID}, InstallationGeneration: plan.InstallationGeneration,
			RequestRef: &operatorv1alpha1.RecoveryRequestReference{Name: plan.RequestName, UID: plan.RequestUID, Generation: plan.RequestGeneration},
			Mode:       string(plan.Mode), SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, BackupRef: plan.BackupRef,
			TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest, Reason: "test approval", ExpiresAt: metav1.NewTime(now.Add(time.Hour)),
		},
	}
	scheme := controllerScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	if err := validateRecoveryApprovalForPlan(t.Context(), kube, plan, false); err != nil {
		t.Fatalf("initial quiescing admission rejected an unreserved approval: %v", err)
	}
	if err := validateRecoveryApprovalForPlan(t.Context(), kube, plan, true); err == nil {
		t.Fatal("later recovery phase accepted an approval without a reservation audit")
	}
	digest, err := approval.RecoveryApprovalDecisionDigest(record.Spec)
	if err != nil {
		t.Fatal(err)
	}
	reservedAt := metav1.NewTime(now)
	completedAt := metav1.NewTime(now.Add(time.Second))
	record.Status = operatorv1alpha1.RecoveryApprovalStatus{Phase: operatorv1alpha1.RecoveryApprovalCompleted, DecisionDigest: digest, ReservedAt: &reservedAt, CompletedAt: &completedAt}
	kube = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	if err := validateRecoveryApprovalForPlan(t.Context(), kube, plan, true); err != nil {
		t.Fatalf("completed approval was rejected for a retry: %v", err)
	}
}

func TestPinRecoveryOperationIDUsesPlanDigest(t *testing.T) {
	request := &lifecycle.Request{Add: &lifecycle.Addition{Workload: workload.Request{}}}
	firstDigest := "sha256:" + strings.Repeat("a", 64)
	if err := pinRecoveryOperationID(request, firstDigest); err != nil {
		t.Fatal(err)
	}
	first := request.OperationID
	if first == "" || request.Add.Workload.Preparation.OperationID != first {
		t.Fatalf("recovery operation ID was not pinned into the request: %+v", request)
	}
	if err := pinRecoveryOperationID(request, firstDigest); err != nil {
		t.Fatal(err)
	}
	if request.OperationID != first {
		t.Fatalf("same recovery plan digest produced a different operation ID: %q then %q", first, request.OperationID)
	}
	if err := pinRecoveryOperationID(request, "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if request.OperationID == first {
		t.Fatal("different recovery plan digests produced the same operation ID")
	}
}

func TestDestructiveRecoveryBackupTimeFallsBackToUpgradeCheckpoint(t *testing.T) {
	captured := time.Unix(100, 0).UTC()
	installation := recoveryInstallation()
	installation.Status.LastBackup = nil
	installation.Status.Upgrade.Checkpoint = &operatorv1alpha1.UpgradeCheckpointStatus{
		ID:            testBackupPairID(),
		DatabaseRefs:  testBackupPairRefs(),
		SourceVersion: installation.Status.ObservedVersion,
		TargetVersion: installation.Status.ObservedVersion,
		CapturedAt:    metav1.NewTime(captured),
		Phase:         operatorv1alpha1.UpgradeCheckpointVerified,
	}
	got, ok := destructiveRecoveryBackupTime(installation, RecoveryPlan{BackupRef: installation.Status.Upgrade.Checkpoint.ID})
	if !ok || !got.Equal(captured) {
		t.Fatalf("checkpoint backup time = %s, %t; want %s, true", got, ok, captured)
	}
	installation.Status.Upgrade.Checkpoint.ID = "sha256:" + strings.Repeat("f", 64)
	if _, ok := destructiveRecoveryBackupTime(installation, RecoveryPlan{BackupRef: installation.Status.Upgrade.Checkpoint.ID}); ok {
		t.Fatal("mismatched checkpoint pair was accepted for RPO calculation")
	}
}

func TestLifecycleRecoveryPersistsAndResumesPinnedOperation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Spec.Central.Replicas = 4
	request := recoveryRequest(now)
	objects := []client.Object{
		installation,
		request,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-recovery", UID: types.UID("node-recovery-uid")}, Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
			Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}},
		}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-recovery", Namespace: installation.Namespace, UID: types.UID("claim-recovery-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "volume-recovery"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "volume-recovery", UID: types.UID("volume-recovery-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "claim-recovery", Namespace: installation.Namespace, UID: types.UID("claim-recovery-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(objects...).Build()
	lifecycleFake := &lifecycleCoordinatorFake{
		reader: kube,
		reference: lifecycle.Reference{
			Journal: workload.Reference{Name: "journal", UID: types.UID("journal-uid")},
			Request: workload.Reference{Name: "request", UID: types.UID("request-uid")},
		},
		recovery: &lifecycle.Recovery{State: lifecycle.State{Phase: lifecycle.Complete}},
	}
	executor := &LifecycleRecoveryExecutor{
		Client:    kube,
		Leases:    lease.New(kube),
		Topology:  &lifecycleTopologyFake{retained: additionTopologyFixture()},
		Lifecycle: lifecycleFake,
	}
	plan, _, err := buildRecoveryPlan(request, installation, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Execute(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	first := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), first); err != nil {
		t.Fatal(err)
	}
	if first.Status.Execution == nil || first.Status.Execution.Operation == nil {
		t.Fatalf("first rejoin execution did not persist its lifecycle operation: %+v", first.Status)
	}
	if lifecycleFake.begin != 1 || lifecycleFake.resume != 1 {
		t.Fatalf("unexpected first rejoin lifecycle calls: begin=%d resume=%d", lifecycleFake.begin, lifecycleFake.resume)
	}
	firstOperation := *first.Status.Execution.Operation
	if len(lifecycleFake.resumeReferences) != 1 || lifecycleFake.resumeReferences[0].JournalName != firstOperation.JournalName || lifecycleFake.resumeReferences[0].RequestName != firstOperation.RequestName {
		t.Fatalf("first Resume did not receive the persisted operation reference: refs=%+v operation=%+v", lifecycleFake.resumeReferences, firstOperation)
	}

	if err := executor.Execute(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	second := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), second); err != nil {
		t.Fatal(err)
	}
	if lifecycleFake.begin != 1 || lifecycleFake.resume != 2 {
		t.Fatalf("retry began a new operation instead of resuming the pinned one: begin=%d resume=%d", lifecycleFake.begin, lifecycleFake.resume)
	}
	if second.Status.Execution == nil || second.Status.Execution.Operation == nil || *second.Status.Execution.Operation != firstOperation {
		t.Fatalf("retry changed the persisted lifecycle operation: first=%+v second=%+v", firstOperation, second.Status.Execution)
	}
}
