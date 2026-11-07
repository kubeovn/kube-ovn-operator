package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestRecoveryRequestBuildsStablePlanAndWaitsForExplicitExecutor(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	req := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	first := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), req.NamespacedName, first); err != nil {
		t.Fatal(err)
	}
	if first.Status.Phase != operatorv1alpha1.RecoveryPhaseAwaitingExecution || first.Status.Plan == nil || first.Status.Plan.Digest == "" {
		t.Fatalf("valid request was not durably queued: %+v", first.Status)
	}
	if first.Status.Plan.TargetMemberRef != "member-recovery" || first.Status.Plan.TargetMemberDigest == "" || first.Status.Plan.TopologyDigest == "" {
		t.Fatalf("rejoin plan did not retain target identity: %+v", first.Status.Plan)
	}
	if condition := findCondition(first.Status.Conditions, "Accepted"); condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "PlanValidated" {
		t.Fatalf("request was not accepted as a validated plan: %+v", first.Status.Conditions)
	}
	transition := findCondition(first.Status.Conditions, "Accepted").LastTransitionTime
	digest := first.Status.Plan.Digest
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	second := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), req.NamespacedName, second); err != nil {
		t.Fatal(err)
	}
	if second.Status.Plan.Digest != digest || !findCondition(second.Status.Conditions, "Accepted").LastTransitionTime.Equal(&transition) {
		t.Fatalf("reconciliation changed an immutable recovery plan: first=%+v second=%+v", first.Status, second.Status)
	}
}

func TestRecoveryRequestRequiresVerifiedBackupForRestoreModes(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	for _, mode := range []operatorv1alpha1.RecoveryMode{operatorv1alpha1.RecoveryModeReconvert, operatorv1alpha1.RecoveryModeFreshBootstrap} {
		t.Run(string(mode), func(t *testing.T) {
			scheme := controllerScheme(t)
			installation := recoveryInstallation()
			request := recoveryRequest(now)
			request.Spec.Mode = mode
			request.Spec.TargetMemberRef = ""
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
			reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
			if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
				t.Fatal(err)
			}
			got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
			if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "BackupRequired" {
				t.Fatalf("restore request passed without a backup pair: %+v", got.Status)
			}
		})
	}
}

func TestRecoveryRequestRejectsMismatchedBackupPair(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Digest: "sha256:topology"}
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Spec.Version, TopologyDigest: "sha256:topology",
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = "sha256:other-pair"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "BackupMismatch" {
		t.Fatalf("mismatched backup pair was accepted: %+v", got.Status)
	}
}

func TestRecoveryRequestRejectsBackupPairIDMismatch(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Spec.Chart.Digest = "sha256:" + strings.Repeat("c", 64)
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Digest: "sha256:topology"}
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: "sha256:" + strings.Repeat("d", 64), Version: installation.Status.ObservedVersion, TopologyDigest: "sha256:topology",
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "BackupMismatch" {
		t.Fatalf("backup pair with mismatched content-addressed ID was accepted: %+v", got.Status)
	}
}

func TestRecoveryPlanBindsVersionsChartAndDatabasePair(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Spec.Chart.Digest = "sha256:" + strings.Repeat("c", 64)
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Digest: "sha256:topology"}
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: "sha256:topology",
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	plan := got.Status.Plan
	if plan == nil || plan.SourceVersion != "v1.17.0" || plan.TargetVersion != "v1.17.0" || plan.ChartDigest != installation.Spec.Chart.Digest || plan.DatabaseRefs["OVN_Northbound"] != "sha256:nb" || plan.DatabaseRefs["OVN_Southbound"] != "sha256:sb" {
		t.Fatalf("recovery plan lost version or pair binding: %+v", plan)
	}
}

func TestRecoveryPlanUsesVerifiedUpgradeCheckpointWhenLastBackupIsAbsent(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Spec.Chart.Digest = "sha256:" + strings.Repeat("c", 64)
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Upgrade.Checkpoint = &operatorv1alpha1.UpgradeCheckpointStatus{
		ID: testBackupPairID(), SourceVersion: installation.Status.ObservedVersion, TargetVersion: "v1.18.0",
		DatabaseRefs:   testBackupPairRefs(),
		TopologyDigest: installation.Status.Topology.Inventory.Digest, CapturedAt: metav1.NewTime(now), Phase: operatorv1alpha1.UpgradeCheckpointVerified,
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.Upgrade.Checkpoint.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseAwaitingExecution || got.Status.Plan == nil || got.Status.Plan.BackupRef != request.Spec.BackupRef || got.Status.Plan.DatabaseRefs["OVN_Northbound"] != "sha256:nb" || got.Status.Plan.DatabaseRefs["OVN_Southbound"] != "sha256:sb" {
		t.Fatalf("verified upgrade checkpoint was not accepted as the recovery pair: %+v", got.Status)
	}
}

func TestRecoveryPlanRejectsFailedUpgradeCheckpoint(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Upgrade.Checkpoint = &operatorv1alpha1.UpgradeCheckpointStatus{
		ID: "sha256:failed-pair", SourceVersion: installation.Status.ObservedVersion, TargetVersion: "v1.18.0",
		DatabaseRefs:   testBackupPairRefs(),
		TopologyDigest: installation.Status.Topology.Inventory.Digest, CapturedAt: metav1.NewTime(now), Phase: operatorv1alpha1.UpgradeCheckpointFailed,
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.Upgrade.Checkpoint.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "BackupMismatch" {
		t.Fatalf("failed upgrade checkpoint was accepted as a recovery pair: %+v", got.Status)
	}
}

func TestRecoveryPlanBindsDestructiveApprovalReference(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Spec.Chart.Digest = "sha256:" + strings.Repeat("c", 64)
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Digest: "sha256:topology"}
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: "sha256:topology",
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	request.Spec.ApprovalRef = &operatorv1alpha1.RecoveryApprovalReference{Name: "recovery-approval", UID: types.UID("approval-uid")}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	accepted := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key.NamespacedName, accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Status.Phase != operatorv1alpha1.RecoveryPhaseAwaitingExecution || accepted.Status.Plan == nil || accepted.Status.Plan.ApprovalRef == nil || *accepted.Status.Plan.ApprovalRef != *request.Spec.ApprovalRef {
		t.Fatalf("destructive approval reference was not pinned in the plan: %+v", accepted.Status)
	}

	accepted.Spec.ApprovalRef.UID = types.UID("replacement-approval-uid")
	if err := kube.Update(t.Context(), accepted); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	changed := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key.NamespacedName, changed); err != nil {
		t.Fatal(err)
	}
	if changed.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(changed.Status.Conditions, "Accepted").Reason != "PlanChanged" {
		t.Fatalf("approval reference drift was not fenced: %+v", changed.Status)
	}
}

func TestRecoveryRequestRejectsApprovalForRejoin(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	request.Spec.ApprovalRef = &operatorv1alpha1.RecoveryApprovalReference{Name: "recovery-approval", UID: types.UID("approval-uid")}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "ApprovalUnsupported" {
		t.Fatalf("rejoin accepted a destructive approval: %+v", got.Status)
	}
}

func TestRecoveryRequestRejectsExpiredAndRecreatedInstallation(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	request.Spec.ExpiresAt = metav1.NewTime(now.Add(-time.Second))
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	key := client.ObjectKeyFromObject(request)
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	expired := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, expired); err != nil {
		t.Fatal(err)
	}
	if expired.Status.Phase != operatorv1alpha1.RecoveryPhaseExpired || findCondition(expired.Status.Conditions, "Accepted").Reason != "Expired" {
		t.Fatalf("expired request was not rejected: %+v", expired.Status)
	}

	request = recoveryRequest(now)
	installation = recoveryInstallation()
	kube = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler.Client = kube
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	current := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Plan == nil {
		t.Fatal("initial recovery plan was not persisted")
	}
	installation.UID = types.UID("recreated-installation")
	if err := kube.Update(t.Context(), installation); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	changed := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, changed); err != nil {
		t.Fatal(err)
	}
	if changed.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(changed.Status.Conditions, "Accepted").Reason != "PlanChanged" {
		t.Fatalf("recreated installation was not fenced: %+v", changed.Status)
	}
}

func TestRecoveryRequestRejectsTargetMemberDrift(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	key := client.ObjectKeyFromObject(request)
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(installation), installation); err != nil {
		t.Fatal(err)
	}
	installation.Spec.Central.MemberRequests[0].NodeName = "node-replaced"
	if err := kube.Update(t.Context(), installation); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "PlanChanged" {
		t.Fatalf("target member drift was not fenced: %+v", got.Status)
	}
}

func TestRecoveryRequestRequiresExplicitRejoinTarget(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	request.Spec.TargetMemberRef = ""
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || findCondition(got.Status.Conditions, "Accepted").Reason != "TargetRequired" {
		t.Fatalf("rejoin without target was accepted: %+v", got.Status)
	}
}

func TestRecoveryRequestExecutorRunsOnlyAfterDurablePlan(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	var executed RecoveryPlan
	executor := recoveryExecutorFunc(func(_ context.Context, plan RecoveryPlan) error {
		executed = plan
		return nil
	})
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: executor}
	key := client.ObjectKeyFromObject(request)
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if executed.Digest == "" || executed.RequestUID != request.UID || executed.InstallationUID != installation.UID {
		t.Fatalf("executor received an unbound plan: %+v", executed)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseSucceeded || got.Status.CompletedAt == nil {
		t.Fatalf("successful execution was not recorded: %+v", got.Status)
	}
}

func TestRecoveryRequestKeepsExecutingWhileLifecycleConverges(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	request := recoveryRequest(now)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: recoveryExecutorFunc(func(context.Context, RecoveryPlan) error { return ErrRecoveryInProgress })}
	key := client.ObjectKeyFromObject(request)
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("in-progress recovery was not requeued: %+v", result)
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseExecuting || findCondition(got.Status.Conditions, "ExecutionReady").Reason != "ExecutionInProgress" {
		t.Fatalf("in-progress recovery became terminal: %+v", got.Status)
	}
}

func TestDestructiveRecoveryPersistsRestartableExecutionPhases(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: installation.Status.Topology.Inventory.Digest,
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	executor := &rollbackExecutorFake{}
	lifecycleExecutor := &LifecycleRecoveryExecutor{Client: kube, Leases: lease.New(kube), Topology: &lifecycleTopologyFake{retained: additionTopologyFixture()}, Lifecycle: &lifecycleCoordinatorFake{reader: kube}, Rollback: executor}
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: lifecycleExecutor}
	key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
	for range 6 {
		if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseSucceeded || got.Status.Execution == nil || got.Status.Execution.Phase != operatorv1alpha1.RollbackPhaseSucceeded || got.Status.Execution.RPOSeconds <= 0 {
		t.Fatalf("destructive recovery did not persist execution completion: %+v", got.Status)
	}
	if !slices.Equal(executor.phases, []operatorv1alpha1.RollbackPhase{operatorv1alpha1.RollbackPhaseQuiescing, operatorv1alpha1.RollbackPhaseRestoring, operatorv1alpha1.RollbackPhaseWorkloads, operatorv1alpha1.RollbackPhaseVerifying}) {
		t.Fatalf("destructive recovery phases were not restartable: %+v", executor.phases)
	}
}

func TestDestructiveRecoveryPersistsRestoreFailureAndStopsRetrying(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: installation.Status.Topology.Inventory.Digest,
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	executor := &rollbackExecutorFake{failPhase: operatorv1alpha1.RollbackPhaseRestoring, failErr: fmt.Errorf("restore rejected by database fence")}
	lifecycleExecutor := &LifecycleRecoveryExecutor{Client: kube, Leases: lease.New(kube), Topology: &lifecycleTopologyFake{retained: additionTopologyFixture()}, Lifecycle: &lifecycleCoordinatorFake{reader: kube}, Rollback: executor}
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: lifecycleExecutor}
	key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
	for range 3 {
		if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	failed := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key.NamespacedName, failed); err != nil {
		t.Fatal(err)
	}
	if failed.Status.Phase != operatorv1alpha1.RecoveryPhaseFailed || failed.Status.Execution == nil || failed.Status.Execution.Phase != operatorv1alpha1.RollbackPhaseFailed {
		t.Fatalf("restore failure was not persisted as terminal: %+v", failed.Status)
	}
	if condition := findCondition(failed.Status.Conditions, "Accepted"); condition == nil || condition.Reason != "ExecutionFailed" {
		t.Fatalf("restore failure condition was not persisted: %+v", failed.Status.Conditions)
	}
	if !slices.Equal(executor.phases, []operatorv1alpha1.RollbackPhase{operatorv1alpha1.RollbackPhaseQuiescing, operatorv1alpha1.RollbackPhaseRestoring}) {
		t.Fatalf("restore failure advanced into a later phase: %+v", executor.phases)
	}
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(executor.phases, []operatorv1alpha1.RollbackPhase{operatorv1alpha1.RollbackPhaseQuiescing, operatorv1alpha1.RollbackPhaseRestoring}) {
		t.Fatalf("terminal restore failure was retried: %+v", executor.phases)
	}
}

func TestDestructiveRecoveryPersistsPendingVerificationReason(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	scheme := controllerScheme(t)
	installation := recoveryInstallation()
	installation.Status.ObservedVersion = installation.Spec.Version
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: installation.Status.Topology.Inventory.Digest,
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
	request.Spec.TargetMemberRef = ""
	request.Spec.BackupRef = installation.Status.LastBackup.ID
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	executor := &rollbackExecutorFake{failPhase: operatorv1alpha1.RollbackPhaseVerifying, failErr: fmt.Errorf("%w: northd NorthdUnhealthy: northd disconnected", ErrRollbackInProgress)}
	lifecycleExecutor := &LifecycleRecoveryExecutor{Client: kube, Leases: lease.New(kube), Topology: &lifecycleTopologyFake{retained: additionTopologyFixture()}, Lifecycle: &lifecycleCoordinatorFake{reader: kube}, Rollback: executor}
	reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: lifecycleExecutor}
	key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
	for range 6 {
		if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	got := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := kube.Get(t.Context(), key.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.RecoveryPhaseExecuting || got.Status.Execution.Phase != operatorv1alpha1.RollbackPhaseVerifying || !strings.Contains(got.Status.Execution.Reason, "northd disconnected") {
		t.Fatalf("pending verification lost its durable retryable reason: %+v", got.Status)
	}
}

func TestDestructiveRecoveryRetriesLeaseContention(t *testing.T) {
	for _, phase := range []operatorv1alpha1.RollbackPhase{operatorv1alpha1.RollbackPhaseQuiescing, operatorv1alpha1.RollbackPhaseRestoring, operatorv1alpha1.RollbackPhaseWorkloads, operatorv1alpha1.RollbackPhaseVerifying} {
		t.Run(string(phase), func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			scheme := controllerScheme(t)
			installation := recoveryInstallation()
			installation.Status.ObservedVersion = installation.Spec.Version
			installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
				ID: testBackupPairID(), Version: installation.Status.ObservedVersion, TopologyDigest: installation.Status.Topology.Inventory.Digest,
				DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(now),
			}
			request := recoveryRequest(now)
			request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
			request.Spec.TargetMemberRef = ""
			request.Spec.BackupRef = installation.Status.LastBackup.ID
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
			steps := &rollbackExecutorFake{}
			executor := &LifecycleRecoveryExecutor{Client: kube, Leases: lease.New(kube), Topology: &lifecycleTopologyFake{retained: additionTopologyFixture()}, Lifecycle: &lifecycleCoordinatorFake{reader: kube}, Rollback: steps}
			reconciler := &RecoveryRequestReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }, Executor: executor}
			key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
			if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			stored := new(operatorv1alpha1.KubeOVNRecoveryRequest)
			if err := kube.Get(t.Context(), key.NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			digest := stored.Status.Plan.Digest
			stored.Status.Execution = &operatorv1alpha1.RecoveryExecutionStatus{Phase: phase, StartedAt: metav1.NewTime(now)}
			if err := kube.Status().Update(t.Context(), stored); err != nil {
				t.Fatal(err)
			}
			// Hold the real operation Lease as the installation reconciler would.
			// No rollback step may run until this independent holder releases it.
			if err := lease.New(kube).With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
				result, err := reconciler.Reconcile(t.Context(), key)
				if err != nil {
					return err
				}
				if err := kube.Get(ctx, key.NamespacedName, stored); err != nil {
					return err
				}
				if result.RequeueAfter <= 0 || stored.Status.Phase != operatorv1alpha1.RecoveryPhaseExecuting || stored.Status.Execution.Phase != phase || len(steps.phases) != 0 {
					t.Fatalf("lease contention became terminal or advanced recovery: result=%+v status=%+v steps=%v", result, stored.Status, steps.phases)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for range 5 {
				if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			}
			if err := kube.Get(t.Context(), key.NamespacedName, stored); err != nil {
				t.Fatal(err)
			}
			if stored.Status.Phase != operatorv1alpha1.RecoveryPhaseSucceeded || stored.Status.Plan.Digest != digest || len(steps.phases) == 0 || steps.phases[0] != phase {
				t.Fatalf("recovery did not resume its pinned phase after Lease release: status=%+v steps=%v", stored.Status, steps.phases)
			}
		})
	}
}

func TestInstallationYieldsToExecutingRecovery(t *testing.T) {
	now := time.Unix(10000, 0).UTC()
	installation := scheduledBackupInstallation(now)
	installation.Spec.Upgrade.Backup.Schedule = ""
	request := recoveryRequest(now)
	request.Namespace = installation.Namespace
	request.Spec.InstallationRef = installation.Name
	request.Status = operatorv1alpha1.KubeOVNRecoveryRequestStatus{
		Phase: operatorv1alpha1.RecoveryPhaseExecuting,
		Plan:  &operatorv1alpha1.RecoveryPlanStatus{InstallationUID: installation.UID},
	}
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Clock: func() time.Time { return now }, Leases: lease.New(kube), Lifecycle: lifecycleFake, Renderer: objectRenderer{}}
	key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(installation)}
	result, err := reconciler.Reconcile(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if lifecycleFake.resume != 0 || result.RequeueAfter <= 0 {
		t.Fatalf("installation competed with active recovery: resumes=%d result=%+v", lifecycleFake.resume, result)
	}
	stored := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), key.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	version := stored.ResourceVersion
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), key.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	if stored.ResourceVersion != version {
		t.Fatal("unchanged recovery wait rewrote installation status and generated another event")
	}
	called := false
	err = reconciler.withInstallationLease(t.Context(), installation, func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, errRecoveryExecuting) || called {
		t.Fatalf("Lease admission did not recheck active recovery: err=%v called=%t", err, called)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), request); err != nil {
		t.Fatal(err)
	}
	request.Status.Phase = operatorv1alpha1.RecoveryPhaseSucceeded
	if err := kube.Status().Update(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if lifecycleFake.resume != 1 {
		t.Fatal("terminal recovery did not release normal installation reconciliation")
	}
}

type recoveryExecutorFunc func(context.Context, RecoveryPlan) error

func (f recoveryExecutorFunc) Execute(ctx context.Context, plan RecoveryPlan) error {
	return f(ctx, plan)
}

func recoveryInstallation() *operatorv1alpha1.KubeOVNInstallation {
	return &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("installation-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{
			Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true,
			MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member-recovery", NodeName: "node-recovery", ClaimName: "claim-recovery", VolumeName: "volume-recovery", Image: "example/central@sha256:" + strings.Repeat("a", 64), NorthboundAddress: "tcp:192.0.2.10:6643", SouthboundAddress: "tcp:192.0.2.10:6644", NorthboundClientAddress: "tcp:192.0.2.10:6641", SouthboundClientAddress: "tcp:192.0.2.10:6642"}},
		}},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{Topology: operatorv1alpha1.TopologyStatus{Inventory: &operatorv1alpha1.CentralTopologyReference{Name: "retained", UID: types.UID("retained-uid"), Digest: "sha256:topology"}}},
	}
}

func recoveryRequest(now time.Time) *operatorv1alpha1.KubeOVNRecoveryRequest {
	return &operatorv1alpha1.KubeOVNRecoveryRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "recover", Namespace: "default", UID: types.UID("request-uid"), Generation: 1},
		Spec:       operatorv1alpha1.KubeOVNRecoveryRequestSpec{InstallationRef: "demo", Reason: "replace failed member", Mode: operatorv1alpha1.RecoveryModeRejoin, TargetMemberRef: "member-recovery", ApprovedBy: "human-review", ExpiresAt: metav1.NewTime(now.Add(time.Hour))},
	}
}

func testBackupPairRefs() map[string]string {
	return map[string]string{"OVN_Northbound": "sha256:nb", "OVN_Southbound": "sha256:sb"}
}

func testBackupPairID() string {
	data, _ := stdjson.Marshal(testBackupPairRefs(), stdjson.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for index := range conditions {
		if conditions[index].Type == conditionType {
			return &conditions[index]
		}
	}
	return nil
}

var _ RecoveryExecutor = recoveryExecutorFunc(nil)

func TestRecoveryPlanSurvivesTimestampWirePrecision(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	request := recoveryRequest(now)
	request.Spec.ExpiresAt = metav1.NewTime(now.Add(time.Hour + 123456*time.Microsecond))
	plan, reason, err := buildRecoveryPlan(request, recoveryInstallation(), now)
	if err != nil {
		t.Fatalf("build recovery plan: %s: %v", reason, err)
	}
	// The API's metav1.Time status field serializes at second precision even
	// when the incoming request used a fractional RFC3339 expiration.
	wire, err := stdjson.Marshal(recoveryPlanStatus(plan))
	if err != nil {
		t.Fatal(err)
	}
	var stored operatorv1alpha1.RecoveryPlanStatus
	if err := stdjson.Unmarshal(wire, &stored); err != nil {
		t.Fatal(err)
	}
	if !sameRecoveryPlan(&stored, recoveryPlanStatus(plan)) {
		t.Fatalf("unchanged request invalidated its serialized plan: wire=%s expiry=%s", wire, plan.ExpiresAt.Format(time.RFC3339Nano))
	}
	changed := plan
	changed.ExpiresAt = changed.ExpiresAt.Add(time.Nanosecond)
	changed.Digest = recoveryPlanDigest(changed)
	if sameRecoveryPlan(&stored, recoveryPlanStatus(changed)) {
		t.Fatal("normalizing the status timestamp hid a changed expiration from the immutable digest")
	}
}
