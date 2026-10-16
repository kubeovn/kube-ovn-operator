package controller

import (
	"context"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
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
		ID: "sha256:verified-pair", Version: installation.Spec.Version, TopologyDigest: "sha256:topology",
		DatabaseRefs: map[string]string{"OVN_Northbound": "sha256:nb", "OVN_Southbound": "sha256:sb"}, CompletedAt: metav1.NewTime(now),
	}
	request := recoveryRequest(now)
	request.Spec.Mode = operatorv1alpha1.RecoveryModeReconvert
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

type recoveryExecutorFunc func(context.Context, RecoveryPlan) error

func (f recoveryExecutorFunc) Execute(ctx context.Context, plan RecoveryPlan) error {
	return f(ctx, plan)
}

func recoveryInstallation() *operatorv1alpha1.KubeOVNInstallation {
	return &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("installation-uid"), Generation: 2},
		Spec:       operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}},
	}
}

func recoveryRequest(now time.Time) *operatorv1alpha1.KubeOVNRecoveryRequest {
	return &operatorv1alpha1.KubeOVNRecoveryRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "recover", Namespace: "default", UID: types.UID("request-uid"), Generation: 1},
		Spec:       operatorv1alpha1.KubeOVNRecoveryRequestSpec{InstallationRef: "demo", Reason: "replace failed member", Mode: operatorv1alpha1.RecoveryModeRejoin, ApprovedBy: "human-review", ExpiresAt: metav1.NewTime(now.Add(time.Hour))},
	}
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
