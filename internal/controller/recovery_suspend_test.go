package controller

import (
	"context"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestSuspendedRecoveryRequestWaitsBeforeBindingAndAdmission(t *testing.T) {
	for _, mode := range []api.RecoveryMode{api.RecoveryModeRejoin, api.RecoveryModeReconvert, api.RecoveryModeFreshBootstrap} {
		t.Run(string(mode), func(t *testing.T) {
			now := time.Unix(100, 0).UTC()
			installation, request := recoveryInstallation(), recoveryRequest(now)
			request.Spec.Suspend, request.Spec.Mode = true, mode
			if mode != api.RecoveryModeRejoin {
				installation.Status.ObservedVersion = installation.Spec.Version
				installation.Status.LastBackup = &api.BackupStatus{ID: testBackupPairID(), Version: installation.Spec.Version, DatabaseRefs: testBackupPairRefs(), TopologyDigest: installation.Status.Topology.Inventory.Digest, CompletedAt: metav1.NewTime(now)}
				request.Spec.TargetMemberName, request.Spec.BackupRef = "", testBackupPairID()
			}
			kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation, request).WithObjects(installation, request).Build()
			calls := 0
			var admitted RecoveryPlan
			r := &RecoveryRequestReconciler{Client: kube, Clock: func() time.Time { return now }, Executor: recoveryExecutorFunc(func(_ context.Context, plan RecoveryPlan) error {
				calls++
				admitted = plan
				return nil
			})}
			key := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}
			var version string
			for attempt := range 2 {
				result, err := r.Reconcile(t.Context(), key)
				if err != nil {
					t.Fatal(err)
				}
				if err := kube.Get(t.Context(), key.NamespacedName, request); err != nil {
					t.Fatal(err)
				}
				condition := findCondition(request.Status.Conditions, "Accepted")
				if calls != 0 || request.Status.Plan != nil || request.Status.Execution != nil || request.Status.Phase != api.RecoveryPhasePending || condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "Suspended" || result.RequeueAfter <= 0 {
					t.Fatalf("suspended request reached admission: result=%+v status=%+v calls=%d", result, request.Status, calls)
				}
				if attempt > 0 && version != request.ResourceVersion {
					t.Fatal("unchanged suspended request rewrote status")
				}
				version = request.ResourceVersion
			}
			request.Spec.Suspend = false
			request.Generation++ // The fake API does not advance generation for spec updates.
			if mode != api.RecoveryModeRejoin {
				request.Spec.ApprovalRef = &api.RecoveryApprovalReference{Name: "independent-approval", UID: "approval-uid"}
			}
			if err := kube.Update(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := r.Reconcile(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			}
			if err := kube.Get(t.Context(), key.NamespacedName, request); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || admitted.RequestUID != request.UID || admitted.RequestGeneration != request.Generation || !sameRecoveryApprovalRef(admitted.ApprovalRef, request.Spec.ApprovalRef) || request.Status.Phase != api.RecoveryPhaseSucceeded {
				t.Fatalf("resume lost its final API binding: plan=%+v status=%+v calls=%d", admitted, request.Status, calls)
			}
		})
	}
}

func TestSuspendedRecoveryRequestExpiresWithoutAdmission(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	request := recoveryRequest(now)
	request.Spec.Suspend = true
	request.Spec.ExpiresAt = metav1.NewTime(now)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(request).WithObjects(request).Build()
	r := &RecoveryRequestReconciler{Client: kube, Clock: func() time.Time { return now }}
	if _, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(request)}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(request), request); err != nil {
		t.Fatal(err)
	}
	if request.Status.Phase != api.RecoveryPhaseExpired || request.Status.Plan != nil {
		t.Fatalf("suspension bypassed expiry: %+v", request.Status)
	}
}

func TestRecoveryPlanRejectsSuspendedRequest(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	request := recoveryRequest(now)
	request.Spec.Suspend = true
	if _, _, err := buildRecoveryPlan(request, recoveryInstallation(), now); err == nil {
		t.Fatal("a suspended request produced an executable plan")
	}
}
