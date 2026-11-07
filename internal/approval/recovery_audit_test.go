package approval

import (
	"context"
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func withRecoveryAuditProvider(t *testing.T, record *api.KubeOVNRecoveryApproval, fn func(context.Context, client.Client, *RecoveryProvider)) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid"}
	provider, err := NewRecovery(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	manager := lease.New(kube)
	if err := manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			fn(ctx, kube, provider)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryCompletionRejectsMalformedAudit(t *testing.T) {
	for name, mutate := range map[string]func(*api.RecoveryApprovalStatus){
		"missing digest":       func(s *api.RecoveryApprovalStatus) { s.DecisionDigest = "" },
		"wrong digest":         func(s *api.RecoveryApprovalStatus) { s.DecisionDigest = "sha256:" + strings.Repeat("f", 64) },
		"missing reservation":  func(s *api.RecoveryApprovalStatus) { s.ReservedAt = nil },
		"zero reservation":     func(s *api.RecoveryApprovalStatus) { s.ReservedAt = new(metav1.Time) },
		"reservation predates": func(s *api.RecoveryApprovalStatus) { s.ReservedAt = new(metav1.NewTime(time.Unix(1, 0))) },
	} {
		t.Run(name, func(t *testing.T) {
			record, binding, ref := recoveryApprovalRecord()
			digest, err := recoveryDecisionDigest(record.Spec)
			if err != nil {
				t.Fatal(err)
			}
			now := metav1.Now()
			record.Status = api.RecoveryApprovalStatus{Phase: api.RecoveryApprovalCompleted, DecisionDigest: digest, ReservedAt: &now, CompletedAt: &now}
			mutate(&record.Status)
			withRecoveryAuditProvider(t, record, func(ctx context.Context, _ client.Client, provider *RecoveryProvider) {
				if err := provider.Complete(ctx, ref, binding); err == nil {
					t.Fatal("malformed completion audit was accepted")
				}
			})
		})
	}
}

func TestRecoveryCompletionRecordsConvergenceAfterRevocation(t *testing.T) {
	record, binding, ref := recoveryApprovalRecord()
	withRecoveryAuditProvider(t, record, func(ctx context.Context, kube client.Client, provider *RecoveryProvider) {
		if err := provider.Reserve(ctx, ref, binding); err != nil {
			t.Fatal(err)
		}
		current := new(api.KubeOVNRecoveryApproval)
		if err := kube.Get(ctx, client.ObjectKeyFromObject(record), current); err != nil {
			t.Fatal(err)
		}
		digest := current.Status.DecisionDigest
		current.Spec.Revoked = true
		if err := kube.Update(ctx, current); err != nil {
			t.Fatal(err)
		}
		if err := provider.Reserve(ctx, ref, binding); err == nil {
			t.Fatal("revoked approval admitted more writes")
		}
		if err := provider.Complete(ctx, ref, binding); err != nil {
			t.Fatalf("revocation prevented recording an already converged restore: %v", err)
		}
		if err := kube.Get(ctx, client.ObjectKeyFromObject(record), current); err != nil {
			t.Fatal(err)
		}
		if current.Status.Phase != api.RecoveryApprovalCompleted || current.Status.DecisionDigest != digest {
			t.Fatalf("completion changed the reserved decision: %+v", current.Status)
		}
	})
}
