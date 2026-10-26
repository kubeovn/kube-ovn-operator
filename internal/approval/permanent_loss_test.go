package approval

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const operationID = "eeee0000-1111-2222-3333-444444444444"

func testSnapshot(database string) ovnadmin.Snapshot {
	return ovnadmin.Snapshot{
		Database: database, ClusterID: "dddd0000-1111-2222-3333-444444444444",
		LeaderServerID: "aaaa0000-1111-2222-3333-444444444444", Term: 7, CommitIndex: 20, AppliedIndex: 20,
		Members: []membership.Member{
			{ServerID: "aaaa0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.1:6643", Healthy: true, Leader: true},
			{ServerID: "bbbb0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.2:6643", Healthy: true},
			{ServerID: "cccc0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.3:6643"},
		},
	}
}

func testRecord() *api.KubeOVNMemberRemovalApproval {
	s := testSnapshot("OVN_Northbound")
	return &api.KubeOVNMemberRemovalApproval{
		Name: "lost-node", Namespace: "test", UID: "approval-uid", Generation: 1,
		CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		Spec: api.MemberRemovalApprovalSpec{Decision: api.MemberRemovalDecision{
			InstallationRef: api.InstallationReference{Name: "ovn", UID: "installation-uid"},
			OperationID:     operationID, Reason: "Hardware permanently lost", ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour).Truncate(time.Second)),
			Targets: []api.MemberRemovalTarget{
				{Database: s.Database, ClusterID: s.ClusterID, ServerID: s.Members[2].ServerID, Address: s.Members[2].Address},
				{Database: "OVN_Southbound", ClusterID: s.ClusterID, ServerID: s.Members[2].ServerID, Address: s.Members[2].Address},
			},
		}},
	}
}

type fixture struct {
	kube         client.Client
	provider     *Provider
	installation *api.KubeOVNInstallation
	ref          membership.ApprovalReference
	change       membership.Change
}

func setup(t *testing.T, funcs interceptor.Funcs) fixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	record := testRecord()
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).WithInterceptorFuncs(funcs).Build()
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid"}
	provider, err := New(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	ref := membership.ApprovalReference{Name: record.Name, UID: string(record.UID)}
	s := testSnapshot("OVN_Northbound")
	return fixture{kube: kube, provider: provider, installation: installation, ref: ref, change: membership.Change{
		Operation: membership.OperationKick, OperationID: operationID, ApprovalRef: ref, ClusterID: s.ClusterID,
		Member: s.Members[2], PermanentLossApproved: true, Reason: record.Spec.Decision.Reason,
	}}
}

func (f fixture) guarded(t *testing.T, fn func(context.Context) error) error {
	t.Helper()
	manager := lease.New(f.kube)
	return manager.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, f.installation, lease.Central, fn)
	})
}

func (f fixture) record(t *testing.T) *api.KubeOVNMemberRemovalApproval {
	t.Helper()
	record := new(api.KubeOVNMemberRemovalApproval)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: f.ref.Name}, record); err != nil {
		t.Fatal(err)
	}
	return record
}

func observations() []ovnadmin.Snapshot {
	result := []ovnadmin.Snapshot{testSnapshot("OVN_Northbound"), testSnapshot("OVN_Southbound")}
	for i := range result {
		result[i].Members = result[i].Members[:2]
	}
	return result
}

func TestApprovalSurvivesRestartAndIsConsumedOnce(t *testing.T) {
	f := setup(t, interceptor.Funcs{})
	err := f.guarded(t, func(ctx context.Context) error {
		if _, err := f.provider.VerifyPermanentLoss(ctx, testSnapshot("OVN_Northbound"), f.change); err == nil {
			t.Fatal("unreserved approval authorized removal")
		}
		if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
			return err
		}
		reservedRecord := f.record(t)
		// A new provider has no in-memory reservation state.
		restarted, err := New(f.kube, f.installation)
		if err != nil {
			return err
		}
		if err := restarted.Reserve(ctx, f.ref, operationID); err != nil {
			return err
		}
		if got := f.record(t); got.ResourceVersion != reservedRecord.ResourceVersion {
			t.Fatal("retry rewrote reservation")
		}
		for _, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
			expiry, err := restarted.VerifyPermanentLoss(ctx, testSnapshot(database), f.change)
			if err != nil {
				return err
			}
			if !expiry.Equal(reservedRecord.Spec.Decision.ExpiresAt.Time) {
				t.Fatal("expiry does not match authoritative record")
			}
		}
		if err := restarted.Complete(ctx, f.ref, operationID, observations()); err != nil {
			return err
		}
		completedRecord := f.record(t)
		if err := restarted.Complete(ctx, f.ref, operationID, nil); err != nil {
			return err
		}
		if got := f.record(t); got.ResourceVersion != completedRecord.ResourceVersion || !reflect.DeepEqual(got.Status, completedRecord.Status) {
			t.Fatal("completion retry rewrote audit")
		}
		if err := restarted.Reserve(ctx, f.ref, operationID); err == nil {
			t.Fatal("completed approval was reserved again")
		}
		if _, err := restarted.VerifyPermanentLoss(ctx, testSnapshot("OVN_Northbound"), f.change); err == nil {
			t.Fatal("completed approval authorized removal")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestApprovalRejectsChangedOrInvalidDecisions(t *testing.T) {
	for _, scenario := range []string{"installation", "operation", "reference UID", "recreated UID", "namespace", "cluster", "server", "address", "database", "duplicate database", "short cluster", "short server", "hostname", "reason", "revoked", "expired", "owner", "deleted", "missing generation", "digest"} {
		t.Run(scenario, func(t *testing.T) {
			f := setup(t, interceptor.Funcs{})
			err := f.guarded(t, func(ctx context.Context) error {
				if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
					return err
				}
				record := f.record(t)
				switch scenario {
				case "installation":
					record.Spec.Decision.InstallationRef.UID = "other"
				case "operation":
					f.change.OperationID = "ffff0000-1111-2222-3333-444444444444"
				case "reference UID":
					f.change.ApprovalRef.UID = "other"
				case "recreated UID":
					record.UID = "new-uid"
				case "namespace":
					f.provider.namespace = "other"
				case "cluster":
					record.Spec.Decision.Targets[0].ClusterID = "ffff0000-1111-2222-3333-444444444444"
				case "server":
					record.Spec.Decision.Targets[0].ServerID = "ffff0000-1111-2222-3333-444444444444"
				case "address":
					record.Spec.Decision.Targets[0].Address = "tcp:192.0.2.9:6643"
				case "database":
					record.Spec.Decision.Targets[0].Database = "other"
				case "duplicate database":
					record.Spec.Decision.Targets[1].Database = "OVN_Northbound"
				case "short cluster":
					record.Spec.Decision.Targets[0].ClusterID = "abcd"
				case "short server":
					record.Spec.Decision.Targets[0].ServerID = "abcd"
				case "hostname":
					record.Spec.Decision.Targets[0].Address = "tcp:node:6643"
				case "reason":
					record.Spec.Decision.Reason = " "
				case "revoked":
					record.Spec.Revoked = true
				case "expired":
					record.Spec.Decision.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Second))
				case "owner":
					record.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "other", UID: "owner"}}
				case "deleted":
					record.Finalizers = []string{"test.example/hold"}
				case "missing generation":
					record.Generation = 0
				case "digest":
					record.Spec.Decision.Reason += " changed"
				}
				if err := f.kube.Update(ctx, record); err != nil {
					return err
				}
				if scenario == "deleted" {
					if err := f.kube.Delete(ctx, record); err != nil {
						return err
					}
				}
				if _, err := f.provider.VerifyPermanentLoss(ctx, testSnapshot("OVN_Northbound"), f.change); err == nil {
					t.Fatal("invalid approval authorized removal")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApprovalRequiresCurrentUnhealthyTargetAndQuorum(t *testing.T) {
	for _, scenario := range []string{"healthy", "no quorum", "target absent", "target address", "cluster", "database", "leave", "unconfirmed", "unapplied", "leader"} {
		t.Run(scenario, func(t *testing.T) {
			f := setup(t, interceptor.Funcs{})
			err := f.guarded(t, func(ctx context.Context) error {
				if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
					return err
				}
				s := testSnapshot("OVN_Northbound")
				switch scenario {
				case "healthy":
					s.Members[2].Healthy = true
				case "no quorum":
					s.Members[1].Healthy = false
				case "target absent":
					s.Members = s.Members[:2]
				case "target address":
					s.Members[2].Address = "tcp:192.0.2.9:6643"
				case "cluster":
					s.ClusterID = "ffff0000-1111-2222-3333-444444444444"
				case "database":
					s.Database = "other"
				case "leave":
					f.change.Operation = membership.OperationLeave
				case "unconfirmed":
					f.change.PermanentLossApproved = false
				case "unapplied":
					s.AppliedIndex--
				case "leader":
					s.Members[2].Leader = true
				}
				if _, err := f.provider.VerifyPermanentLoss(ctx, s, f.change); err == nil {
					t.Fatal("unsafe observation authorized removal")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func recoveryApprovalRecord() (*api.KubeOVNRecoveryApproval, RecoveryBinding, api.RecoveryApprovalReference) {
	now := time.Now().Add(-time.Minute).Truncate(time.Second)
	record := &api.KubeOVNRecoveryApproval{
		ObjectMeta: metav1.ObjectMeta{Name: "recovery-approval", Namespace: "test", UID: "recovery-approval-uid", Generation: 1, CreationTimestamp: metav1.NewTime(now)},
		Spec: api.RecoveryApprovalSpec{
			InstallationRef: api.InstallationReference{Name: "ovn", UID: "installation-uid"}, InstallationGeneration: 2,
			RequestRef: &api.RecoveryRequestReference{Name: "recover", UID: "request-uid", Generation: 3}, Mode: "reconvert",
			SourceVersion: "v1.17.0", TargetVersion: "v1.16.0", BackupRef: "sha256:" + strings.Repeat("a", 64),
			TopologyDigest: "sha256:" + strings.Repeat("b", 64), ChartDigest: "sha256:" + strings.Repeat("c", 64),
			Reason: "lost quorum requires restore", ExpiresAt: metav1.NewTime(now.Add(time.Hour)),
		},
	}
	binding := RecoveryBinding{InstallationName: "ovn", InstallationUID: "installation-uid", InstallationGeneration: 2, RequestName: "recover", RequestUID: "request-uid", RequestGeneration: 3, Mode: "reconvert", SourceVersion: "v1.17.0", TargetVersion: "v1.16.0", BackupRef: record.Spec.BackupRef, TopologyDigest: record.Spec.TopologyDigest, ChartDigest: record.Spec.ChartDigest}
	ref := api.RecoveryApprovalReference{Name: record.Name, UID: record.UID}
	return record, binding, ref
}

func TestRecoveryApprovalSurvivesRestartAndIsConsumedOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	record, binding, ref := recoveryApprovalRecord()
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid"}
	provider, err := NewRecovery(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	manager := lease.New(kube)
	if err := manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			if err := provider.Reserve(ctx, ref, binding); err != nil {
				return err
			}
			reserved := new(api.KubeOVNRecoveryApproval)
			if err := kube.Get(ctx, client.ObjectKeyFromObject(record), reserved); err != nil {
				return err
			}
			restarted, err := NewRecovery(kube, installation)
			if err != nil {
				return err
			}
			if err := restarted.Reserve(ctx, ref, binding); err != nil {
				return err
			}
			if err := restarted.Complete(ctx, ref, binding); err != nil {
				return err
			}
			completed := new(api.KubeOVNRecoveryApproval)
			if err := kube.Get(ctx, client.ObjectKeyFromObject(record), completed); err != nil {
				return err
			}
			if err := restarted.Complete(ctx, ref, binding); err != nil {
				return err
			}
			current := new(api.KubeOVNRecoveryApproval)
			if err := kube.Get(ctx, client.ObjectKeyFromObject(record), current); err != nil {
				return err
			}
			if current.ResourceVersion != completed.ResourceVersion || !reflect.DeepEqual(current.Status, completed.Status) {
				t.Fatal("recovery approval completion retry rewrote audit")
			}
			if err := restarted.Reserve(ctx, ref, binding); err == nil {
				t.Fatal("completed recovery approval was reserved again")
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryApprovalRejectsBindingDrift(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	record, binding, ref := recoveryApprovalRecord()
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid"}
	provider, err := NewRecovery(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	binding.TargetVersion = "v1.15.0"
	manager := lease.New(kube)
	err = manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			return provider.Reserve(ctx, ref, binding)
		})
	})
	if err == nil || !strings.Contains(err.Error(), "does not bind") {
		t.Fatalf("binding drift was accepted: %v", err)
	}
}

func TestRecoveryApprovalRejectsUnsupportedMode(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	record, binding, ref := recoveryApprovalRecord()
	binding.Mode = "eraseEverything"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(record).WithObjects(record).Build()
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid"}
	provider, err := NewRecovery(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	manager := lease.New(kube)
	err = manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			return provider.Reserve(ctx, ref, binding)
		})
	})
	if err == nil || !strings.Contains(err.Error(), "mode") || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported recovery mode was accepted: %v", err)
	}
}

func TestCompletionRequiresEveryDatabaseToConverge(t *testing.T) {
	for _, scenario := range []string{"missing SB", "duplicate", "wrong cluster", "old member", "no quorum", "wrong database", "unreserved"} {
		t.Run(scenario, func(t *testing.T) {
			f := setup(t, interceptor.Funcs{})
			err := f.guarded(t, func(ctx context.Context) error {
				if scenario != "unreserved" {
					if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
						return err
					}
				}
				observed := observations()
				switch scenario {
				case "missing SB":
					observed = observed[:1]
				case "duplicate":
					observed[1] = observed[0]
				case "wrong cluster":
					observed[1].ClusterID = "ffff0000-1111-2222-3333-444444444444"
				case "old member":
					observed[1] = testSnapshot("OVN_Southbound")
				case "no quorum":
					observed[1].Members[1].Healthy = false
				case "wrong database":
					observed[1].Database = "other"
				}
				if err := f.provider.Complete(ctx, f.ref, operationID, observed); err == nil {
					t.Fatal("incomplete operation consumed approval")
				}
				if f.record(t).Status.Phase == Completed {
					t.Fatal("failed completion changed audit")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRevocationAndExpiryStillAllowCompletionAudit(t *testing.T) {
	for _, revoked := range []bool{true, false} {
		t.Run(map[bool]string{true: "revoked", false: "expired"}[revoked], func(t *testing.T) {
			f := setup(t, interceptor.Funcs{})
			err := f.guarded(t, func(ctx context.Context) error {
				if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
					return err
				}
				record := f.record(t)
				if revoked {
					record.Spec.Revoked = true
				} else {
					// Seed a reservation made before expiry without changing its decision digest.
					record.Spec.Decision.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Second))
				}
				if err := f.kube.Update(ctx, record); err != nil {
					return err
				}
				if !revoked {
					current, digest, err := f.provider.read(ctx, f.ref, operationID, false)
					if err != nil {
						return err
					}
					current.Status.DecisionDigest = digest
					current.Status.ReservedAt = new(metav1.NewTime(time.Now().Add(-30 * time.Second)))
					if err := f.kube.Status().Update(ctx, current); err != nil {
						return err
					}
				}
				if _, err := f.provider.VerifyPermanentLoss(ctx, testSnapshot("OVN_Northbound"), f.change); err == nil {
					t.Fatal("inactive decision authorized removal")
				}
				return f.provider.Complete(ctx, f.ref, operationID, observations())
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApprovalNeverOverwritesResourceVersionConflicts(t *testing.T) {
	for _, completing := range []bool{false, true} {
		t.Run(map[bool]string{false: "reserve", true: "complete"}[completing], func(t *testing.T) {
			conflict := !completing
			f := setup(t, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if conflict {
					return apierrors.NewConflict(schema.GroupResource{Group: api.GroupVersion.Group, Resource: "kubeovnmemberremovalapprovals"}, obj.GetName(), errors.New("concurrent update"))
				}
				return c.SubResource(subresource).Update(ctx, obj, opts...)
			}})
			err := f.guarded(t, func(ctx context.Context) error {
				if completing {
					if err := f.provider.Reserve(ctx, f.ref, operationID); err != nil {
						return err
					}
					conflict = true
				}
				before := f.record(t)
				var err error
				if completing {
					err = f.provider.Complete(ctx, f.ref, operationID, observations())
				} else {
					err = f.provider.Reserve(ctx, f.ref, operationID)
				}
				if !apierrors.IsConflict(err) {
					t.Fatalf("expected conflict, got %v", err)
				}
				if !reflect.DeepEqual(f.record(t).Status, before.Status) {
					t.Fatal("conflict overwrote status")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApprovalRequiresInstallationBoundLeases(t *testing.T) {
	f := setup(t, interceptor.Funcs{})
	if err := f.provider.Reserve(t.Context(), f.ref, operationID); err == nil {
		t.Fatal("reserved without Leases")
	}
	f.installation.UID = "other-installation"
	if err := f.guarded(t, func(ctx context.Context) error { return f.provider.Reserve(ctx, f.ref, operationID) }); err == nil {
		t.Fatal("foreign installation Leases accepted")
	}
}
