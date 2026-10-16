package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type scheduledLeaseRunner struct{}

func (scheduledLeaseRunner) With(ctx context.Context, _ *api.KubeOVNInstallation, _ lease.Scope, fn func(context.Context) error) error {
	return fn(ctx)
}

type scheduledTopologyProvider struct {
	topology health.CentralTopology
}

func (p scheduledTopologyProvider) VerifyRetained(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	return nil, nil
}

func (p scheduledTopologyProvider) Ensure(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	return nil, nil
}

func (p scheduledTopologyProvider) Retained(context.Context, *api.KubeOVNInstallation) (health.CentralTopology, error) {
	return p.topology, nil
}

var _ topology.InventoryProvider = scheduledTopologyProvider{}

func TestEnsureScheduledBackupSkipsWhenNotConfiguredOrNotDue(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	installation := scheduledBackupInstallation(now)
	captures := 0
	reconciler := scheduledBackupReconciler(&captures, nil)
	installation.Spec.Upgrade.Backup.Schedule = ""
	if outcome, err := reconciler.ensureScheduledBackup(t.Context(), installation, now); err != nil || outcome.Due {
		t.Fatalf("disabled schedule was handled: outcome=%+v err=%v", outcome, err)
	}
	installation.Spec.Upgrade.Backup.Schedule = "1h"
	installation.Status.LastBackup.CompletedAt = metav1.NewTime(now.Add(-10 * time.Minute))
	if outcome, err := reconciler.ensureScheduledBackup(t.Context(), installation, now); err != nil || outcome.Due || outcome.RequeueAfter != 50*time.Minute {
		t.Fatalf("backup was captured before interval elapsed: outcome=%+v err=%v", outcome, err)
	}
	if captures != 0 {
		t.Fatalf("unexpected captures: %d", captures)
	}
}

func TestEnsureScheduledBackupCapturesAndBindsVerifiedPair(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	installation := scheduledBackupInstallation(now)
	captures := 0
	reconciler := scheduledBackupReconciler(&captures, func(_ context.Context, _ *api.KubeOVNInstallation, _ health.CentralTopology, binding lifecycle.BackupBinding) (*api.BackupStatus, error) {
		refs := map[string]string{"OVN_Northbound": "nb", "OVN_Southbound": "sb"}
		data, _ := stdjson.Marshal(refs, stdjson.Deterministic(true))
		return &api.BackupStatus{ID: fmt.Sprintf("sha256:%x", sha256.Sum256(data)), DatabaseRefs: refs, Destination: binding.Destination, Version: binding.Version, TopologyDigest: binding.TopologyDigest, CompletedAt: metav1.NewTime(now)}, nil
	})
	outcome, err := reconciler.ensureScheduledBackup(t.Context(), installation, now)
	if err != nil || !outcome.Due || outcome.RequeueAfter != time.Hour {
		t.Fatalf("scheduled capture failed: outcome=%+v err=%v", outcome, err)
	}
	if captures != 1 || installation.Status.LastBackup == nil || installation.Status.LastBackup.ID == "" {
		t.Fatalf("scheduled pair was not persisted in memory: captures=%d backup=%+v", captures, installation.Status.LastBackup)
	}
}

func TestEnsureScheduledBackupFailurePreservesVerifiedPair(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	installation := scheduledBackupInstallation(now)
	previous := new(api.BackupStatus)
	installation.Status.LastBackup.DeepCopyInto(previous)
	reconciler := scheduledBackupReconciler(new(int), func(context.Context, *api.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*api.BackupStatus, error) {
		return nil, errors.New("object store unavailable")
	})
	outcome, err := reconciler.ensureScheduledBackup(t.Context(), installation, now)
	if err == nil || !outcome.Due {
		t.Fatalf("failed scheduled capture was not reported: outcome=%+v err=%v", outcome, err)
	}
	if installation.Status.LastBackup == nil || installation.Status.LastBackup.ID != previous.ID {
		t.Fatalf("failed capture replaced the verified pair: %+v", installation.Status.LastBackup)
	}
}

func TestEnsureScheduledBackupSkipsUnsafeInstallationStates(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	for _, test := range []struct {
		name   string
		mutate func(*api.KubeOVNInstallation)
	}{
		{name: "no topology", mutate: func(i *api.KubeOVNInstallation) { i.Status.Topology.Inventory = nil }},
		{name: "upgrade", mutate: func(i *api.KubeOVNInstallation) { i.Spec.Version = "v1.18.0" }},
		{name: "active lifecycle", mutate: func(i *api.KubeOVNInstallation) {
			i.Status.Topology.Operation = &api.CentralOperationReference{HandoffApplied: false}
		}},
		{name: "hcp", mutate: func(i *api.KubeOVNInstallation) { i.Spec.HCP.Enabled = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			installation := scheduledBackupInstallation(now)
			test.mutate(installation)
			captures := 0
			reconciler := scheduledBackupReconciler(&captures, func(context.Context, *api.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*api.BackupStatus, error) {
				return nil, errors.New("must not capture")
			})
			if outcome, err := reconciler.ensureScheduledBackup(t.Context(), installation, now); err != nil || outcome.Due {
				t.Fatalf("unsafe state was scheduled: outcome=%+v err=%v", outcome, err)
			}
			if captures != 0 {
				t.Fatalf("unsafe state invoked capture: %d", captures)
			}
		})
	}
}

func TestReconcileScheduledBackupReturnsBeforeChartApply(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	installation := scheduledBackupInstallation(now)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(installation).Build()
	captures := 0
	reconciler := scheduledBackupReconciler(&captures, func(_ context.Context, _ *api.KubeOVNInstallation, _ health.CentralTopology, binding lifecycle.BackupBinding) (*api.BackupStatus, error) {
		refs := map[string]string{"OVN_Northbound": "nb", "OVN_Southbound": "sb"}
		data, _ := stdjson.Marshal(refs, stdjson.Deterministic(true))
		return &api.BackupStatus{ID: fmt.Sprintf("sha256:%x", sha256.Sum256(data)), DatabaseRefs: refs, Destination: binding.Destination, Version: binding.Version, TopologyDigest: binding.TopologyDigest, CompletedAt: metav1.NewTime(now)}, nil
	})
	reconciler.Client, reconciler.Scheme, reconciler.Clock = kube, controllerScheme(t), func() time.Time { return now }
	workload := &observedWorkload{}
	reconciler.Renderer, reconciler.Workload = objectRenderer{}, workload
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != time.Hour || captures != 1 || workload.applyCalls != 0 {
		t.Fatalf("scheduled backup did not fence Chart apply: result=%+v captures=%d applyCalls=%d", result, captures, workload.applyCalls)
	}
	got := new(api.KubeOVNInstallation)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastBackup == nil || !got.Status.LastBackup.CompletedAt.Time.Equal(now) || got.Status.Upgrade.Step != "BackupSchedule" {
		t.Fatalf("scheduled checkpoint was not persisted: backup=%+v step=%q wantTime=%s", got.Status.LastBackup, got.Status.Upgrade.Step, now)
	}
	if err := kube.Get(t.Context(), client.ObjectKey{Namespace: installation.Namespace, Name: planConfigMapPrefix + installation.Name}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("scheduled backup unexpectedly wrote a Chart plan")
	}
}

func scheduledBackupReconciler(captures *int, capture func(context.Context, *api.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*api.BackupStatus, error)) *KubeOVNInstallationReconciler {
	return &KubeOVNInstallationReconciler{
		Leases: scheduledLeaseRunner{}, Topology: scheduledTopologyProvider{topology: validScheduledTopology()},
		ScheduledBackup: func(ctx context.Context, installation *api.KubeOVNInstallation, retained health.CentralTopology, binding lifecycle.BackupBinding) (*api.BackupStatus, error) {
			*captures++
			return capture(ctx, installation, retained, binding)
		},
	}
}

func scheduledBackupInstallation(now time.Time) *api.KubeOVNInstallation {
	return &api.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "installation", Generation: 1},
		Spec:       api.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: api.InstallationModeFull, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3}, Upgrade: api.UpgradeSpec{Backup: api.BackupSpec{Required: true, Destination: "s3://example/kube-ovn", Schedule: "1h"}}},
		Status:     api.KubeOVNInstallationStatus{ApplyCheckpoint: &api.ApplyCheckpointStatus{Version: "v1.17.0"}, ObservedVersion: "v1.17.0", Topology: api.TopologyStatus{Inventory: &api.CentralTopologyReference{Name: "inventory", UID: "inventory-uid", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, LastBackup: &api.BackupStatus{ID: "previous", DatabaseRefs: map[string]string{"OVN_Northbound": "nb", "OVN_Southbound": "sb"}, Destination: "s3://example/kube-ovn", Version: "v1.17.0", TopologyDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CompletedAt: metav1.NewTime(now.Add(-2 * time.Hour))}},
	}
}

func validScheduledTopology() health.CentralTopology {
	return health.CentralTopology{Members: []health.CentralIdentity{{
		Node:     health.ObjectIdentity{Name: "node", UID: "node-uid"},
		Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "central", UID: "central-uid"}, Kind: "StatefulSet"},
		Storage:  []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: "/var/lib/ovn-member"}},
		Pod:      health.ObjectIdentity{Name: "central-0", UID: "pod-uid"}, ContainerID: "containerd://central",
		Databases: [2]health.DatabaseIdentity{{Database: "OVN_Northbound", ClusterID: "11111111-2222-3333-4444-555555555555", ServerID: "11111111-2222-3333-4444-555555555556", Address: "tcp:10.0.0.1:6643", ClientAddress: "tcp:10.0.0.1:6641"}, {Database: "OVN_Southbound", ClusterID: "11111111-2222-3333-4444-555555555555", ServerID: "11111111-2222-3333-4444-555555555557", Address: "tcp:10.0.0.1:6644", ClientAddress: "tcp:10.0.0.1:6642"}},
	}}}
}
