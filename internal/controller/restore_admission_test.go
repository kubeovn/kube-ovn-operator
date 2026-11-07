package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type admissionRestoreRecorder struct {
	lifecycleCoordinatorFake
	restores int
}

func (r *admissionRestoreRecorder) RestoreBackupPair(context.Context, *api.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *api.BackupStatus) error {
	r.restores++
	return nil
}

func TestRestoreRechecksAdmissionAfterQuiesce(t *testing.T) {
	for _, mode := range []string{"rollback", "reconvert", "freshBootstrap"} {
		for _, change := range []string{"unchanged", "revoked", "expired", "replaced", "completed", "request changed", "healthy quorum", "quorum lost"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				installation := recoveryInstallation()
				installation.Spec.Chart.Digest = "sha256:" + strings.Repeat("a", 64)
				installation.Status.ObservedVersion = installation.Spec.Version
				installation.Status.LastBackup = &api.BackupStatus{
					ID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), Version: installation.Spec.Version,
					TopologyDigest: installation.Status.Topology.Inventory.Digest, CompletedAt: metav1.Now(),
				}
				plan := RollbackPlan{
					InstallationNamespace: installation.Namespace, InstallationName: installation.Name, InstallationUID: installation.UID,
					InstallationGeneration: installation.Generation, SourceVersion: installation.Status.ObservedVersion, TargetVersion: installation.Spec.Version,
					BackupID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), TopologyDigest: installation.Status.Topology.Inventory.Digest,
					ChartDigest: installation.Spec.Chart.Digest, Mode: mode,
					ApprovalRef: &api.RecoveryApprovalReference{Name: "restore-approval", UID: "approval-uid"},
				}
				if mode != "rollback" {
					plan.RequestName, plan.RequestUID, plan.RequestGeneration = "restore", "request-uid", 2
				}
				record := &api.KubeOVNRecoveryApproval{
					Name: plan.ApprovalRef.Name, Namespace: installation.Namespace, UID: plan.ApprovalRef.UID, Generation: 1,
					CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
					Spec: api.RecoveryApprovalSpec{
						InstallationRef:        api.InstallationReference{Name: installation.Name, UID: installation.UID},
						InstallationGeneration: installation.Generation, Mode: mode, SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion,
						BackupRef: plan.BackupID, TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest,
						Reason: "Disposable test approval", ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour)),
					},
				}
				if mode != "rollback" {
					record.Spec.RequestRef = &api.RecoveryRequestReference{Name: plan.RequestName, UID: plan.RequestUID, Generation: plan.RequestGeneration}
				}
				if change == "expired" {
					record.Spec.ExpiresAt = metav1.NewTime(time.Now().Add(2 * time.Second).Truncate(time.Second))
				}
				kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(record).WithObjects(installation, record).Build()
				manager := lease.New(kube)
				restorer := &admissionRestoreRecorder{reader: kube}
				workload := new(rollbackWorkloadFake)
				quorum := true
				if change == "healthy quorum" || change == "quorum lost" {
					plan.ApprovalRef = nil
				}
				executor := &KubernetesRollbackExecutor{
					Client: kube, Lifecycle: restorer, Topology: &lifecycleTopologyFake{retained: additionTopologyFixture()},
					RendererFactory: func(*api.KubeOVNInstallation) chart.Renderer { return nil },
					WorkloadFactory: func(apply.InstallationIdentity) apply.ApplyClient { return workload },
					ApprovalFactory: func(installation *api.KubeOVNInstallation) (approval.RecoveryApprover, error) {
						return approval.NewRecovery(kube, installation)
					},
					OVN: ovnProviderFunc(func(context.Context, *api.KubeOVNInstallation) (health.OVNReport, error) {
						return health.OVNReport{Ready: quorum, Message: "quorum lost after quiesce"}, nil
					}),
				}
				step := func(phase api.RollbackPhase) error {
					return manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
						return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
							_, err := executor.Step(ctx, plan, phase)
							return err
						})
					})
				}
				if err := step(api.RollbackPhaseQuiescing); err != nil {
					t.Fatal(err)
				}
				if err := kube.Get(t.Context(), client.ObjectKeyFromObject(record), record); err != nil {
					t.Fatal(err)
				}
				if plan.ApprovalRef != nil && record.Status.Phase != api.RecoveryApprovalReserved || workload.quiesceCalls != 1 || restorer.restores != 0 {
					t.Fatalf("quiesce did not reserve approval before restore: approval=%+v workload=%+v restores=%d", record.Status, workload, restorer.restores)
				}
				switch change {
				case "revoked":
					record.Spec.Revoked = true
					if err := kube.Update(t.Context(), record); err != nil {
						t.Fatal(err)
					}
				case "expired":
					time.Sleep(time.Until(record.Spec.ExpiresAt.Add(time.Millisecond)))
				case "replaced":
					if err := kube.Delete(t.Context(), record); err != nil {
						t.Fatal(err)
					}
					record.UID, record.ResourceVersion = "replacement-uid", ""
					if err := kube.Create(t.Context(), record); err != nil {
						t.Fatal(err)
					}
				case "completed":
					record.Status.Phase, record.Status.CompletedAt = api.RecoveryApprovalCompleted, new(metav1.Now())
					if err := kube.Status().Update(t.Context(), record); err != nil {
						t.Fatal(err)
					}
				case "request changed":
					plan.RequestName, plan.RequestUID, plan.RequestGeneration = "other-request", "other-uid", 3
				case "quorum lost":
					quorum = false
				}
				// Resume through a fresh executor, as after a controller restart.
				restarted := *executor
				executor = &restarted
				err := step(api.RollbackPhaseRestoring)
				if change == "unchanged" || change == "healthy quorum" {
					if err != nil || restorer.restores != 1 {
						t.Fatalf("valid reserved approval did not resume: err=%v restores=%d", err, restorer.restores)
					}
				} else if err == nil || restorer.restores != 0 {
					t.Fatalf("%s approval reached database restore: err=%v restores=%d", change, err, restorer.restores)
				} else if plan.ApprovalRef != nil && !strings.Contains(err.Error(), "recovery approval") || plan.ApprovalRef == nil && !strings.Contains(err.Error(), "quorum is unavailable") {
					t.Fatalf("restore failed for an unrelated reason: %v", err)
				}
				if workload.applyCalls != 0 || workload.unquiesceCalls != 0 {
					t.Fatalf("restore admission unexpectedly resumed workloads: %+v", workload)
				}
			})
		}
	}
}
