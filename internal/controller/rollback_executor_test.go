package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type rollbackRendererFunc func(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error)

func (f rollbackRendererFunc) Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return f(ctx, spec)
}

func TestRollbackBackupStatusUsesOriginalCaptureTime(t *testing.T) {
	captured := metav1.NewTime(time.Unix(100, 0).UTC())
	plan := RollbackPlan{BackupID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), TargetVersion: "v1.16.0", TopologyDigest: "sha256:topology"}
	installation := &operatorv1alpha1.KubeOVNInstallation{Status: operatorv1alpha1.KubeOVNInstallationStatus{LastBackup: &operatorv1alpha1.BackupStatus{
		ID: plan.BackupID, DatabaseRefs: testBackupPairRefs(), Version: plan.TargetVersion, TopologyDigest: plan.TopologyDigest, CompletedAt: captured,
	}}}
	backup, err := rollbackBackupStatus(installation, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !backup.CompletedAt.Time.Equal(captured.Time) {
		t.Fatalf("rollback replaced capture time: got %v want %v", backup.CompletedAt, captured)
	}
	backup.DatabaseRefs["OVN_Northbound"] = "changed"
	if installation.Status.LastBackup.DatabaseRefs["OVN_Northbound"] != "sha256:nb" {
		t.Fatal("rollback backup status was not copied before use")
	}
}

func TestRollbackBackupStatusFallsBackToUpgradeCheckpoint(t *testing.T) {
	captured := metav1.NewTime(time.Unix(200, 0).UTC())
	plan := RollbackPlan{BackupID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), TargetVersion: "v1.16.0", TopologyDigest: "sha256:topology", Destination: "s3://backups"}
	installation := &operatorv1alpha1.KubeOVNInstallation{Status: operatorv1alpha1.KubeOVNInstallationStatus{Upgrade: operatorv1alpha1.UpgradeStatus{Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
		ID: plan.BackupID, DatabaseRefs: testBackupPairRefs(), SourceVersion: plan.TargetVersion, Destination: "s3://backups", TopologyDigest: plan.TopologyDigest, CapturedAt: captured, Phase: operatorv1alpha1.UpgradeCheckpointVerified,
	}}}}
	backup, err := rollbackBackupStatus(installation, plan)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Destination != "s3://backups" || !backup.CompletedAt.Time.Equal(captured.Time) {
		t.Fatalf("checkpoint binding was not preserved: %+v", backup)
	}
}

func TestRollbackBackupStatusRejectsFailedUpgradeCheckpoint(t *testing.T) {
	plan := RollbackPlan{BackupID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), TargetVersion: "v1.16.0", TopologyDigest: "sha256:topology"}
	installation := &operatorv1alpha1.KubeOVNInstallation{Status: operatorv1alpha1.KubeOVNInstallationStatus{Upgrade: operatorv1alpha1.UpgradeStatus{Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
		ID: plan.BackupID, DatabaseRefs: testBackupPairRefs(), SourceVersion: plan.TargetVersion, TopologyDigest: plan.TopologyDigest, CapturedAt: metav1.NewTime(time.Unix(200, 0)), Phase: operatorv1alpha1.UpgradeCheckpointFailed,
	}}}}
	if _, err := rollbackBackupStatus(installation, plan); err == nil {
		t.Fatal("failed upgrade checkpoint was accepted for rollback")
	}
}

func TestRollbackBackupStatusRejectsChangedPair(t *testing.T) {
	plan := RollbackPlan{BackupID: testBackupPairID(), DatabaseRefs: testBackupPairRefs(), TargetVersion: "v1.16.0", TopologyDigest: "sha256:topology"}
	installation := &operatorv1alpha1.KubeOVNInstallation{Status: operatorv1alpha1.KubeOVNInstallationStatus{LastBackup: &operatorv1alpha1.BackupStatus{
		ID: plan.BackupID, DatabaseRefs: map[string]string{"OVN_Northbound": "changed", "OVN_Southbound": "sha256:sb"}, Version: plan.TargetVersion, TopologyDigest: plan.TopologyDigest, CompletedAt: metav1.NewTime(time.Unix(100, 0)),
	}}}
	if _, err := rollbackBackupStatus(installation, plan); err == nil {
		t.Fatal("changed rollback pair was accepted")
	}
}

func TestRollbackOperationTokenBindsTheAdmittedOperation(t *testing.T) {
	base := RollbackPlan{
		InstallationNamespace:  "default",
		InstallationName:       "ovn",
		InstallationUID:        "installation-uid",
		InstallationGeneration: 7,
		SourceVersion:          "v1.17.0",
		TargetVersion:          "v1.16.0",
		BackupID:               testBackupPairID(),
		DatabaseRefs:           testBackupPairRefs(),
		TopologyDigest:         "sha256:" + strings.Repeat("a", 64),
		ChartDigest:            "sha256:" + strings.Repeat("b", 64),
		Mode:                   "reconvert",
	}
	first, err := rollbackOperationToken(base)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := rollbackOperationToken(base)
	if err != nil {
		t.Fatal(err)
	}
	if first != retry {
		t.Fatalf("same admitted operation changed token: %q != %q", first, retry)
	}
	newRequest := base
	newRequest.RequestName = "recovery-new"
	newRequest.RequestUID = "request-new"
	other, err := rollbackOperationToken(newRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first == other {
		t.Fatal("different recovery requests reused the same workload operation token")
	}
	if !validRollbackDigest(first) {
		t.Fatalf("derived token is not a SHA-256 digest: %q", first)
	}
}

func TestRollbackOperationTokenRejectsUnverifiedExplicitToken(t *testing.T) {
	if _, err := rollbackOperationToken(RollbackPlan{OperationToken: "backup-pair"}); err == nil {
		t.Fatal("unverified explicit rollback operation token was accepted")
	}
}

func TestRenderRollbackTargetPreservesRetainedDeploymentAndProbes(t *testing.T) {
	chartDigest := "sha256:" + strings.Repeat("a", 64)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "rollback", Namespace: "default", UID: "installation"},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Mode:         operatorv1alpha1.InstallationModeFull,
			Version:      "v1.16.0",
			Chart:        operatorv1alpha1.ChartSpec{Digest: chartDigest},
			Central:      operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true},
			NetworkProbe: &operatorv1alpha1.NetworkProbeSpec{Image: "probe@" + chartDigest},
		},
	}
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "ovn-central"},
		"spec":       map[string]any{"replicas": int64(3)},
	}}
	ovs := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "DaemonSet",
		"metadata":   map[string]any{"name": "ovs"},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "ovs"}},
			"template": map[string]any{"spec": map[string]any{"nodeSelector": map[string]any{"kubernetes.io/os": "linux"}}},
		},
	}}
	retained := health.CentralTopology{Members: []health.CentralIdentity{
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "ovn-central"}, Kind: "Deployment"}},
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "ovn-central"}, Kind: "Deployment"}},
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "independent-member"}, Kind: "StatefulSet"}},
	}}
	e := &KubernetesRollbackExecutor{RendererFactory: func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer {
		return rollbackRendererFunc(func(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
			return chart.Rendered{Digest: chartDigest, Objects: []*unstructured.Unstructured{deployment, ovs}}, nil
		})
	}}
	rendered, err := e.renderRollbackTarget(t.Context(), installation, retained, chartDigest)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
	if err != nil || !found || got != 2 {
		t.Fatalf("rollback restored independent member into Chart Deployment: replicas=%d found=%t err=%v", got, found, err)
	}
	if len(rendered.Objects) != 5 {
		t.Fatalf("rollback did not preserve the two probe DaemonSets and Service: objects=%d", len(rendered.Objects))
	}
}

func TestRollbackRestoreFailureDoesNotResumeOrApplyWorkloads(t *testing.T) {
	chartDigest := "sha256:" + strings.Repeat("a", 64)
	installation := recoveryInstallation()
	installation.Spec.Version = "v1.16.0"
	installation.Spec.Chart.Digest = chartDigest
	installation.Status.ObservedVersion = "v1.17.0"
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{
		ID: testBackupPairID(), Version: installation.Spec.Version, TopologyDigest: installation.Status.Topology.Inventory.Digest,
		DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(time.Unix(100, 0).UTC()),
	}
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithObjects(installation).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube, restoreErr: errors.New("restore rejected by database fence")}
	workload := new(rollbackWorkloadFake)
	executor := &KubernetesRollbackExecutor{
		Client:    kube,
		Lifecycle: lifecycleFake,
		Topology:  &lifecycleTopologyFake{retained: additionTopologyFixture()},
		RendererFactory: func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer {
			return rollbackRendererFunc(func(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
				return chart.Rendered{Digest: chartDigest, Objects: []*unstructured.Unstructured{{Object: map[string]any{
					"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central"},
					"spec": map[string]any{"replicas": int64(3)},
				}}}}, nil
			})
		},
		WorkloadFactory: func(apply.InstallationIdentity) apply.ApplyClient { return workload },
		OVN: ovnProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
			return health.OVNReport{Ready: true}, nil
		}),
	}
	plan := RollbackPlan{
		InstallationNamespace: installation.Namespace, InstallationName: installation.Name, InstallationUID: installation.UID, InstallationGeneration: installation.Generation,
		SourceVersion: installation.Status.ObservedVersion, TargetVersion: installation.Spec.Version, BackupID: installation.Status.LastBackup.ID,
		DatabaseRefs: testBackupPairRefs(), Destination: installation.Status.LastBackup.Destination, TopologyDigest: installation.Status.Topology.Inventory.Digest,
		ChartDigest: chartDigest, Mode: "reconvert",
	}
	next, err := executor.Step(t.Context(), plan, operatorv1alpha1.RollbackPhaseQuiescing)
	if err != nil || next != operatorv1alpha1.RollbackPhaseRestoring {
		t.Fatalf("quiescing did not advance to restore: next=%q err=%v", next, err)
	}
	if workload.quiesceCalls != 1 || workload.unquiesceCalls != 0 || workload.applyCalls != 0 {
		t.Fatalf("unexpected workload calls before restore: %+v", workload)
	}
	if _, err := executor.Step(t.Context(), plan, operatorv1alpha1.RollbackPhaseRestoring); err == nil || !strings.Contains(err.Error(), "restore rollback database pair") {
		t.Fatalf("restore failure was not returned: %v", err)
	}
	if workload.unquiesceCalls != 0 || workload.applyCalls != 0 {
		t.Fatalf("restore failure resumed or applied workloads: %+v", workload)
	}
}

type rollbackWorkloadFake struct {
	applyCalls     int
	quiesceCalls   int
	unquiesceCalls int
}

func (f *rollbackWorkloadFake) DryRun(context.Context, []*unstructured.Unstructured) error {
	return nil
}

func (f *rollbackWorkloadFake) Apply(context.Context, []*unstructured.Unstructured) error {
	f.applyCalls++
	return nil
}

func (f *rollbackWorkloadFake) Observe(context.Context, []*unstructured.Unstructured) (apply.Observation, error) {
	return apply.Observation{Ready: true}, nil
}

func (f *rollbackWorkloadFake) Quiesce(context.Context, string) error {
	f.quiesceCalls++
	return nil
}

func (f *rollbackWorkloadFake) Unquiesce(context.Context, string, []*unstructured.Unstructured) error {
	f.unquiesceCalls++
	return nil
}

var _ apply.RollbackClient = (*rollbackWorkloadFake)(nil)
