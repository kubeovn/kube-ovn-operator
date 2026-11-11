package controller

import (
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestRenderOfflineRebuildArtifactsBindsImmutableInputs(t *testing.T) {
	now := time.Date(2026, time.November, 11, 10, 0, 0, 0, time.UTC)
	canonical := builderTestPlan()
	const admissionDigest = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	plan := RecoveryPlan{RequestNamespace: "ovn-system", RequestName: "recover", RequestUID: "request-uid", InstallationNamespace: "ovn-system", InstallationUID: "installation-uid", OperationID: canonical.OperationID, TopologyDigest: canonical.TopologyDigest, SourceVersion: canonical.SourceVersion, TargetVersion: canonical.TargetVersion, BackupRef: canonical.BackupPairID, DatabaseRefs: map[string]string{"OVN_Northbound": canonical.Databases[0].BackupRef, "OVN_Southbound": canonical.Databases[1].BackupRef}, ApprovalRef: &operatorv1alpha1.RecoveryApprovalReference{Name: canonical.Approval.Name, UID: types.UID(canonical.Approval.UID)}, Mode: operatorv1alpha1.RecoveryModeFreshBootstrap, Digest: admissionDigest}
	controller, blockDeletion := true, true
	owner := metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNRecoveryRequest", Name: plan.RequestName, UID: plan.RequestUID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}
	proof := recovery.IsolationProof{PlanDigest: builderCanonicalDigest(t, canonical), OperationID: canonical.OperationID, RetainedMembers: []string{"old"}, VerifiedAt: now}
	artifacts, err := RenderOfflineRebuildArtifacts(OfflineRebuildArtifactInputs{Plan: plan, CanonicalPlan: canonical, IsolationProof: proof, ProofObservedAt: now, PlanConfigMap: "rebuild-plan", ProofConfigMap: "rebuild-proof", BackupPVCName: "backup", BackupPVCNamespace: plan.RequestNamespace, Owner: owner, OperationID: canonical.OperationID, ApprovalDecisionDigest: canonical.Approval.DecisionDigest, CanonicalPlanDigest: builderCanonicalDigest(t, canonical)})
	if err != nil {
		t.Fatal(err)
	}
	if artifacts.PlanConfigMap.Immutable == nil || !*artifacts.PlanConfigMap.Immutable || artifacts.PlanConfigMap.Data["plan.json"] == "" || artifacts.ProofConfigMap.Data["proof.json"] == "" {
		t.Fatalf("renderer did not create immutable artifact data: %+v", artifacts)
	}
	if artifacts.PlanConfigMap.Annotations[recovery.RebuildCanonicalPlanDigestAnnotation] != artifacts.CanonicalPlanDigest || artifacts.PlanDataDigest != artifacts.CanonicalPlanDigest || artifacts.BackupPairID != plan.BackupRef {
		t.Fatalf("renderer lost canonical or backup binding: %+v", artifacts)
	}
	if len(artifacts.PlanConfigMap.Data) != 1 || len(artifacts.ProofConfigMap.Data) != 1 {
		t.Fatal("renderer emitted extra ConfigMap data keys")
	}
}

func TestRenderOfflineRebuildArtifactsRejectsBindingDrift(t *testing.T) {
	now := time.Date(2026, time.November, 11, 10, 0, 0, 0, time.UTC)
	canonical := builderTestPlan()
	plan := RecoveryPlan{RequestNamespace: "ovn-system", RequestName: "recover", RequestUID: "request-uid", InstallationNamespace: "ovn-system", InstallationUID: "installation-uid", OperationID: canonical.OperationID, TopologyDigest: canonical.TopologyDigest, SourceVersion: canonical.SourceVersion, TargetVersion: canonical.TargetVersion, BackupRef: canonical.BackupPairID, DatabaseRefs: map[string]string{"OVN_Northbound": canonical.Databases[0].BackupRef, "OVN_Southbound": canonical.Databases[1].BackupRef}, ApprovalRef: &operatorv1alpha1.RecoveryApprovalReference{Name: canonical.Approval.Name, UID: types.UID(canonical.Approval.UID)}, Mode: operatorv1alpha1.RecoveryModeFreshBootstrap, Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
	controller, blockDeletion := true, true
	owner := metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNRecoveryRequest", Name: plan.RequestName, UID: plan.RequestUID, Controller: &controller, BlockOwnerDeletion: &blockDeletion}
	base := OfflineRebuildArtifactInputs{Plan: plan, CanonicalPlan: canonical, IsolationProof: recovery.IsolationProof{PlanDigest: builderCanonicalDigest(t, canonical), OperationID: canonical.OperationID, RetainedMembers: []string{"old"}, VerifiedAt: now}, ProofObservedAt: now, PlanConfigMap: "rebuild-plan", ProofConfigMap: "rebuild-proof", BackupPVCName: "backup", BackupPVCNamespace: plan.RequestNamespace, Owner: owner, OperationID: canonical.OperationID, ApprovalDecisionDigest: canonical.Approval.DecisionDigest, CanonicalPlanDigest: builderCanonicalDigest(t, canonical)}
	cases := []struct {
		name string
		edit func(*OfflineRebuildArtifactInputs)
		want string
	}{
		{name: "wrong backup namespace", edit: func(in *OfflineRebuildArtifactInputs) { in.BackupPVCNamespace = "other" }, want: "backup PVC"},
		{name: "wrong operation", edit: func(in *OfflineRebuildArtifactInputs) { in.OperationID = "11111111-1111-1111-1111-111111111111" }, want: "operation ID"},
		{name: "topology drift", edit: func(in *OfflineRebuildArtifactInputs) { in.Plan.TopologyDigest = "sha256:" + strings.Repeat("a", 64) }, want: "topology digest"},
		{name: "invalid approval decision digest", edit: func(in *OfflineRebuildArtifactInputs) {
			in.ApprovalDecisionDigest = "sha256:" + strings.Repeat("f", 64)
		}, want: "approval decision digest"},
		{name: "proof operation drift", edit: func(in *OfflineRebuildArtifactInputs) {
			in.IsolationProof.OperationID = "11111111-1111-1111-1111-111111111111"
		}, want: "isolation proof"},
		{name: "unknown mode", edit: func(in *OfflineRebuildArtifactInputs) { in.Plan.Mode = operatorv1alpha1.RecoveryMode("unexpected") }, want: "reconvert or freshBootstrap"},
		{name: "missing proof clock", edit: func(in *OfflineRebuildArtifactInputs) { in.ProofObservedAt = time.Time{} }, want: "observation time"},
		{name: "foreign owner", edit: func(in *OfflineRebuildArtifactInputs) { in.Owner.UID = "foreign" }, want: "owner"},
		{name: "expired proof", edit: func(in *OfflineRebuildArtifactInputs) { in.ProofObservedAt = now.Add(6 * time.Minute) }, want: "proof"},
		{name: "duplicate names", edit: func(in *OfflineRebuildArtifactInputs) { in.ProofConfigMap = in.PlanConfigMap }, want: "distinct"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := base
			tc.edit(&inputs)
			if _, err := RenderOfflineRebuildArtifacts(inputs); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("drift was accepted or wrong error: %v", err)
			}
		})
	}
}

func builderCanonicalDigest(t *testing.T, plan recovery.Plan) string {
	t.Helper()
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
