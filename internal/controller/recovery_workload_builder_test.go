package controller

import (
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	"k8s.io/apimachinery/pkg/types"
)

func TestStaticOfflineRebuildWorkloadBuilderBindsCanonicalAndAdmissionDigests(t *testing.T) {
	canonical := builderTestPlan()
	canonicalJSON, canonicalDigest := builderCanonicalData(t, canonical)
	admissionDigest := "sha256:" + strings.Repeat("e", 64)
	inputs := OfflineRebuildWorkloadInputs{
		AdmissionPlanDigest: admissionDigest, CanonicalPlan: canonical, MemberName: "new", NodeName: "node-6",
		Image: "example/recovery@sha256:" + strings.Repeat("f", 64), ServiceAccountName: "recovery",
		PlanConfigMapName: "rebuild-plan", PlanConfigMapUID: "plan-uid", PlanConfigMapData: canonicalJSON, PlanConfigMapDataDigest: canonicalDigest,
		ProofConfigMapName: "rebuild-proof", ProofConfigMapUID: "proof-uid", ProofConfigMapData: "proof", ProofConfigMapDataDigest: recovery.ConfigMapDataDigest("proof"),
		Backup:        recovery.WorkloadVolume{Name: "backup", Claim: "backup", ClaimUID: "backup-uid", PVUID: "backup-pv-uid"},
		Target:        recovery.WorkloadVolume{Name: "target", Claim: "target", ClaimUID: "target-uid", PVUID: "target-pv-uid"},
		Retained:      []recovery.WorkloadVolume{{Name: "old", Claim: "old", ClaimUID: "old-uid", PVUID: "old-pv-uid", ReadOnly: true}},
		TargetStorage: builderStorage("6"), RetainedStorage: map[string]recovery.StorageIdentity{"old": builderStorage("1")},
	}
	builder, err := NewStaticOfflineRebuildWorkloadBuilder(inputs)
	if err != nil {
		t.Fatal(err)
	}
	plan := RecoveryPlan{RequestNamespace: "ovn-system", RequestName: "recover", RequestUID: types.UID("request-uid"), RequestGeneration: 1, InstallationUID: "installation-uid", Mode: operatorv1alpha1.RecoveryModeFreshBootstrap, Digest: admissionDigest}
	spec, err := builder.Build(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if spec.PlanDigest != admissionDigest || spec.CanonicalPlanDigest == admissionDigest || spec.CanonicalPlanDigest == "" {
		t.Fatalf("builder did not separate admission and canonical digests: %+v", spec)
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("builder returned an invalid workload: %v", err)
	}
	inputs.PlanConfigMapData = "tampered"
	if _, err := NewStaticOfflineRebuildWorkloadBuilder(inputs); err == nil || !strings.Contains(err.Error(), "content does not match") {
		t.Fatalf("plan ConfigMap content drift was accepted: %v", err)
	}
}

func TestStaticOfflineRebuildWorkloadBuilderRejectsStorageDrift(t *testing.T) {
	canonical := builderTestPlan()
	canonicalJSON, canonicalDigest := builderCanonicalData(t, canonical)
	inputs := OfflineRebuildWorkloadInputs{AdmissionPlanDigest: "sha256:" + strings.Repeat("e", 64), CanonicalPlan: canonical, MemberName: "new", NodeName: "node-6", Image: "example/recovery@sha256:" + strings.Repeat("f", 64), ServiceAccountName: "recovery", PlanConfigMapName: "plan", PlanConfigMapUID: "plan-uid", PlanConfigMapData: canonicalJSON, PlanConfigMapDataDigest: canonicalDigest, ProofConfigMapName: "proof", ProofConfigMapUID: "proof-uid", ProofConfigMapData: "proof", ProofConfigMapDataDigest: recovery.ConfigMapDataDigest("proof"), Backup: recovery.WorkloadVolume{Name: "backup", Claim: "backup", ClaimUID: "backup-uid", PVUID: "backup-pv-uid"}, Target: recovery.WorkloadVolume{Name: "target", Claim: "target", ClaimUID: "target-uid", PVUID: "target-pv-uid"}, Retained: []recovery.WorkloadVolume{{Name: "old", Claim: "old", ClaimUID: "old-uid", PVUID: "old-pv-uid", ReadOnly: true}}, TargetStorage: builderStorage("different"), RetainedStorage: map[string]recovery.StorageIdentity{"old": builderStorage("1")}}
	if _, err := NewStaticOfflineRebuildWorkloadBuilder(inputs); err == nil || !strings.Contains(err.Error(), "target member identity") {
		t.Fatalf("storage drift was accepted: %v", err)
	}
}

func builderCanonicalData(t *testing.T, plan recovery.Plan) (string, string) {
	t.Helper()
	canonical, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	return string(data), digest
}

func builderTestPlan() recovery.Plan {
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("a", 64), "OVN_Southbound": "sha256:" + strings.Repeat("b", 64)}
	data, _ := json.Marshal(refs, json.Deterministic(true))
	return recovery.Plan{FormatVersion: recovery.FormatVersion, InstallationUID: "installation-uid", OperationID: "eeee0000-1111-2222-3333-444444444444", SourceVersion: "v1.16.13", TargetVersion: "v1.16.13", BackupPairID: fmt.Sprintf("sha256:%x", sha256.Sum256(data)), Approval: recovery.Approval{Name: "approval", UID: "approval-uid", DecisionDigest: "sha256:" + strings.Repeat("c", 64)}, RetainedMembers: []recovery.Member{{Name: "old", NodeName: "node-1", Storage: builderStorage("1"), NorthboundRAFTAddress: "tcp:192.0.2.1:6643", SouthboundRAFTAddress: "tcp:192.0.2.1:6644", NorthboundClient: "tcp:192.0.2.1:6641", SouthboundClient: "tcp:192.0.2.1:6642"}}, TargetMembers: []recovery.Member{{Name: "new", NodeName: "node-6", Storage: builderStorage("6"), NorthboundRAFTAddress: "tcp:192.0.2.6:6643", SouthboundRAFTAddress: "tcp:192.0.2.6:6644", NorthboundClient: "tcp:192.0.2.6:6641", SouthboundClient: "tcp:192.0.2.6:6642"}}, Databases: []recovery.Database{{Name: "OVN_Northbound", BackupRef: refs["OVN_Northbound"], SourceClusterID: "aaaa0000-1111-2222-3333-444444444444", TargetClusterID: "cccc0000-1111-2222-3333-444444444444"}, {Name: "OVN_Southbound", BackupRef: refs["OVN_Southbound"], SourceClusterID: "bbbb0000-1111-2222-3333-444444444444", TargetClusterID: "dddd0000-1111-2222-3333-444444444444"}}}
}

func builderStorage(suffix string) recovery.StorageIdentity {
	return recovery.StorageIdentity{NodeUID: "node-uid-" + suffix, WorkloadUID: "workload-uid-" + suffix, PodUID: "pod-uid-" + suffix, ContainerID: "container-" + suffix, VolumeUID: "volume-uid-" + suffix}
}
