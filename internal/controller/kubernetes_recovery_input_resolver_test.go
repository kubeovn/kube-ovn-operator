package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKubernetesOfflineRebuildWorkloadResolverResolvesExplicitIdentities(t *testing.T) {
	canonical := builderTestPlan()
	canonicalJSON, canonicalDigest := builderCanonicalData(t, canonical)
	const admissionDigest = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	plan := RecoveryPlan{RequestNamespace: "ovn-system", RequestName: "recover", RequestUID: "request-uid", InstallationNamespace: "ovn-system", InstallationName: "installation", InstallationUID: "installation-uid", SourceVersion: canonical.SourceVersion, TargetVersion: canonical.TargetVersion, ApprovalRef: &operatorv1alpha1.RecoveryApprovalReference{Name: canonical.Approval.Name, UID: types.UID(canonical.Approval.UID)}, Digest: admissionDigest, BackupRef: canonical.BackupPairID, DatabaseRefs: map[string]string{"OVN_Northbound": canonical.Databases[0].BackupRef, "OVN_Southbound": canonical.Databases[1].BackupRef}}
	request := &operatorv1alpha1.KubeOVNRecoveryRequest{ObjectMeta: metav1.ObjectMeta{Name: plan.RequestName, Namespace: plan.RequestNamespace, UID: plan.RequestUID}}
	owner := recoveryRequestOwner(request)
	planConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: plan.RequestNamespace, UID: "plan-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: admissionDigest, recovery.RebuildCanonicalPlanDigestAnnotation: canonicalDigest}}, Immutable: new(true), Data: map[string]string{"plan.json": canonicalJSON}}
	proofConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "proof", Namespace: plan.RequestNamespace, UID: "proof-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: admissionDigest, recovery.RebuildCanonicalPlanDigestAnnotation: canonicalDigest}}, Immutable: new(true), Data: map[string]string{"proof.json": "proof"}}
	backupPVC, backupPV := resolverPVC("backup", "backup-uid", "backup-pv-uid", "backup-pv")
	backupPVC.Annotations = map[string]string{RecoveryBackupPairAnnotation: canonical.BackupPairID}
	targetPVC, targetPV := resolverPVC("target", "volume-uid-6", "target-pv-uid", "target-pv")
	retainedPVC, retainedPV := resolverPVC("old", "volume-uid-1", "retained-pv-uid", "retained-pv")
	node6 := resolverNode("node-6", "node-uid-6")
	node1 := resolverNode("node-1", "node-uid-1")
	targetPod := resolverPod("pod-uid-6", "node-6", "workload-uid-6", "container-6", "target")
	retainedPod := resolverPod("pod-uid-1", "node-1", "workload-uid-1", "container-1", "old")
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithObjects(planConfigMap, proofConfigMap, backupPVC, backupPV, targetPVC, targetPV, retainedPVC, retainedPV, node6, node1, targetPod, retainedPod).Build()
	resolver := &KubernetesOfflineRebuildWorkloadResolver{Reader: kube, Namespace: plan.RequestNamespace, PlanConfigMapName: "plan", ProofConfigMapName: "proof", BackupClaimName: "backup", TargetMemberName: "new", TargetClaimName: "target", RetainedClaimNames: map[string]string{"old": "old"}, Image: "example/recovery@sha256:" + strings.Repeat("f", 64), ServiceAccountName: "recovery"}
	inputs, err := resolver.Resolve(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if inputs.Target.Claim != "target" || inputs.Target.PVUID != "target-pv-uid" || inputs.TargetStorage != canonical.TargetMembers[0].Storage || inputs.Backup.Claim != "backup" || inputs.Retained[0].Claim != "old" {
		t.Fatalf("resolver returned incorrect live identities: %+v", inputs)
	}
	if _, err := NewStaticOfflineRebuildWorkloadBuilder(inputs); err != nil {
		t.Fatalf("resolver output did not satisfy the static workload contract: %v", err)
	}
}

func TestKubernetesOfflineRebuildWorkloadResolverRejectsReadinessAndDrift(t *testing.T) {
	canonical := builderTestPlan()
	canonicalJSON, canonicalDigest := builderCanonicalData(t, canonical)
	const admissionDigest = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	plan := RecoveryPlan{RequestNamespace: "ovn-system", RequestName: "recover", RequestUID: "request-uid", InstallationNamespace: "ovn-system", InstallationName: "installation", InstallationUID: "installation-uid", SourceVersion: canonical.SourceVersion, TargetVersion: canonical.TargetVersion, ApprovalRef: &operatorv1alpha1.RecoveryApprovalReference{Name: canonical.Approval.Name, UID: types.UID(canonical.Approval.UID)}, Digest: admissionDigest, BackupRef: canonical.BackupPairID, DatabaseRefs: map[string]string{"OVN_Northbound": canonical.Databases[0].BackupRef, "OVN_Southbound": canonical.Databases[1].BackupRef}}
	request := &operatorv1alpha1.KubeOVNRecoveryRequest{ObjectMeta: metav1.ObjectMeta{Name: plan.RequestName, Namespace: plan.RequestNamespace, UID: plan.RequestUID}}
	owner := recoveryRequestOwner(request)
	planConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "plan", Namespace: plan.RequestNamespace, UID: "plan-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: admissionDigest, recovery.RebuildCanonicalPlanDigestAnnotation: canonicalDigest}}, Immutable: new(true), Data: map[string]string{"plan.json": canonicalJSON}}
	proofConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "proof", Namespace: plan.RequestNamespace, UID: "proof-uid", OwnerReferences: []metav1.OwnerReference{owner}, Annotations: map[string]string{recovery.RebuildPlanDigestAnnotation: admissionDigest, recovery.RebuildCanonicalPlanDigestAnnotation: canonicalDigest}}, Immutable: new(true), Data: map[string]string{"proof.json": "proof"}}
	backupPVC, backupPV := resolverPVC("backup", "backup-uid", "backup-pv-uid", "backup-pv")
	backupPVC.Annotations = map[string]string{RecoveryBackupPairAnnotation: canonical.BackupPairID}
	targetPVC, targetPV := resolverPVC("target", "volume-uid-6", "target-pv-uid", "target-pv")
	retainedPVC, retainedPV := resolverPVC("old", "volume-uid-1", "retained-pv-uid", "retained-pv")
	objects := []client.Object{planConfigMap, proofConfigMap, backupPVC, backupPV, targetPVC, targetPV, retainedPVC, retainedPV, resolverNode("node-6", "node-uid-6"), resolverNode("node-1", "node-uid-1"), resolverPod("pod-uid-6", "node-6", "workload-uid-6", "container-6", "target"), resolverPod("pod-uid-1", "node-1", "workload-uid-1", "container-1", "old")}
	resolver := &KubernetesOfflineRebuildWorkloadResolver{Namespace: plan.RequestNamespace, PlanConfigMapName: "plan", ProofConfigMapName: "proof", BackupClaimName: "backup", TargetMemberName: "new", TargetClaimName: "target", RetainedClaimNames: map[string]string{"old": "old"}, Image: "example/recovery@sha256:" + strings.Repeat("f", 64), ServiceAccountName: "recovery"}
	missing := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithObjects(objects[1:]...).Build()
	resolver.Reader = missing
	if _, err := resolver.Resolve(t.Context(), plan); !errors.Is(err, ErrRecoveryWorkloadNotReady) {
		t.Fatalf("missing plan ConfigMap did not remain not ready: %v", err)
	}
	for _, missingName := range []string{"target-pv", "pod-uid-6", "node-6"} {
		filtered := make([]client.Object, 0, len(objects)-1)
		for _, object := range objects {
			if object.GetName() != missingName {
				filtered = append(filtered, object)
			}
		}
		resolver.Reader = fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithObjects(filtered...).Build()
		if _, err := resolver.Resolve(t.Context(), plan); !errors.Is(err, ErrRecoveryWorkloadNotReady) {
			t.Fatalf("missing %s did not remain not ready: %v", missingName, err)
		}
	}
	driftedPVC := targetPVC.DeepCopy()
	driftedPVC.UID = "wrong-volume-uid"
	objectsWithDrift := make([]client.Object, 0, len(objects))
	for _, object := range objects {
		if object.GetName() == targetPVC.Name {
			objectsWithDrift = append(objectsWithDrift, driftedPVC)
			continue
		}
		objectsWithDrift = append(objectsWithDrift, object)
	}
	resolver.Reader = fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithObjects(objectsWithDrift...).Build()
	if _, err := resolver.Resolve(t.Context(), plan); err == nil || !strings.Contains(err.Error(), "identity or binding") {
		t.Fatalf("PVC UID drift was accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := resolver.Resolve(ctx, plan); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation was not preserved: %v", err)
	}
}

func resolverPVC(name string, uid types.UID, pvUID types.UID, pvName string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ovn-system", UID: uid}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: pvName}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvName, UID: pvUID}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: name, Namespace: "ovn-system", UID: uid}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	return pvc, pv
}

func resolverNode(name string, uid types.UID) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
}

func resolverPod(uid types.UID, nodeName string, workloadUID types.UID, containerID, claimName string) *corev1.Pod {
	controller := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: string(uid), Namespace: "ovn-system", UID: uid, OwnerReferences: []metav1.OwnerReference{{UID: workloadUID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: nodeName, Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}}}, Containers: []corev1.Container{{Name: "ovn-central", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/ovn"}}}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: containerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
}
