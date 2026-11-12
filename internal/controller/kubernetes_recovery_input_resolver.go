package controller

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RecoveryBackupPairAnnotation binds an explicitly selected backup PVC to the
// authenticated pair recorded in the recovery plan. A resolver must never
// infer a backup PVC from a file or object-storage reference.
const RecoveryBackupPairAnnotation = recovery.BackupPairAnnotation

// KubernetesOfflineRebuildWorkloadResolver resolves one read-only Kubernetes
// snapshot for a destructive rebuild. Every selector is explicit: this type
// does not discover ConfigMaps, PVCs, or target members from names guessed from
// the recovery plan.
type KubernetesOfflineRebuildWorkloadResolver struct {
	Reader client.Reader

	Namespace          string
	PlanConfigMapName  string
	ProofConfigMapName string
	BackupClaimName    string
	TargetMemberName   string
	TargetClaimName    string
	RetainedClaimNames map[string]string
	Image              string
	ServiceAccountName string
}

func (r *KubernetesOfflineRebuildWorkloadResolver) Resolve(ctx context.Context, plan RecoveryPlan) (OfflineRebuildWorkloadInputs, error) {
	if r == nil || r.Reader == nil {
		return OfflineRebuildWorkloadInputs{}, errors.New("kubernetes offline rebuild resolver requires a live client")
	}
	if err := ctx.Err(); err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	if err := r.validateOptions(plan); err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	namespace := r.Namespace
	request := &operatorv1alpha1.KubeOVNRecoveryRequest{ObjectMeta: metav1.ObjectMeta{Name: plan.RequestName, Namespace: plan.RequestNamespace, UID: plan.RequestUID}}
	canonical, canonicalDigest, planData, planConfigMap, err := r.resolvePlanConfigMap(ctx, plan, request)
	if err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	if len(canonical.TargetMembers) != 1 || canonical.TargetMembers[0].Name != r.TargetMemberName {
		return OfflineRebuildWorkloadInputs{}, errors.New("kubernetes offline rebuild resolver target member is not the sole canonical target")
	}
	if len(r.RetainedClaimNames) != len(canonical.RetainedMembers) {
		return OfflineRebuildWorkloadInputs{}, errors.New("kubernetes offline rebuild resolver requires one explicit PVC for every retained member")
	}
	for name := range r.RetainedClaimNames {
		found := false
		for _, member := range canonical.RetainedMembers {
			found = found || member.Name == name
		}
		if !found {
			return OfflineRebuildWorkloadInputs{}, fmt.Errorf("kubernetes offline rebuild resolver has an unknown retained member %q", name)
		}
	}
	proofData, proofConfigMap, err := r.resolveProofConfigMap(ctx, plan, request, canonicalDigest)
	if err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	backup, err := r.resolvePVC(ctx, namespace, r.BackupClaimName, "backup", false, "")
	if err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	if backup.pvc.Annotations[RecoveryBackupPairAnnotation] != canonical.BackupPairID || canonical.BackupPairID != plan.BackupRef {
		return OfflineRebuildWorkloadInputs{}, errors.New("backup PVC is not bound to the admitted recovery pair")
	}

	targetMember := canonical.TargetMembers[0]
	target, err := r.resolveMember(ctx, namespace, targetMember, r.TargetClaimName, "target", false)
	if err != nil {
		return OfflineRebuildWorkloadInputs{}, err
	}
	retained := make([]recovery.WorkloadVolume, 0, len(canonical.RetainedMembers))
	retainedStorage := make(map[string]recovery.StorageIdentity, len(canonical.RetainedMembers))
	for _, member := range canonical.RetainedMembers {
		claimName := r.RetainedClaimNames[member.Name]
		resolved, err := r.resolveMember(ctx, namespace, member, claimName, member.Name, true)
		if err != nil {
			return OfflineRebuildWorkloadInputs{}, err
		}
		retained = append(retained, resolved.volume)
		retainedStorage[member.Name] = member.Storage
	}

	return OfflineRebuildWorkloadInputs{
		AdmissionPlanDigest:      plan.Digest,
		CanonicalPlan:            canonical,
		MemberName:               targetMember.Name,
		NodeName:                 targetMember.NodeName,
		Image:                    r.Image,
		ServiceAccountName:       r.ServiceAccountName,
		PlanConfigMapName:        planConfigMap.Name,
		PlanConfigMapUID:         planConfigMap.UID,
		PlanConfigMapData:        planData,
		PlanConfigMapDataDigest:  recovery.ConfigMapDataDigest(planData),
		ProofConfigMapName:       proofConfigMap.Name,
		ProofConfigMapUID:        proofConfigMap.UID,
		ProofConfigMapData:       proofData,
		ProofConfigMapDataDigest: recovery.ConfigMapDataDigest(proofData),
		Backup:                   backup.volume,
		Target:                   target.volume,
		Retained:                 retained,
		TargetStorage:            targetMember.Storage,
		RetainedStorage:          retainedStorage,
	}, nil
}

type resolvedVolume struct {
	volume recovery.WorkloadVolume
	pvc    *corev1.PersistentVolumeClaim
}

func (r *KubernetesOfflineRebuildWorkloadResolver) validateOptions(plan RecoveryPlan) error {
	for field, value := range map[string]string{
		"namespace": r.Namespace, "plan ConfigMap": r.PlanConfigMapName, "proof ConfigMap": r.ProofConfigMapName,
		"backup PVC": r.BackupClaimName, "target member": r.TargetMemberName, "target PVC": r.TargetClaimName,
		"image": r.Image, "service account": r.ServiceAccountName,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("kubernetes offline rebuild resolver requires %s", field)
		}
	}
	if r.Namespace != plan.RequestNamespace || plan.RequestNamespace != plan.InstallationNamespace {
		return errors.New("kubernetes offline rebuild resolver namespace does not match the admitted plan")
	}
	if plan.InstallationGeneration < 1 || plan.RequestGeneration < 1 || plan.OperationID == "" || plan.SourceVersion == "" || plan.TargetVersion == "" || plan.ChartDigest == "" || plan.TopologyDigest == "" {
		return errors.New("kubernetes offline rebuild resolver requires a complete admitted plan identity")
	}
	if len(plan.DatabaseRefs) != 2 || plan.BackupRef == "" {
		return errors.New("kubernetes offline rebuild resolver requires an admitted backup pair")
	}
	return nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) resolvePlanConfigMap(ctx context.Context, plan RecoveryPlan, request *operatorv1alpha1.KubeOVNRecoveryRequest) (recovery.Plan, string, string, *corev1.ConfigMap, error) {
	object, err := r.configMap(ctx, r.PlanConfigMapName, plan, request)
	if err != nil {
		return recovery.Plan{}, "", "", nil, err
	}
	data := object.Data["plan.json"]
	if strings.TrimSpace(data) == "" {
		return recovery.Plan{}, "", "", nil, errors.New("offline rebuild plan ConfigMap has no plan.json")
	}
	var decoded recovery.Plan
	if err := json.Unmarshal([]byte(data), &decoded); err != nil {
		return recovery.Plan{}, "", "", nil, fmt.Errorf("decode offline rebuild plan.json: %w", err)
	}
	canonical, digest, err := decoded.Canonical()
	if err != nil {
		return recovery.Plan{}, "", "", nil, fmt.Errorf("validate offline rebuild plan.json: %w", err)
	}
	if object.Annotations[recovery.RebuildPlanDigestAnnotation] != plan.Digest || object.Annotations[recovery.RebuildCanonicalPlanDigestAnnotation] != digest || recovery.ConfigMapDataDigest(data) != digest {
		return recovery.Plan{}, "", "", nil, errors.New("offline rebuild plan ConfigMap content or digest is not bound to the admitted plan")
	}
	if canonical.InstallationUID != string(plan.InstallationUID) || canonical.OperationID != plan.OperationID || canonical.TopologyDigest != plan.TopologyDigest || canonical.SourceVersion != plan.SourceVersion || canonical.TargetVersion != plan.TargetVersion || canonical.BackupPairID != plan.BackupRef || !databaseRefsMatch(canonical.Databases, plan.DatabaseRefs) || plan.ApprovalRef == nil || canonical.Approval.Name != plan.ApprovalRef.Name || types.UID(canonical.Approval.UID) != plan.ApprovalRef.UID {
		return recovery.Plan{}, "", "", nil, errors.New("offline rebuild canonical plan is not bound to the admitted plan")
	}
	if err := r.resolveApproval(ctx, plan, canonical); err != nil {
		return recovery.Plan{}, "", "", nil, err
	}
	return canonical, digest, data, object, nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) resolveApproval(ctx context.Context, plan RecoveryPlan, canonical recovery.Plan) error {
	if plan.ApprovalRef == nil || strings.TrimSpace(plan.ApprovalRef.Name) == "" || plan.ApprovalRef.UID == "" {
		return errors.New("offline rebuild recovery approval binding is missing")
	}
	binding := approval.RecoveryBinding{
		InstallationName: plan.InstallationName, InstallationUID: plan.InstallationUID, InstallationGeneration: plan.InstallationGeneration,
		RequestName: plan.RequestName, RequestUID: plan.RequestUID, RequestGeneration: plan.RequestGeneration,
		Mode: string(plan.Mode), SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, BackupRef: plan.BackupRef,
		TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest,
	}
	digest, err := approval.ValidateRecoveryApproval(ctx, r.Reader, r.Namespace, operatorv1alpha1.InstallationReference{Name: plan.InstallationName, UID: plan.InstallationUID}, *plan.ApprovalRef, binding, true)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: offline rebuild recovery approval %q is not available", ErrRecoveryWorkloadNotReady, plan.ApprovalRef.Name)
		}
		return fmt.Errorf("validate offline rebuild recovery approval %q: %w", plan.ApprovalRef.Name, err)
	}
	if digest != canonical.Approval.DecisionDigest || !validRecoverySHA256(digest) {
		return errors.New("offline rebuild recovery approval decision digest does not match the canonical plan")
	}
	approvalRecord := new(operatorv1alpha1.KubeOVNRecoveryApproval)
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: plan.ApprovalRef.Name}, approvalRecord); err != nil {
		return fmt.Errorf("read offline rebuild recovery approval status: %w", err)
	}
	if approvalRecord.Status.DecisionDigest != digest {
		return errors.New("offline rebuild recovery approval decision digest does not match the canonical plan")
	}
	if err := approval.ValidateRecoveryApprovalStatus(approvalRecord, digest); err != nil {
		return fmt.Errorf("offline rebuild recovery approval reservation audit is invalid: %w", err)
	}
	return nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) resolveProofConfigMap(ctx context.Context, plan RecoveryPlan, request *operatorv1alpha1.KubeOVNRecoveryRequest, canonicalDigest string) (string, *corev1.ConfigMap, error) {
	object, err := r.configMap(ctx, r.ProofConfigMapName, plan, request)
	if err != nil {
		return "", nil, err
	}
	data := object.Data["proof.json"]
	if strings.TrimSpace(data) == "" {
		return "", nil, errors.New("offline rebuild proof ConfigMap has no proof.json")
	}
	if object.Annotations[recovery.RebuildPlanDigestAnnotation] != plan.Digest || object.Annotations[recovery.RebuildCanonicalPlanDigestAnnotation] != canonicalDigest {
		return "", nil, errors.New("offline rebuild proof ConfigMap is not bound to the admitted plan")
	}
	return data, object, nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) configMap(ctx context.Context, name string, plan RecoveryPlan, request *operatorv1alpha1.KubeOVNRecoveryRequest) (*corev1.ConfigMap, error) {
	object := new(corev1.ConfigMap)
	err := r.Reader.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: name}, object)
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: offline rebuild ConfigMap %q is not available", ErrRecoveryWorkloadNotReady, name)
	}
	if err != nil {
		return nil, fmt.Errorf("read offline rebuild ConfigMap %q: %w", name, err)
	}
	if object.DeletionTimestamp != nil || object.Immutable == nil || !*object.Immutable || !hasRecoveryRequestOwnerConfigMap(object, request) {
		return nil, fmt.Errorf("offline rebuild ConfigMap %q is mutable, deleting, or owned by another request", name)
	}
	return object, nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) resolveMember(ctx context.Context, namespace string, member recovery.Member, claimName, volumeName string, readOnly bool) (resolvedVolume, error) {
	if strings.TrimSpace(claimName) == "" {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q has no explicit PVC", member.Name)
	}
	resolved, err := r.resolvePVC(ctx, namespace, claimName, volumeName, readOnly, member.Storage.VolumeUID)
	if err != nil {
		return resolvedVolume{}, fmt.Errorf("resolve offline rebuild member %q: %w", member.Name, err)
	}
	pod, err := podByUID(ctx, r.Reader, namespace, member.Storage.PodUID)
	if err != nil {
		return resolvedVolume{}, fmt.Errorf("resolve offline rebuild member %q Pod: %w", member.Name, err)
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != member.NodeName || !podHasClaim(pod, claimName) {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q Pod identity or PVC mapping changed", member.Name)
	}
	if err := verifyControllerOwner(pod, member.Storage.WorkloadUID); err != nil {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q: %w", member.Name, err)
	}
	containerID, ok := containerIDFor(pod, "ovn-central")
	if !ok || containerID != member.Storage.ContainerID {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q container identity is not running", member.Name)
	}
	node := new(corev1.Node)
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: member.NodeName}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return resolvedVolume{}, fmt.Errorf("%w: offline rebuild member %q node is not available", ErrRecoveryWorkloadNotReady, member.Name)
		}
		return resolvedVolume{}, fmt.Errorf("read offline rebuild member %q node: %w", member.Name, err)
	}
	if node.UID != types.UID(member.Storage.NodeUID) || node.DeletionTimestamp != nil {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q node identity changed", member.Name)
	}
	if !readOnly && !podMountsWritableClaim(pod, claimName) {
		return resolvedVolume{}, fmt.Errorf("offline rebuild member %q PVC is not mounted writable by ovn-central", member.Name)
	}
	resolved.volume.ReadOnly = readOnly
	return resolved, nil
}

func (r *KubernetesOfflineRebuildWorkloadResolver) resolvePVC(ctx context.Context, namespace, claimName, volumeName string, readOnly bool, expectedUID string) (resolvedVolume, error) {
	pvc := new(corev1.PersistentVolumeClaim)
	err := r.Reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: claimName}, pvc)
	if apierrors.IsNotFound(err) {
		return resolvedVolume{}, fmt.Errorf("%w: offline rebuild PVC %q is not available", ErrRecoveryWorkloadNotReady, claimName)
	}
	if err != nil {
		return resolvedVolume{}, fmt.Errorf("read offline rebuild PVC %q: %w", claimName, err)
	}
	if pvc.DeletionTimestamp != nil || (expectedUID != "" && string(pvc.UID) != expectedUID) || pvc.Status.Phase != corev1.ClaimBound || strings.TrimSpace(pvc.Spec.VolumeName) == "" {
		return resolvedVolume{}, fmt.Errorf("offline rebuild PVC %q identity or binding changed", claimName)
	}
	pv := new(corev1.PersistentVolume)
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, pv); err != nil {
		if apierrors.IsNotFound(err) {
			return resolvedVolume{}, fmt.Errorf("%w: offline rebuild PV for PVC %q is not available", ErrRecoveryWorkloadNotReady, claimName)
		}
		return resolvedVolume{}, fmt.Errorf("read offline rebuild PV for PVC %q: %w", claimName, err)
	}
	if pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != pvc.Name || pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.UID != pvc.UID {
		return resolvedVolume{}, fmt.Errorf("offline rebuild PV for PVC %q is not bidirectionally bound", claimName)
	}
	return resolvedVolume{volume: recovery.WorkloadVolume{Name: volumeName, Claim: pvc.Name, ClaimUID: pvc.UID, PVUID: pv.UID, ReadOnly: readOnly}, pvc: pvc}, nil
}

func podHasClaim(pod *corev1.Pod, claimName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claimName {
			return true
		}
	}
	return false
}

func podMountsWritableClaim(pod *corev1.Pod, claimName string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != claimName {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name != "ovn-central" {
				continue
			}
			for _, mount := range container.VolumeMounts {
				if mount.Name == volume.Name && strings.TrimSpace(mount.MountPath) != "" && !mount.ReadOnly {
					return true
				}
			}
		}
	}
	return false
}

func podByUID(ctx context.Context, reader client.Reader, namespace, uid string) (*corev1.Pod, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(uid) == "" {
		return nil, errors.New("pod ownership lookup requires namespace and UID")
	}
	list := new(corev1.PodList)
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var match *corev1.Pod
	for index := range list.Items {
		pod := &list.Items[index]
		if string(pod.UID) != uid {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("multiple Pods with UID %q", uid)
		}
		match = pod
	}
	if match == nil {
		return nil, fmt.Errorf("%w: no Pod with UID %q in namespace %q", ErrRecoveryWorkloadNotReady, uid, namespace)
	}
	return match, nil
}

func verifyControllerOwner(pod *corev1.Pod, workloadUID string) error {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && string(owner.UID) == workloadUID {
			return nil
		}
	}
	return errors.New("pod controller workload identity does not match the recovery plan")
}

func containerIDFor(pod *corev1.Pod, name string) (string, bool) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == name && status.State.Running != nil && strings.TrimSpace(status.ContainerID) != "" {
			return status.ContainerID, true
		}
	}
	return "", false
}

func databaseRefsMatch(databases []recovery.Database, refs map[string]string) bool {
	if len(databases) != len(refs) {
		return false
	}
	for _, database := range databases {
		if refs[database.Name] != database.BackupRef {
			return false
		}
	}
	return true
}

var _ OfflineRebuildWorkloadInputResolver = (*KubernetesOfflineRebuildWorkloadResolver)(nil)
