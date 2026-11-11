package controller

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const recoveryArtifactMaxBytes = 64 << 10

// OfflineRebuildArtifactInputs is the explicit, already-authorized input to
// the pure artifact renderer. It does not read or mutate Kubernetes objects.
// Backup PVC binding remains a separate live check performed by the resolver.
type OfflineRebuildArtifactInputs struct {
	Plan                   RecoveryPlan
	CanonicalPlan          recovery.Plan
	IsolationProof         recovery.IsolationProof
	ProofObservedAt        time.Time
	PlanConfigMap          string
	ProofConfigMap         string
	BackupPVCName          string
	BackupPVCNamespace     string
	Owner                  metav1.OwnerReference
	OperationID            string
	ApprovalDecisionDigest string
	CanonicalPlanDigest    string
}

// OfflineRebuildArtifacts contains immutable ConfigMaps and the explicit
// backup PVC binding contract required by KubernetesOfflineRebuildWorkloadResolver.
type OfflineRebuildArtifacts struct {
	PlanConfigMap       *corev1.ConfigMap
	ProofConfigMap      *corev1.ConfigMap
	CanonicalPlanDigest string
	PlanDataDigest      string
	ProofDataDigest     string
	BackupPVCNamespace  string
	BackupPVCName       string
	BackupPairID        string
}

// RenderOfflineRebuildArtifacts validates all cross-object bindings and
// renders immutable plan/proof ConfigMaps. The caller decides if and when to
// create them; this function never performs an API write.
func RenderOfflineRebuildArtifacts(inputs OfflineRebuildArtifactInputs) (OfflineRebuildArtifacts, error) {
	if err := validateArtifactInputs(inputs); err != nil {
		return OfflineRebuildArtifacts{}, err
	}
	canonical, canonicalDigest, err := inputs.CanonicalPlan.Canonical()
	if err != nil {
		return OfflineRebuildArtifacts{}, fmt.Errorf("validate canonical recovery plan: %w", err)
	}
	if err := validateCanonicalArtifactBinding(inputs.Plan, canonical, canonicalDigest); err != nil {
		return OfflineRebuildArtifacts{}, err
	}
	if !validRecoverySHA256(inputs.CanonicalPlanDigest) || inputs.CanonicalPlanDigest != canonicalDigest {
		return OfflineRebuildArtifacts{}, errors.New("recovery artifact canonical plan digest does not match the supplied digest")
	}
	if !validRecoverySHA256(inputs.ApprovalDecisionDigest) || inputs.ApprovalDecisionDigest != canonical.Approval.DecisionDigest {
		return OfflineRebuildArtifacts{}, errors.New("recovery artifact approval decision digest does not match the canonical plan")
	}
	if inputs.ProofObservedAt.IsZero() {
		return OfflineRebuildArtifacts{}, errors.New("recovery artifacts require an explicit proof observation time")
	}
	if err := inputs.IsolationProof.ValidateAt(canonical, inputs.ProofObservedAt); err != nil {
		return OfflineRebuildArtifacts{}, fmt.Errorf("validate recovery isolation proof: %w", err)
	}
	planData, err := json.Marshal(canonical)
	if err != nil {
		return OfflineRebuildArtifacts{}, fmt.Errorf("marshal canonical recovery plan: %w", err)
	}
	proofData, err := json.Marshal(inputs.IsolationProof)
	if err != nil {
		return OfflineRebuildArtifacts{}, fmt.Errorf("marshal recovery isolation proof: %w", err)
	}
	if len(planData) > recoveryArtifactMaxBytes || len(proofData) > recoveryArtifactMaxBytes {
		return OfflineRebuildArtifacts{}, errors.New("recovery artifact ConfigMap data exceeds the size limit")
	}
	planDataString, proofDataString := string(planData), string(proofData)
	planDataDigest := recovery.ConfigMapDataDigest(planDataString)
	proofDataDigest := recovery.ConfigMapDataDigest(proofDataString)
	annotations := artifactAnnotations(inputs.Plan, canonical, canonicalDigest)
	return OfflineRebuildArtifacts{
		PlanConfigMap:       &corev1.ConfigMap{ObjectMeta: artifactMetadata(inputs.PlanConfigMap, inputs.Plan.RequestNamespace, annotations, inputs.Owner), Immutable: new(true), Data: map[string]string{"plan.json": planDataString}},
		ProofConfigMap:      &corev1.ConfigMap{ObjectMeta: artifactMetadata(inputs.ProofConfigMap, inputs.Plan.RequestNamespace, annotations, inputs.Owner), Immutable: new(true), Data: map[string]string{"proof.json": proofDataString}},
		CanonicalPlanDigest: canonicalDigest,
		PlanDataDigest:      planDataDigest,
		ProofDataDigest:     proofDataDigest,
		BackupPVCNamespace:  inputs.BackupPVCNamespace,
		BackupPVCName:       inputs.BackupPVCName,
		BackupPairID:        canonical.BackupPairID,
	}, nil
}

func validateArtifactInputs(inputs OfflineRebuildArtifactInputs) error {
	plan := inputs.Plan
	if plan.Mode != operatorv1alpha1.RecoveryModeReconvert && plan.Mode != operatorv1alpha1.RecoveryModeFreshBootstrap {
		return errors.New("recovery artifacts require reconvert or freshBootstrap mode")
	}
	if strings.TrimSpace(plan.BackupRef) == "" {
		return errors.New("recovery artifacts require a backup pair")
	}
	if strings.TrimSpace(plan.RequestNamespace) == "" || plan.RequestNamespace != plan.InstallationNamespace || plan.InstallationUID == "" || plan.RequestUID == "" || plan.RequestName == "" || plan.Digest == "" {
		return errors.New("recovery artifacts require a complete request and installation identity")
	}
	if strings.TrimSpace(inputs.BackupPVCNamespace) == "" || strings.TrimSpace(inputs.BackupPVCName) == "" || inputs.BackupPVCNamespace != plan.RequestNamespace {
		return errors.New("recovery artifacts require an explicit backup PVC in the request namespace")
	}
	if len(validation.IsDNS1123Subdomain(inputs.BackupPVCName)) != 0 {
		return errors.New("recovery artifact backup PVC name is invalid")
	}
	if strings.TrimSpace(inputs.OperationID) == "" || !membership.ValidID(inputs.OperationID) || inputs.OperationID != plan.OperationID {
		return errors.New("recovery artifacts require a valid operation ID")
	}
	if inputs.Owner.APIVersion != operatorv1alpha1.GroupVersion.String() || inputs.Owner.Kind != "KubeOVNRecoveryRequest" || inputs.Owner.Name != plan.RequestName || inputs.Owner.UID != plan.RequestUID || inputs.Owner.Controller == nil || !*inputs.Owner.Controller || inputs.Owner.BlockOwnerDeletion == nil || !*inputs.Owner.BlockOwnerDeletion {
		return errors.New("recovery artifact owner must be the exact RecoveryRequest controller")
	}
	for field, name := range map[string]string{"plan ConfigMap": inputs.PlanConfigMap, "proof ConfigMap": inputs.ProofConfigMap} {
		if strings.TrimSpace(name) == "" || len(validation.IsDNS1123Subdomain(name)) != 0 {
			return fmt.Errorf("recovery artifact %s name is invalid", field)
		}
	}
	if inputs.PlanConfigMap == inputs.ProofConfigMap {
		return errors.New("recovery artifact ConfigMap names must be distinct")
	}
	if inputs.CanonicalPlan.OperationID != inputs.OperationID {
		return errors.New("recovery artifact operation ID does not match the canonical plan")
	}
	if inputs.CanonicalPlan.TopologyDigest != inputs.Plan.TopologyDigest {
		return errors.New("recovery artifact topology digest does not match the admitted plan")
	}
	if !validRecoverySHA256(inputs.CanonicalPlan.Approval.DecisionDigest) {
		return errors.New("recovery artifact approval decision digest is invalid")
	}
	if inputs.Plan.SourceVersion == "" || inputs.Plan.TargetVersion == "" || inputs.Plan.ApprovalRef == nil {
		return errors.New("recovery artifacts require source, target and approval bindings")
	}
	return nil
}

func validateCanonicalArtifactBinding(plan RecoveryPlan, canonical recovery.Plan, canonicalDigest string) error {
	if !validRecoverySHA256(plan.Digest) || canonicalDigest == "" || canonical.InstallationUID != string(plan.InstallationUID) || canonical.SourceVersion != plan.SourceVersion || canonical.TargetVersion != plan.TargetVersion || canonical.TopologyDigest != plan.TopologyDigest || canonical.BackupPairID != plan.BackupRef || !databaseRefsMatch(canonical.Databases, plan.DatabaseRefs) {
		return errors.New("canonical recovery plan is not bound to the admitted plan")
	}
	if plan.ApprovalRef == nil || canonical.Approval.Name != plan.ApprovalRef.Name || types.UID(canonical.Approval.UID) != plan.ApprovalRef.UID {
		return errors.New("canonical recovery approval is not bound to the admitted plan")
	}
	return nil
}

func artifactAnnotations(plan RecoveryPlan, canonical recovery.Plan, canonicalDigest string) map[string]string {
	return map[string]string{
		recovery.RebuildPlanDigestAnnotation:          plan.Digest,
		recovery.RebuildCanonicalPlanDigestAnnotation: canonicalDigest,
		recovery.RebuildOperationAnnotation:           canonical.OperationID,
		recovery.RebuildInstallationAnnotation:        string(plan.InstallationUID),
	}
}

func artifactMetadata(name, namespace string, annotations map[string]string, owner metav1.OwnerReference) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: annotations, OwnerReferences: []metav1.OwnerReference{*owner.DeepCopy()}}
}
