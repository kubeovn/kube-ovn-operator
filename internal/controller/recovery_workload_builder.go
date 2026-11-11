package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
	"k8s.io/apimachinery/pkg/types"
)

// OfflineRebuildWorkloadInputs is the explicit, already-resolved input set
// required by a production builder. A resolver must obtain these identities
// from live Kubernetes objects and immutable ConfigMaps; backup file or S3
// references cannot be converted into a PVC by inference.
type OfflineRebuildWorkloadInputs struct {
	AdmissionPlanDigest string
	CanonicalPlan       recovery.Plan

	MemberName         string
	NodeName           string
	Image              string
	ServiceAccountName string

	PlanConfigMapName        string
	PlanConfigMapUID         types.UID
	PlanConfigMapData        string
	PlanConfigMapDataDigest  string
	ProofConfigMapName       string
	ProofConfigMapUID        types.UID
	ProofConfigMapData       string
	ProofConfigMapDataDigest string

	Backup   recovery.WorkloadVolume
	Target   recovery.WorkloadVolume
	Retained []recovery.WorkloadVolume

	TargetStorage   recovery.StorageIdentity
	RetainedStorage map[string]recovery.StorageIdentity
}

// OfflineRebuildWorkloadInputResolver resolves the live Kubernetes snapshot
// required by a rebuild. Implementations must return ErrRecoveryWorkloadNotReady
// when an artifact is not ready and must never synthesize missing identities.
type OfflineRebuildWorkloadInputResolver interface {
	Resolve(context.Context, RecoveryPlan) (OfflineRebuildWorkloadInputs, error)
}

// ResolvingOfflineRebuildWorkloadBuilder adapts a live input resolver to the
// validated static builder. It is a seam for a future Kubernetes resolver and
// is intentionally not installed into the default controller.
type ResolvingOfflineRebuildWorkloadBuilder struct {
	Resolver OfflineRebuildWorkloadInputResolver
}

func (b *ResolvingOfflineRebuildWorkloadBuilder) Build(ctx context.Context, plan RecoveryPlan) (recovery.WorkloadSpec, error) {
	if b == nil || b.Resolver == nil {
		return recovery.WorkloadSpec{}, errors.New("offline rebuild workload input resolver is not configured")
	}
	inputs, err := b.Resolver.Resolve(ctx, plan)
	if err != nil {
		return recovery.WorkloadSpec{}, err
	}
	static, err := NewStaticOfflineRebuildWorkloadBuilder(inputs)
	if err != nil {
		return recovery.WorkloadSpec{}, fmt.Errorf("validate resolved offline rebuild workload inputs: %w", err)
	}
	return static.Build(ctx, plan)
}

// StaticOfflineRebuildWorkloadBuilder validates a complete production input
// snapshot and renders it into the executor's immutable WorkloadSpec. It is
// intentionally not installed into the default controller: resolving the
// snapshot from live workloads remains an explicit integration step.
type StaticOfflineRebuildWorkloadBuilder struct {
	inputs          OfflineRebuildWorkloadInputs
	canonicalPlan   recovery.Plan
	canonicalDigest string
}

// NewStaticOfflineRebuildWorkloadBuilder validates and captures one input
// snapshot. The returned builder never fills missing identities with defaults.
func NewStaticOfflineRebuildWorkloadBuilder(inputs OfflineRebuildWorkloadInputs) (*StaticOfflineRebuildWorkloadBuilder, error) {
	canonical, canonicalDigest, err := inputs.CanonicalPlan.Canonical()
	if err != nil {
		return nil, fmt.Errorf("validate canonical offline recovery plan: %w", err)
	}
	if !validRecoverySHA256(inputs.AdmissionPlanDigest) {
		return nil, errors.New("offline rebuild admission plan digest must be a SHA-256 value")
	}
	for field, value := range map[string]string{
		"member name": inputs.MemberName, "node name": inputs.NodeName, "image": inputs.Image,
		"service account": inputs.ServiceAccountName, "plan ConfigMap": inputs.PlanConfigMapName,
		"proof ConfigMap": inputs.ProofConfigMapName,
	} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("offline rebuild input requires %s", field)
		}
	}
	if inputs.PlanConfigMapDataDigest != canonicalDigest {
		return nil, errors.New("offline rebuild plan ConfigMap data digest does not match the canonical plan")
	}
	if strings.TrimSpace(inputs.PlanConfigMapData) == "" || recovery.ConfigMapDataDigest(inputs.PlanConfigMapData) != inputs.PlanConfigMapDataDigest {
		return nil, errors.New("offline rebuild plan ConfigMap content does not match its digest")
	}
	if strings.TrimSpace(inputs.ProofConfigMapData) == "" || recovery.ConfigMapDataDigest(inputs.ProofConfigMapData) != inputs.ProofConfigMapDataDigest {
		return nil, errors.New("offline rebuild proof ConfigMap content does not match its digest")
	}
	for field, value := range map[string]types.UID{
		"plan ConfigMap UID": inputs.PlanConfigMapUID, "proof ConfigMap UID": inputs.ProofConfigMapUID,
		"backup PVC UID": inputs.Backup.ClaimUID, "backup PV UID": inputs.Backup.PVUID,
		"target PVC UID": inputs.Target.ClaimUID, "target PV UID": inputs.Target.PVUID,
	} {
		if value == "" {
			return nil, fmt.Errorf("offline rebuild input requires %s", field)
		}
	}
	for field, value := range map[string]string{
		"plan ConfigMap data digest":  inputs.PlanConfigMapDataDigest,
		"proof ConfigMap data digest": inputs.ProofConfigMapDataDigest,
	} {
		if !validRecoverySHA256(value) {
			return nil, fmt.Errorf("offline rebuild input %s must be a SHA-256 value", field)
		}
	}
	if inputs.Backup.Name != "backup" || inputs.Target.Name != "target" || inputs.Target.ReadOnly || inputs.Backup.ReadOnly {
		return nil, errors.New("offline rebuild backup and target volumes have invalid names or access modes")
	}
	if err := validateCanonicalMemberInputs(canonical, inputs); err != nil {
		return nil, err
	}
	return &StaticOfflineRebuildWorkloadBuilder{inputs: cloneOfflineRebuildWorkloadInputs(inputs), canonicalPlan: canonical, canonicalDigest: canonicalDigest}, nil
}

func (b *StaticOfflineRebuildWorkloadBuilder) Build(ctx context.Context, plan RecoveryPlan) (recovery.WorkloadSpec, error) {
	if b == nil {
		return recovery.WorkloadSpec{}, errors.New("offline rebuild workload builder is nil")
	}
	if err := ctx.Err(); err != nil {
		return recovery.WorkloadSpec{}, err
	}
	if plan.Digest != b.inputs.AdmissionPlanDigest || plan.InstallationUID != types.UID(b.canonicalPlan.InstallationUID) {
		return recovery.WorkloadSpec{}, errors.New("offline rebuild admission plan identity does not match the resolved workload inputs")
	}
	if plan.Mode == operatorv1alpha1.RecoveryModeRejoin {
		return recovery.WorkloadSpec{}, errors.New("offline rebuild workload builder does not handle rejoin recovery")
	}
	return recovery.WorkloadSpec{
		Namespace: plan.RequestNamespace, Name: recovery.DeterministicRebuildJobName(plan.Digest),
		InstallationUID: string(plan.InstallationUID), OperationID: b.canonicalPlan.OperationID,
		PlanDigest: plan.Digest, CanonicalPlanDigest: b.canonicalDigest,
		MemberName: b.inputs.MemberName, NodeName: b.inputs.NodeName, Image: b.inputs.Image, ServiceAccountName: b.inputs.ServiceAccountName,
		PlanConfigMapName: b.inputs.PlanConfigMapName, PlanConfigMapUID: b.inputs.PlanConfigMapUID, PlanConfigMapDataDigest: b.inputs.PlanConfigMapDataDigest,
		IsolationProofConfigMap: b.inputs.ProofConfigMapName, IsolationProofConfigMapUID: b.inputs.ProofConfigMapUID, IsolationProofDataDigest: b.inputs.ProofConfigMapDataDigest,
		BackupClaim: b.inputs.Backup.Claim, BackupClaimUID: b.inputs.Backup.ClaimUID, BackupPVUID: b.inputs.Backup.PVUID,
		Target: b.inputs.Target, Retained: append([]recovery.WorkloadVolume(nil), b.inputs.Retained...), TargetStorage: b.inputs.TargetStorage, RetainedStorage: maps.Clone(b.inputs.RetainedStorage),
	}, nil
}

func validateCanonicalMemberInputs(plan recovery.Plan, inputs OfflineRebuildWorkloadInputs) error {
	var target *recovery.Member
	for index := range plan.TargetMembers {
		member := &plan.TargetMembers[index]
		if member.Name == inputs.MemberName {
			target = member
			break
		}
	}
	if target == nil || target.NodeName != inputs.NodeName || target.Storage != inputs.TargetStorage {
		return errors.New("offline rebuild target member identity does not match the canonical plan")
	}
	if len(inputs.Retained) != len(plan.RetainedMembers) || len(inputs.RetainedStorage) != len(plan.RetainedMembers) {
		return errors.New("offline rebuild retained member identities do not match the canonical plan")
	}
	for _, member := range plan.RetainedMembers {
		storage, ok := inputs.RetainedStorage[member.Name]
		if !ok || storage != member.Storage {
			return fmt.Errorf("offline rebuild retained member %q identity does not match the canonical plan", member.Name)
		}
		found := false
		for _, volume := range inputs.Retained {
			if volume.Name == member.Name {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("offline rebuild retained member %q has no PVC input", member.Name)
		}
	}
	return nil
}

func cloneOfflineRebuildWorkloadInputs(inputs OfflineRebuildWorkloadInputs) OfflineRebuildWorkloadInputs {
	inputs.CanonicalPlan.RetainedMembers = append([]recovery.Member(nil), inputs.CanonicalPlan.RetainedMembers...)
	inputs.CanonicalPlan.TargetMembers = append([]recovery.Member(nil), inputs.CanonicalPlan.TargetMembers...)
	inputs.CanonicalPlan.Databases = append([]recovery.Database(nil), inputs.CanonicalPlan.Databases...)
	inputs.Retained = append([]recovery.WorkloadVolume(nil), inputs.Retained...)
	inputs.RetainedStorage = maps.Clone(inputs.RetainedStorage)
	return inputs
}

func validRecoverySHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

var _ OfflineRebuildWorkloadBuilder = (*StaticOfflineRebuildWorkloadBuilder)(nil)
var _ OfflineRebuildWorkloadBuilder = (*ResolvingOfflineRebuildWorkloadBuilder)(nil)
