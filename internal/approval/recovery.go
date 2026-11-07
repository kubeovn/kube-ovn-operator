package approval

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RecoveryBinding is the immutable target identity an approval must authorize.
// Request fields are empty for an installation-level upgrade rollback and are
// populated for a KubeOVNRecoveryRequest execution.
type RecoveryBinding struct {
	InstallationName       string
	InstallationUID        types.UID
	InstallationGeneration int64
	RequestName            string
	RequestUID             types.UID
	RequestGeneration      int64
	Mode                   string
	SourceVersion          string
	TargetVersion          string
	BackupRef              string
	TopologyDigest         string
	ChartDigest            string
}

// RecoveryProvider reserves and completes a human-created approval. The
// operator can update only status, and a completed approval cannot authorize a
// second destructive operation.
type RecoveryProvider struct {
	kube         client.Client
	namespace    string
	installation operatorv1alpha1.InstallationReference
}

type RecoveryApprover interface {
	Reserve(context.Context, operatorv1alpha1.RecoveryApprovalReference, RecoveryBinding) error
	Complete(context.Context, operatorv1alpha1.RecoveryApprovalReference, RecoveryBinding) error
}

var _ RecoveryApprover = (*RecoveryProvider)(nil)

func NewRecovery(kube client.Client, installation *operatorv1alpha1.KubeOVNInstallation) (*RecoveryProvider, error) {
	if kube == nil || installation == nil || installation.Name == "" || installation.Namespace == "" || installation.UID == "" {
		return nil, errors.New("recovery approval provider requires a client and installation identity")
	}
	return &RecoveryProvider{kube: kube, namespace: installation.Namespace, installation: operatorv1alpha1.InstallationReference{Name: installation.Name, UID: installation.UID}}, nil
}

func (p *RecoveryProvider) Reserve(ctx context.Context, ref operatorv1alpha1.RecoveryApprovalReference, binding RecoveryBinding) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	record, digest, err := p.read(ctx, ref, binding, true)
	if err != nil {
		return err
	}
	switch record.Status.Phase {
	case "":
		if record.Status.DecisionDigest != "" || record.Status.ReservedAt != nil || record.Status.CompletedAt != nil {
			return errors.New("recovery approval contains an incomplete reservation")
		}
		now := metav1.Now()
		record.Status = operatorv1alpha1.RecoveryApprovalStatus{Phase: operatorv1alpha1.RecoveryApprovalReserved, DecisionDigest: digest, ReservedAt: &now}
	case operatorv1alpha1.RecoveryApprovalReserved:
		if err := reservedRecovery(record, digest); err != nil {
			return err
		}
		return nil
	case operatorv1alpha1.RecoveryApprovalCompleted:
		return errors.New("recovery approval has already been completed")
	default:
		return errors.New("recovery approval has an unknown status phase")
	}
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return err
	}
	if err := p.kube.Status().Update(ctx, record); err != nil {
		return fmt.Errorf("reserve recovery approval: %w", err)
	}
	current, currentDigest, err := p.read(ctx, ref, binding, true)
	if err != nil {
		return err
	}
	return reservedRecovery(current, currentDigest)
}

func (p *RecoveryProvider) Complete(ctx context.Context, ref operatorv1alpha1.RecoveryApprovalReference, binding RecoveryBinding) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	record, digest, err := p.read(ctx, ref, binding, false)
	if err != nil {
		return err
	}
	if record.Status.Phase == operatorv1alpha1.RecoveryApprovalCompleted {
		return completedRecovery(record, digest)
	}
	if err := reservedRecovery(record, digest); err != nil {
		return err
	}
	now := metav1.Now()
	record.Status.Phase = operatorv1alpha1.RecoveryApprovalCompleted
	record.Status.CompletedAt = &now
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return err
	}
	if err := p.kube.Status().Update(ctx, record); err != nil {
		return fmt.Errorf("complete recovery approval: %w", err)
	}
	current, currentDigest, err := p.read(ctx, ref, binding, false)
	if err != nil {
		return err
	}
	return completedRecovery(current, currentDigest)
}

func (p *RecoveryProvider) read(ctx context.Context, ref operatorv1alpha1.RecoveryApprovalReference, binding RecoveryBinding, active bool) (*operatorv1alpha1.KubeOVNRecoveryApproval, string, error) {
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return nil, "", err
	}
	if p == nil || p.kube == nil {
		return nil, "", errors.New("recovery approval provider is not configured")
	}
	if ref.Name == "" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || ref.UID == "" {
		return nil, "", errors.New("recovery approval requires a pinned name and UID")
	}
	if binding.InstallationName == "" || binding.InstallationUID == "" || binding.InstallationGeneration < 1 || binding.Mode == "" || binding.SourceVersion == "" || binding.TargetVersion == "" || binding.BackupRef == "" || binding.TopologyDigest == "" || binding.ChartDigest == "" {
		return nil, "", errors.New("recovery approval binding is incomplete")
	}
	if !validRecoveryApprovalMode(binding.Mode) {
		return nil, "", fmt.Errorf("recovery approval mode %q is unsupported", binding.Mode)
	}
	record := new(operatorv1alpha1.KubeOVNRecoveryApproval)
	if err := p.kube.Get(ctx, types.NamespacedName{Namespace: p.namespace, Name: ref.Name}, record); err != nil {
		return nil, "", err
	}
	if types.UID(record.UID) != ref.UID || record.DeletionTimestamp != nil || record.Generation < 1 || record.CreationTimestamp.IsZero() || len(record.OwnerReferences) != 0 {
		return nil, "", errors.New("recovery approval identity changed, is being deleted, or is subject to owner garbage collection")
	}
	decision := record.Spec
	if !validRecoveryApprovalMode(decision.Mode) {
		return nil, "", fmt.Errorf("recovery approval mode %q is unsupported", decision.Mode)
	}
	if decision.InstallationRef != p.installation || decision.InstallationRef.Name != binding.InstallationName || decision.InstallationRef.UID != binding.InstallationUID || decision.InstallationGeneration != binding.InstallationGeneration || decision.Mode != binding.Mode || decision.SourceVersion != binding.SourceVersion || decision.TargetVersion != binding.TargetVersion || decision.BackupRef != binding.BackupRef || decision.TopologyDigest != binding.TopologyDigest || decision.ChartDigest != binding.ChartDigest || strings.TrimSpace(decision.Reason) == "" || decision.ExpiresAt.IsZero() || !decision.ExpiresAt.After(record.CreationTimestamp.Time) {
		return nil, "", errors.New("recovery approval does not bind the requested installation, version, backup and topology")
	}
	if decision.RequestRef == nil {
		if binding.RequestName != "" || binding.RequestUID != "" || binding.RequestGeneration != 0 {
			return nil, "", errors.New("recovery approval is not bound to the requested RecoveryRequest")
		}
	} else if binding.RequestName == "" || decision.RequestRef.Name != binding.RequestName || decision.RequestRef.UID != binding.RequestUID || decision.RequestRef.Generation != binding.RequestGeneration {
		return nil, "", errors.New("recovery approval request identity does not match")
	}
	if active && (decision.Revoked || !time.Now().Before(decision.ExpiresAt.Time)) {
		return nil, "", errors.New("recovery approval is revoked or expired")
	}
	digest, err := recoveryDecisionDigest(decision)
	if err != nil {
		return nil, "", err
	}
	return record, digest, nil
}

func validRecoveryApprovalMode(mode string) bool {
	switch mode {
	case "rollback", string(operatorv1alpha1.RecoveryModeReconvert), string(operatorv1alpha1.RecoveryModeFreshBootstrap):
		return true
	default:
		return false
	}
}

func recoveryDecisionDigest(decision operatorv1alpha1.RecoveryApprovalSpec) (string, error) {
	data, err := json.Marshal(decision, json.Deterministic(true))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

func reservedRecovery(record *operatorv1alpha1.KubeOVNRecoveryApproval, digest string) error {
	status := record.Status
	if status.Phase != operatorv1alpha1.RecoveryApprovalReserved || status.DecisionDigest != digest || status.ReservedAt == nil || status.ReservedAt.IsZero() || status.ReservedAt.Before(&record.CreationTimestamp) || status.CompletedAt != nil {
		return errors.New("recovery approval is not reserved for this immutable operation")
	}
	return nil
}

func completedRecovery(record *operatorv1alpha1.KubeOVNRecoveryApproval, digest string) error {
	if err := reservedRecovery(record, digest); err != nil && record.Status.Phase != operatorv1alpha1.RecoveryApprovalCompleted {
		return err
	}
	if record.Status.Phase != operatorv1alpha1.RecoveryApprovalCompleted || record.Status.CompletedAt == nil || record.Status.CompletedAt.IsZero() || record.Status.CompletedAt.Before(record.Status.ReservedAt) {
		return errors.New("recovery approval completion audit is incomplete")
	}
	return nil
}
