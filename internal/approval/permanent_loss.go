// Package approval verifies explicit, durable decisions without deriving them
// from Pod health, node deletion or a caller-supplied approver name.
package approval

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	Reserved  = "Reserved"
	Completed = "Completed"
)

// Provider requires an uncached Kubernetes client. Approval creation/update
// permission belongs to human approvers, while the operator may only read spec
// and update status. The Kubernetes API audit trail identifies the creator.
// These permissions must be installed deliberately before using this adapter.
type Provider struct {
	kube         client.Client
	namespace    string
	installation operatorv1alpha1.InstallationReference
}

var _ ovnadmin.LossVerifier = (*Provider)(nil)

func New(kube client.Client, installation *operatorv1alpha1.KubeOVNInstallation) (*Provider, error) {
	if kube == nil || installation == nil || installation.Name == "" || installation.Namespace == "" || installation.UID == "" {
		return nil, errors.New("approval provider requires a client and installation identity")
	}
	return &Provider{kube: kube, namespace: installation.Namespace, installation: operatorv1alpha1.InstallationReference{Name: installation.Name, UID: installation.UID}}, nil
}

// Reserve records the operation and immutable decision digest before a kick.
// It is retryable for that same operation, but never recycles a completed record.
func (p *Provider) Reserve(ctx context.Context, ref membership.ApprovalReference, operationID string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	record, digest, err := p.read(ctx, ref, operationID, true)
	if err != nil {
		return err
	}
	if record.Status.Phase != "" {
		return reserved(record, digest)
	}
	status := record.Status
	if status.OperationID != "" || status.DecisionDigest != "" || status.ReservedAt != nil || status.CompletedAt != nil || len(status.Observations) != 0 {
		return errors.New("approval contains an incomplete reservation")
	}
	record.Status = operatorv1alpha1.MemberRemovalApprovalStatus{Phase: Reserved, OperationID: operationID, DecisionDigest: digest, ReservedAt: new(metav1.Now())}
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return err
	}
	if err := p.kube.Status().Update(ctx, record); err != nil {
		return fmt.Errorf("reserve approval: %w", err)
	}
	current, currentDigest, err := p.read(ctx, ref, operationID, true)
	if err != nil {
		return err
	}
	return reserved(current, currentDigest)
}

func (p *Provider) VerifyPermanentLoss(ctx context.Context, snapshot ovnadmin.Snapshot, change membership.Change) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if change.Operation != membership.OperationKick || !change.PermanentLossApproved || snapshot.ClusterID != change.ClusterID {
		return time.Time{}, errors.New("approval can only authorize an explicitly requested lost-member kick")
	}
	if err := snapshot.Validate(); err != nil {
		return time.Time{}, err
	}
	record, digest, err := p.read(ctx, change.ApprovalRef, change.OperationID, true)
	if err != nil {
		return time.Time{}, err
	}
	if err := reserved(record, digest); err != nil {
		return time.Time{}, err
	}
	approved := false
	for _, target := range record.Spec.Decision.Targets {
		if target.Database == snapshot.Database && target.ClusterID == snapshot.ClusterID && target.ServerID == change.Member.ServerID && target.Address == change.Member.Address {
			approved = true
		}
	}
	if !approved {
		return time.Time{}, errors.New("approval does not name this exact database, server UUID and address")
	}
	for _, member := range snapshot.Members {
		if member.ServerID == change.Member.ServerID && member.Address == change.Member.Address && !member.Healthy && !member.Leader {
			if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
				return time.Time{}, err
			}
			if !time.Now().Before(record.Spec.Decision.ExpiresAt.Time) {
				return time.Time{}, errors.New("approval expired during verification")
			}
			return record.Spec.Decision.ExpiresAt.Time, nil
		}
	}
	return time.Time{}, errors.New("approved member is no longer an unhealthy non-leader in the observed configuration")
}

// Complete records fresh observations supplied by the lifecycle executor under
// the same operation Leases. Every approved database must have quorum and have
// removed its exact old SID. Expiry/revocation block dispatch, not recording a
// removal that has already converged. Completed audit status is never rewritten.
func (p *Provider) Complete(ctx context.Context, ref membership.ApprovalReference, operationID string, observations []ovnadmin.Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	record, digest, err := p.read(ctx, ref, operationID, false)
	if err != nil {
		return err
	}
	if record.Status.Phase == Completed {
		return completed(record, digest)
	}
	if err := reserved(record, digest); err != nil {
		return err
	}
	targets := record.Spec.Decision.Targets
	if len(observations) != len(targets) {
		return errors.New("completion requires observations for every approved database")
	}
	now := metav1.Now()
	seen := map[string]bool{}
	var evidence []operatorv1alpha1.MemberRemovalObservation
	for _, observation := range observations {
		if seen[observation.Database] {
			return errors.New("duplicate database completion observation")
		}
		seen[observation.Database] = true
		if err := observation.Validate(); err != nil {
			return err
		}
		index := slices.IndexFunc(targets, func(t operatorv1alpha1.MemberRemovalTarget) bool {
			return t.Database == observation.Database && t.ClusterID == observation.ClusterID
		})
		if index < 0 {
			return errors.New("completion observation is for a different database identity")
		}
		if slices.ContainsFunc(observation.Members, func(m membership.Member) bool { return m.ServerID == targets[index].ServerID }) {
			return errors.New("approved member is still present; removal has not converged")
		}
		evidence = append(evidence, operatorv1alpha1.MemberRemovalObservation{Database: observation.Database, ClusterID: observation.ClusterID, Term: observation.Term, CommitIndex: observation.CommitIndex, ObservedAt: now})
	}
	slices.SortFunc(evidence, func(a, b operatorv1alpha1.MemberRemovalObservation) int { return cmp.Compare(a.Database, b.Database) })
	record.Status.Phase, record.Status.CompletedAt, record.Status.Observations = Completed, &now, evidence
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return err
	}
	if err := p.kube.Status().Update(ctx, record); err != nil {
		return fmt.Errorf("complete approval: %w", err)
	}
	current, currentDigest, err := p.read(ctx, ref, operationID, false)
	if err != nil {
		return err
	}
	return completed(current, currentDigest)
}

func (p *Provider) read(ctx context.Context, ref membership.ApprovalReference, operationID string, active bool) (*operatorv1alpha1.KubeOVNMemberRemovalApproval, string, error) {
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return nil, "", err
	}
	if ref.Name == "" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || ref.UID == "" || !membership.ValidID(operationID) {
		return nil, "", errors.New("approval requires a pinned name, UID and full operation UUID")
	}
	record := new(operatorv1alpha1.KubeOVNMemberRemovalApproval)
	if err := p.kube.Get(ctx, types.NamespacedName{Namespace: p.namespace, Name: ref.Name}, record); err != nil {
		return nil, "", err
	}
	if string(record.UID) != ref.UID || record.DeletionTimestamp != nil || record.Generation < 1 || record.CreationTimestamp.IsZero() || len(record.OwnerReferences) != 0 {
		return nil, "", errors.New("approval identity changed, is being deleted, or is subject to owner garbage collection")
	}
	decision := record.Spec.Decision
	if decision.InstallationRef != p.installation || decision.OperationID != operationID || strings.TrimSpace(decision.Reason) == "" || len(decision.Reason) > 2048 || decision.ExpiresAt.IsZero() || !decision.ExpiresAt.After(record.CreationTimestamp.Time) {
		return nil, "", errors.New("approval does not bind a valid decision to this installation and operation")
	}
	if active && (record.Spec.Revoked || !time.Now().Before(decision.ExpiresAt.Time)) {
		return nil, "", errors.New("approval is revoked or expired")
	}
	if len(decision.Targets) < 1 || len(decision.Targets) > 2 {
		return nil, "", errors.New("approval must identify one or both OVN databases")
	}
	seen := map[string]bool{}
	for _, target := range decision.Targets {
		if (target.Database != "OVN_Northbound" && target.Database != "OVN_Southbound") || seen[target.Database] || !membership.ValidID(target.ClusterID) {
			return nil, "", errors.New("approval has an invalid or duplicate database identity")
		}
		seen[target.Database] = true
		if err := membership.ValidateMembers([]membership.Member{{ServerID: target.ServerID, Address: target.Address}}); err != nil {
			return nil, "", fmt.Errorf("approval target: %w", err)
		}
	}
	decision.Targets = slices.Clone(decision.Targets)
	slices.SortFunc(decision.Targets, func(a, b operatorv1alpha1.MemberRemovalTarget) int { return cmp.Compare(a.Database, b.Database) })
	content, err := json.Marshal(decision)
	if err != nil {
		return nil, "", err
	}
	return record, fmt.Sprintf("sha256:%x", sha256.Sum256(content)), nil
}

func reservationMatches(record *operatorv1alpha1.KubeOVNMemberRemovalApproval, digest string) bool {
	status := record.Status
	return status.OperationID == record.Spec.Decision.OperationID && status.DecisionDigest == digest && status.ReservedAt != nil && !status.ReservedAt.IsZero() && !status.ReservedAt.After(time.Now()) && !status.ReservedAt.Before(&record.CreationTimestamp) && status.ReservedAt.Before(&record.Spec.Decision.ExpiresAt)
}

func reserved(record *operatorv1alpha1.KubeOVNMemberRemovalApproval, digest string) error {
	if record.Status.Phase != Reserved || !reservationMatches(record, digest) || record.Status.CompletedAt != nil || len(record.Status.Observations) != 0 {
		return errors.New("approval is not reserved for this immutable operation or has already been consumed")
	}
	return nil
}

func completed(record *operatorv1alpha1.KubeOVNMemberRemovalApproval, digest string) error {
	status := record.Status
	if status.Phase != Completed || !reservationMatches(record, digest) || status.CompletedAt == nil || status.CompletedAt.IsZero() || status.CompletedAt.Before(status.ReservedAt) || status.CompletedAt.After(time.Now()) || len(status.Observations) != len(record.Spec.Decision.Targets) {
		return errors.New("approval completion audit is incomplete or belongs to another operation")
	}
	seen := map[string]bool{}
	for _, evidence := range status.Observations {
		if seen[evidence.Database] || evidence.Term < 1 || evidence.CommitIndex < 1 || !evidence.ObservedAt.Equal(status.CompletedAt) || !slices.ContainsFunc(record.Spec.Decision.Targets, func(target operatorv1alpha1.MemberRemovalTarget) bool {
			return target.Database == evidence.Database && target.ClusterID == evidence.ClusterID
		}) {
			return errors.New("approval completion audit has invalid database evidence")
		}
		seen[evidence.Database] = true
	}
	return nil
}
