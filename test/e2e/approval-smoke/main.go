// This smoke test uses the existing kind job's admin access. It does not create
// credentials or bind approval permissions to the operator or another actor.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	authorizationv1 "k8s.io/api/authorization/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Approval admission, reservation, revocation, completion and RBAC checks passed")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, coordinationv1.AddToScheme, authorizationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			return err
		}
	}
	config := ctrl.GetConfigOrDie()
	config.Timeout = 5 * time.Second
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	installation := new(api.KubeOVNInstallation)
	if err := kube.Get(ctx, client.ObjectKey{Namespace: "kube-ovn-operator-system", Name: "kube-ovn"}, installation); err != nil {
		return err
	}
	provider, err := approval.New(kube, installation)
	if err != nil {
		return err
	}
	snapshot := ovnadmin.Snapshot{
		Database: "OVN_Northbound", ClusterID: uuid.New().String(), Term: 7, CommitIndex: 20, AppliedIndex: 20,
		Members: []membership.Member{
			{ServerID: uuid.New().String(), Address: "tcp:192.0.2.1:6643", Healthy: true, Leader: true},
			{ServerID: uuid.New().String(), Address: "tcp:192.0.2.2:6643", Healthy: true},
			{ServerID: uuid.New().String(), Address: "tcp:192.0.2.3:6643"},
		},
	}
	snapshot.LeaderServerID = snapshot.Members[0].ServerID
	decision := api.MemberRemovalDecision{
		InstallationRef: api.InstallationReference{Name: installation.Name, UID: installation.UID}, OperationID: uuid.New().String(),
		Reason: "Synthetic smoke-test lost member", ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour)),
		Targets: []api.MemberRemovalTarget{
			{Database: snapshot.Database, ClusterID: snapshot.ClusterID, ServerID: snapshot.Members[2].ServerID, Address: snapshot.Members[2].Address},
			{Database: "OVN_Southbound", ClusterID: snapshot.ClusterID, ServerID: snapshot.Members[2].ServerID, Address: "tcp:192.0.2.3:6644"},
		},
	}
	record := &api.KubeOVNMemberRemovalApproval{Name: "smoke-" + uuid.New().String(), Namespace: installation.Namespace, Spec: api.MemberRemovalApprovalSpec{Decision: decision}}
	if err := kube.Create(ctx, record); err != nil {
		return err
	}
	key := client.ObjectKeyFromObject(record)
	read := func() (*api.KubeOVNMemberRemovalApproval, error) {
		current := new(api.KubeOVNMemberRemovalApproval)
		return current, kube.Get(ctx, key, current)
	}
	for name, mutate := range map[string]func(*api.KubeOVNMemberRemovalApproval){
		"operation":        func(r *api.KubeOVNMemberRemovalApproval) { r.Spec.Decision.OperationID = uuid.New().String() },
		"installation UID": func(r *api.KubeOVNMemberRemovalApproval) { r.Spec.Decision.InstallationRef.UID = "replacement" },
		"target":           func(r *api.KubeOVNMemberRemovalApproval) { r.Spec.Decision.Targets[0].ServerID = uuid.New().String() },
		"expiry": func(r *api.KubeOVNMemberRemovalApproval) {
			r.Spec.Decision.ExpiresAt = metav1.NewTime(time.Now().Add(2 * time.Hour))
		},
		"reason": func(r *api.KubeOVNMemberRemovalApproval) { r.Spec.Decision.Reason = "changed" },
	} {
		attempt := record.DeepCopy()
		mutate(attempt)
		if err := kube.Update(ctx, attempt); !apierrors.IsInvalid(err) {
			return fmt.Errorf("decision %s was not immutable: %v", name, err)
		}
	}
	duplicate := record.DeepCopy()
	duplicate.Name, duplicate.UID, duplicate.ResourceVersion = "duplicate-"+uuid.New().String(), "", ""
	duplicate.Spec.Decision.Targets[1] = duplicate.Spec.Decision.Targets[0]
	if err := kube.Create(ctx, duplicate); !apierrors.IsInvalid(err) {
		return fmt.Errorf("duplicate target admitted: %v", err)
	}
	ref := membership.ApprovalReference{Name: record.Name, UID: string(record.UID)}
	// A fully shaped status still cannot skip the reservation transition.
	skipped := record.DeepCopy()
	now := metav1.Now()
	skipped.Status = api.MemberRemovalApprovalStatus{
		Phase: approval.Completed, OperationID: decision.OperationID,
		DecisionDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ReservedAt:     &now, CompletedAt: &now,
	}
	for _, target := range decision.Targets {
		skipped.Status.Observations = append(skipped.Status.Observations, api.MemberRemovalObservation{Database: target.Database, ClusterID: target.ClusterID, Term: 7, CommitIndex: 20, ObservedAt: now})
	}
	if err := kube.Status().Update(ctx, skipped); !apierrors.IsInvalid(err) {
		return fmt.Errorf("completion skipped reservation: %v", err)
	}
	change := membership.Change{Operation: membership.OperationKick, OperationID: decision.OperationID, ApprovalRef: ref, ClusterID: snapshot.ClusterID, Member: snapshot.Members[2], PermanentLossApproved: true}
	manager := lease.New(kube)
	err = manager.With(ctx, installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			if err := provider.Reserve(ctx, ref, decision.OperationID); err != nil {
				return err
			}
			if _, err := provider.VerifyPermanentLoss(ctx, snapshot, change); err != nil {
				return err
			}
			reserved, err := read()
			if err != nil {
				return err
			}
			if err := provider.Reserve(ctx, ref, decision.OperationID); err != nil {
				return err
			}
			current, err := read()
			if err != nil {
				return err
			}
			if reserved.ResourceVersion != current.ResourceVersion {
				return errors.New("idempotent reservation changed resource version")
			}
			attempt := current.DeepCopy()
			attempt.Status.DecisionDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
				return fmt.Errorf("reservation changed: %v", err)
			}
			attempt = current.DeepCopy()
			attempt.Status = api.MemberRemovalApprovalStatus{}
			if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
				return fmt.Errorf("reservation cleared: %v", err)
			}
			// Revoke spec using a separate API write. The stale reservation cannot
			// overwrite this decision through the status subresource.
			current.Spec.Revoked = true
			if err := kube.Update(ctx, current); err != nil {
				return err
			}
			if err := kube.Status().Update(ctx, reserved); !apierrors.IsConflict(err) {
				return fmt.Errorf("stale status update did not conflict: %v", err)
			}
			if _, err := provider.VerifyPermanentLoss(ctx, snapshot, change); err == nil {
				return errors.New("revoked approval authorized kick")
			}
			current, err = read()
			if err != nil {
				return err
			}
			attempt = current.DeepCopy()
			attempt.Spec.Revoked = false
			if err := kube.Update(ctx, attempt); !apierrors.IsInvalid(err) {
				return fmt.Errorf("revocation was reversed: %v", err)
			}
			// Synthetic observations test the API/provider contract, not live OVN.
			nb := snapshot
			nb.Members = nb.Members[:2]
			sb := nb
			sb.Database = "OVN_Southbound"
			if err := provider.Complete(ctx, ref, decision.OperationID, []ovnadmin.Snapshot{nb}); err == nil {
				return errors.New("completion omitted SB")
			}
			if err := provider.Complete(ctx, ref, decision.OperationID, []ovnadmin.Snapshot{nb, sb}); err != nil {
				return err
			}
			completed, err := read()
			if err != nil {
				return err
			}
			if err := provider.Complete(ctx, ref, decision.OperationID, nil); err != nil {
				return err
			}
			current, err = read()
			if err != nil {
				return err
			}
			if completed.ResourceVersion != current.ResourceVersion {
				return errors.New("completed audit was rewritten")
			}
			attempt = current.DeepCopy()
			attempt.Status.Observations[0].CommitIndex++
			if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
				return fmt.Errorf("completion audit was mutable: %v", err)
			}
			if err := provider.Reserve(ctx, ref, decision.OperationID); err == nil {
				return errors.New("completed approval was recycled")
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, verb := range []string{"create", "update", "delete"} {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               "system:serviceaccount:kube-ovn-operator-system:kube-ovn-operator",
			Groups:             []string{"system:serviceaccounts", "system:serviceaccounts:kube-ovn-operator-system", "system:authenticated"},
			ResourceAttributes: &authorizationv1.ResourceAttributes{Namespace: installation.Namespace, Group: api.GroupVersion.Group, Resource: "kubeovnmemberremovalapprovals", Verb: verb},
		}}
		if err := kube.Create(ctx, review); err != nil {
			return err
		}
		if review.Status.Allowed || review.Status.EvaluationError != "" {
			return fmt.Errorf("operator approval %s permission was not denied: %+v", verb, review.Status)
		}
	}
	return nil
}
