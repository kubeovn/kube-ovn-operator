package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// These API/provider checks use synthetic backup/topology digests and never
// restore a database. Request UID and generation come from real API objects.
func recoveryAPIRecord(ctx context.Context, kube client.Client, installation *api.KubeOVNInstallation, mode string) (*api.KubeOVNRecoveryApproval, approval.RecoveryBinding, error) {
	source := installation.Status.ObservedVersion
	if source == "" {
		source = installation.Spec.Version
	}
	chartDigest := installation.Spec.Chart.Digest
	if !validSHA256Digest(chartDigest) {
		chartDigest = "sha256:" + strings.Repeat("c", 64)
	}
	record := &api.KubeOVNRecoveryApproval{
		Name: "recovery-smoke-" + uuid.New().String(), Namespace: installation.Namespace,
		Spec: api.RecoveryApprovalSpec{
			InstallationRef: api.InstallationReference{Name: installation.Name, UID: installation.UID}, InstallationGeneration: installation.Generation,
			Mode: mode, SourceVersion: source, TargetVersion: source, BackupRef: "sha256:" + strings.Repeat("a", 64),
			TopologyDigest: "sha256:" + strings.Repeat("b", 64), ChartDigest: chartDigest,
			Reason: "synthetic API/provider recovery approval acceptance", ExpiresAt: metav1.NewTime(time.Now().Add(time.Hour)),
		},
	}
	binding := approval.RecoveryBinding{
		InstallationName: installation.Name, InstallationUID: installation.UID, InstallationGeneration: installation.Generation,
		Mode: mode, SourceVersion: source, TargetVersion: source, BackupRef: record.Spec.BackupRef,
		TopologyDigest: record.Spec.TopologyDigest, ChartDigest: chartDigest,
	}
	if mode != "rollback" {
		request := &api.KubeOVNRecoveryRequest{
			Name: "approval-binding-" + uuid.New().String(), Namespace: installation.Namespace,
			Spec: api.KubeOVNRecoveryRequestSpec{
				InstallationName: installation.Name, Reason: "synthetic API/provider request identity", Mode: api.RecoveryMode(mode),
				BackupRef: record.Spec.BackupRef, RequestedBy: "CI API contract test", ExpiresAt: record.Spec.ExpiresAt,
			},
		}
		if err := kube.Create(ctx, request); err != nil {
			return nil, binding, err
		}
		record.Spec.RequestRef = &api.RecoveryRequestReference{Name: request.Name, UID: request.UID, Generation: request.Generation}
		binding.RequestName, binding.RequestUID, binding.RequestGeneration = request.Name, request.UID, request.Generation
	}
	if err := kube.Create(ctx, record); err != nil {
		return nil, binding, err
	}
	return record, binding, nil
}

func runRecoveryApprovalSmoke(ctx context.Context, kube client.Client, installation *api.KubeOVNInstallation, manager *lease.Manager) error {
	if installation == nil || manager == nil {
		return errors.New("recovery approval smoke requires an installation and lease manager")
	}
	for _, mode := range []string{"rollback", "reconvert", "freshBootstrap"} {
		if err := recoveryApprovalMode(ctx, kube, installation, manager, mode); err != nil {
			return fmt.Errorf("recovery approval %s API acceptance: %w", mode, err)
		}
		fmt.Printf("RecoveryApproval %s: immutable decision, reservation audit, restart, revocation and one-time completion verified on real API; no database restore\n", mode)
	}
	return nil
}

func recoveryApprovalMode(ctx context.Context, kube client.Client, installation *api.KubeOVNInstallation, manager *lease.Manager, mode string) error {
	record, binding, err := recoveryAPIRecord(ctx, kube, installation, mode)
	if err != nil {
		return err
	}
	read := func() (*api.KubeOVNRecoveryApproval, error) {
		current := new(api.KubeOVNRecoveryApproval)
		return current, kube.Get(ctx, client.ObjectKeyFromObject(record), current)
	}
	for name, mutate := range map[string]func(*api.RecoveryApprovalSpec){
		"installation name":       func(s *api.RecoveryApprovalSpec) { s.InstallationRef.Name += "-changed" },
		"installation UID":        func(s *api.RecoveryApprovalSpec) { s.InstallationRef.UID = "other-installation" },
		"installation generation": func(s *api.RecoveryApprovalSpec) { s.InstallationGeneration++ },
		"mode": func(s *api.RecoveryApprovalSpec) {
			if s.Mode == "rollback" {
				s.Mode = "reconvert"
			} else {
				s.Mode = "rollback"
			}
		},
		"source version": func(s *api.RecoveryApprovalSpec) { s.SourceVersion = "v0.0.1" },
		"target version": func(s *api.RecoveryApprovalSpec) { s.TargetVersion = "v0.0.1" },
		"backup":         func(s *api.RecoveryApprovalSpec) { s.BackupRef = "sha256:" + strings.Repeat("d", 64) },
		"topology":       func(s *api.RecoveryApprovalSpec) { s.TopologyDigest = "sha256:" + strings.Repeat("d", 64) },
		"chart":          func(s *api.RecoveryApprovalSpec) { s.ChartDigest = "sha256:" + strings.Repeat("d", 64) },
		"reason":         func(s *api.RecoveryApprovalSpec) { s.Reason = "changed" },
		"expiration":     func(s *api.RecoveryApprovalSpec) { s.ExpiresAt = metav1.NewTime(s.ExpiresAt.Add(time.Hour)) },
		"request identity": func(s *api.RecoveryApprovalSpec) {
			if s.RequestRef == nil {
				s.RequestRef = &api.RecoveryRequestReference{Name: "unexpected", UID: "unexpected", Generation: 1}
			} else {
				s.RequestRef.UID = "other-request"
			}
		},
		"request name": func(s *api.RecoveryApprovalSpec) {
			if s.RequestRef == nil {
				s.RequestRef = &api.RecoveryRequestReference{Name: "unexpected", UID: "unexpected", Generation: 1}
			} else {
				s.RequestRef.Name += "-changed"
			}
		},
		"request generation": func(s *api.RecoveryApprovalSpec) {
			if s.RequestRef == nil {
				s.RequestRef = &api.RecoveryRequestReference{Name: "unexpected", UID: "unexpected", Generation: 1}
			} else {
				s.RequestRef.Generation++
			}
		},
	} {
		attempt := record.DeepCopy()
		mutate(&attempt.Spec)
		if err := kube.Update(ctx, attempt); !apierrors.IsInvalid(err) {
			return fmt.Errorf("decision %s was not immutable: %v", name, err)
		}
	}
	if record.Spec.RequestRef != nil {
		attempt := record.DeepCopy()
		attempt.Spec.RequestRef = nil
		if err := kube.Update(ctx, attempt); !apierrors.IsInvalid(err) {
			return fmt.Errorf("request binding was removed: %v", err)
		}
	}
	for name, status := range map[string]api.RecoveryApprovalStatus{
		"missing digest": {Phase: api.RecoveryApprovalReserved, ReservedAt: new(metav1.Now())},
		"missing time":   {Phase: api.RecoveryApprovalReserved, DecisionDigest: "sha256:" + strings.Repeat("a", 64)},
		"at expiry":      {Phase: api.RecoveryApprovalReserved, DecisionDigest: "sha256:" + strings.Repeat("a", 64), ReservedAt: new(record.Spec.ExpiresAt)},
	} {
		attempt := record.DeepCopy()
		attempt.Status = status
		if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
			return fmt.Errorf("malformed reservation %s was accepted: %v", name, err)
		}
	}
	// A well-shaped completion must not skip its separate reservation write.
	skipped := record.DeepCopy()
	now := metav1.Now()
	skipped.Status = api.RecoveryApprovalStatus{
		Phase: api.RecoveryApprovalCompleted, DecisionDigest: "sha256:" + strings.Repeat("a", 64), ReservedAt: &now, CompletedAt: &now,
	}
	if err := kube.Status().Update(ctx, skipped); !apierrors.IsInvalid(err) {
		return fmt.Errorf("completion skipped reservation: %v", err)
	}
	provider, err := approval.NewRecovery(kube, installation)
	if err != nil {
		return err
	}
	ref := api.RecoveryApprovalReference{Name: record.Name, UID: record.UID}
	return manager.With(ctx, installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			wrong := binding
			wrong.BackupRef = "sha256:" + strings.Repeat("f", 64)
			if err := provider.Reserve(ctx, ref, wrong); err == nil {
				return errors.New("foreign backup authorized reservation")
			}
			if err := provider.Reserve(ctx, ref, binding); err != nil {
				return err
			}
			reserved, err := read()
			if err != nil {
				return err
			}
			if reserved.Status.Phase != api.RecoveryApprovalReserved || reserved.Status.ReservedAt == nil || reserved.Status.DecisionDigest == "" {
				return errors.New("reservation audit was not recorded")
			}
			restarted, err := approval.NewRecovery(kube, installation)
			if err != nil {
				return err
			}
			if err := restarted.Reserve(ctx, ref, binding); err != nil {
				return err
			}
			current, err := read()
			if err != nil {
				return err
			}
			if current.ResourceVersion != reserved.ResourceVersion {
				return errors.New("reservation retry rewrote audit")
			}
			for name, mutate := range map[string]func(*api.RecoveryApprovalStatus){
				"clear reservation":         func(s *api.RecoveryApprovalStatus) { *s = api.RecoveryApprovalStatus{} },
				"digest":                    func(s *api.RecoveryApprovalStatus) { s.DecisionDigest = "sha256:" + strings.Repeat("d", 64) },
				"reserved time":             func(s *api.RecoveryApprovalStatus) { s.ReservedAt = new(metav1.NewTime(s.ReservedAt.Add(time.Second))) },
				"early completion evidence": func(s *api.RecoveryApprovalStatus) { s.CompletedAt = new(metav1.Now()) },
				"missing completion time":   func(s *api.RecoveryApprovalStatus) { s.Phase = api.RecoveryApprovalCompleted },
				"completion before reservation": func(s *api.RecoveryApprovalStatus) {
					s.Phase = api.RecoveryApprovalCompleted
					s.CompletedAt = new(metav1.NewTime(s.ReservedAt.Add(-time.Second)))
				},
			} {
				attempt := current.DeepCopy()
				mutate(&attempt.Status)
				if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
					return fmt.Errorf("reservation %s was mutable: %v", name, err)
				}
			}
			current.Spec.Revoked = true
			if err := kube.Update(ctx, current); err != nil {
				return fmt.Errorf("one-way recovery revocation was rejected: %w", err)
			}
			if err := kube.Status().Update(ctx, reserved); !apierrors.IsConflict(err) {
				return fmt.Errorf("stale status did not conflict with revocation: %v", err)
			}
			if err := restarted.Reserve(ctx, ref, binding); err == nil {
				return errors.New("revoked reservation authorized more writes")
			}
			current, err = read()
			if err != nil {
				return err
			}
			attempt := current.DeepCopy()
			attempt.Spec.Revoked = false
			if err := kube.Update(ctx, attempt); !apierrors.IsInvalid(err) {
				return fmt.Errorf("revocation was reversed: %v", err)
			}
			// Synthetic convergence exercises audit recording, not native writes.
			if err := restarted.Complete(ctx, ref, binding); err != nil {
				return err
			}
			completed, err := read()
			if err != nil {
				return err
			}
			if completed.Status.Phase != api.RecoveryApprovalCompleted || completed.Status.CompletedAt == nil || completed.Status.DecisionDigest != reserved.Status.DecisionDigest {
				return errors.New("completion audit lost its reserved identity")
			}
			if err := restarted.Complete(ctx, ref, binding); err != nil {
				return err
			}
			current, err = read()
			if err != nil {
				return err
			}
			if current.ResourceVersion != completed.ResourceVersion {
				return errors.New("completion retry rewrote audit")
			}
			for name, mutate := range map[string]func(*api.RecoveryApprovalStatus){
				"clear":   func(s *api.RecoveryApprovalStatus) { *s = api.RecoveryApprovalStatus{} },
				"recycle": func(s *api.RecoveryApprovalStatus) { s.Phase, s.CompletedAt = api.RecoveryApprovalReserved, nil },
				"completion time": func(s *api.RecoveryApprovalStatus) {
					s.CompletedAt = new(metav1.NewTime(s.CompletedAt.Add(time.Second)))
				},
			} {
				attempt := current.DeepCopy()
				mutate(&attempt.Status)
				if err := kube.Status().Update(ctx, attempt); !apierrors.IsInvalid(err) {
					return fmt.Errorf("completed audit %s was mutable: %v", name, err)
				}
			}
			if err := restarted.Reserve(ctx, ref, binding); err == nil {
				return errors.New("completed recovery approval was recycled")
			}
			return nil
		})
	})
}
