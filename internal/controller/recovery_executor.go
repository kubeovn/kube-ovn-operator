package controller

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
	"uuid"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrRecoveryInProgress tells the RecoveryRequest reconciler that one durable
// lifecycle step was accepted but the operation has not converged yet. It is
// intentionally separate from an execution failure so retries remain visible
// as Executing instead of being converted into a terminal Failed request.
var ErrRecoveryInProgress = errors.New("recovery execution is still in progress")

// LifecycleRecoveryExecutor adapts an explicit rejoin plan to the existing
// joint NB/SB lifecycle journal. It owns no alternate recovery state: Begin
// and Resume make the ConfigMap journal the source of truth across restarts.
// Destructive restore modes use the separate rollback executor, which requires
// a verified backup pair, workload quiesce capability and an atomic restore
// fence.
type LifecycleRecoveryExecutor struct {
	Client    client.Client
	Leases    lease.Runner
	Topology  topology.InventoryProvider
	Lifecycle lifecycleCoordinator
	Rollback  RollbackStepExecutor
}

func (e *LifecycleRecoveryExecutor) Execute(ctx context.Context, plan RecoveryPlan) error {
	if e == nil || e.Client == nil || e.Leases == nil || e.Topology == nil || e.Lifecycle == nil {
		return errors.New("rejoin recovery executor is not fully configured")
	}
	if plan.Mode != operatorv1alpha1.RecoveryModeRejoin {
		return e.executeDestructive(ctx, plan)
	}
	if plan.TargetMemberRef == "" || plan.TargetMemberDigest == "" || plan.TopologyDigest == "" {
		return errors.New("rejoin recovery plan is missing its target or topology identity")
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.InstallationNamespace, Name: plan.InstallationName}, installation); err != nil {
		return fmt.Errorf("read recovery installation: %w", err)
	}
	if installation.UID != plan.InstallationUID || installation.Generation != plan.InstallationGeneration {
		return errors.New("recovery installation identity or generation changed")
	}
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.RequestNamespace, Name: plan.RequestName}, request); err != nil {
		return fmt.Errorf("read recovery request: %w", err)
	}
	if request.UID != plan.RequestUID || request.Generation != plan.RequestGeneration {
		return errors.New("recovery request identity or generation changed")
	}
	admitted, reason, err := buildRecoveryPlan(request, installation, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("revalidate recovery plan: %s: %w", reason, err)
	}
	if admitted.Digest != plan.Digest {
		return errors.New("recovery plan digest changed before execution")
	}
	if installation.Status.Topology.Inventory == nil || installation.Status.Topology.Inventory.Digest != plan.TopologyDigest {
		return errors.New("recovery retained topology changed after plan admission")
	}
	targetRef, targetDigest, _, err := recoveryTarget(request, installation)
	if err != nil {
		return err
	}
	if targetRef != plan.TargetMemberRef || targetDigest != plan.TargetMemberDigest {
		return errors.New("recovery target member changed after plan admission")
	}
	if request.Status.Execution == nil {
		request.Status.Execution = &operatorv1alpha1.RecoveryExecutionStatus{StartedAt: metav1.NewTime(time.Now().UTC())}
		if err := e.Client.Status().Update(ctx, request); err != nil {
			return fmt.Errorf("persist rejoin recovery start: %w", err)
		}
	}
	var operation lifecycle.Reference
	var recovery *lifecycle.Recovery
	err = e.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return e.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			if request.Status.Execution.Operation != nil {
				pinned := request.Status.Execution.Operation
				recovery, err = e.Lifecycle.Resume(centralCtx, installation, pinned)
				return err
			}
			retained, err := e.Topology.Retained(centralCtx, installation)
			if err != nil {
				return fmt.Errorf("read retained topology for rejoin: %w", err)
			}
			if err := retained.Validate(); err != nil {
				return fmt.Errorf("validate retained topology for rejoin: %w", err)
			}
			lifecycleRequest, err := lifecycle.SelectAdditionForMember(centralCtx, e.Lifecycle.Reader(), installation, retained, plan.TargetMemberRef)
			if err != nil {
				return fmt.Errorf("select rejoin target %q: %w", plan.TargetMemberRef, err)
			}
			if lifecycleRequest == nil {
				return errors.New("rejoin target did not produce a lifecycle operation")
			}
			if err := pinRecoveryOperationID(lifecycleRequest, plan.Digest); err != nil {
				return err
			}
			operation, err = e.Lifecycle.Begin(centralCtx, installation, *lifecycleRequest)
			if err != nil {
				return fmt.Errorf("admit rejoin lifecycle: %w", err)
			}
			pinned := operationReference(operation, installation.Status.Topology.Inventory)
			request.Status.Execution.Operation = pinned
			if err := e.Client.Status().Update(ctx, request); err != nil {
				return fmt.Errorf("persist rejoin lifecycle operation: %w", err)
			}
			recovery, err = e.Lifecycle.Resume(centralCtx, installation, pinned)
			return err
		})
	})
	if err != nil {
		return err
	}
	if recovery == nil || recovery.State.Phase != lifecycle.Complete {
		return ErrRecoveryInProgress
	}
	return nil
}

// pinRecoveryOperationID makes retries after a lost Begin response converge on
// the same lifecycle journal. The recovery plan digest is immutable and already
// binds the installation, request, target member and retained topology.
func pinRecoveryOperationID(request *lifecycle.Request, planDigest string) error {
	if request == nil || request.Add == nil {
		return errors.New("rejoin lifecycle request is missing its member addition")
	}
	digest, ok := strings.CutPrefix(strings.TrimSpace(planDigest), "sha256:")
	if !ok || len(digest) != 64 {
		return errors.New("rejoin recovery plan digest is not a SHA-256 value")
	}
	digestBytes, err := hex.DecodeString(digest)
	if err != nil || len(digestBytes) < 16 {
		return errors.New("rejoin recovery plan digest is invalid")
	}
	var operationID uuid.UUID
	copy(operationID[:], digestBytes[:16])
	// RFC 9562 version and variant bits keep the digest-derived value in the
	// canonical UUID space accepted by the lifecycle request validator.
	operationID[6] = operationID[6]&0x0f | 0x50
	operationID[8] = operationID[8]&0x3f | 0x80
	request.OperationID = operationID.String()
	request.Add.Workload.Preparation.OperationID = request.OperationID
	return nil
}

func (e *LifecycleRecoveryExecutor) executeDestructive(ctx context.Context, plan RecoveryPlan) error {
	if e.Rollback == nil {
		return fmt.Errorf("recovery mode %q requires a fenced database restore and workload rollback executor", plan.Mode)
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.InstallationNamespace, Name: plan.InstallationName}, installation); err != nil {
		return fmt.Errorf("read destructive recovery installation: %w", err)
	}
	if installation.UID != plan.InstallationUID || installation.Generation != plan.InstallationGeneration || installation.Spec.Version != plan.TargetVersion || installation.Status.ObservedVersion != plan.SourceVersion {
		return errors.New("destructive recovery installation identity or version changed")
	}
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.RequestNamespace, Name: plan.RequestName}, request); err != nil {
		return fmt.Errorf("read destructive recovery request: %w", err)
	}
	if request.UID != plan.RequestUID || request.Generation != plan.RequestGeneration || request.Status.Plan == nil || request.Status.Plan.Digest != plan.Digest {
		return errors.New("destructive recovery request identity or immutable plan changed")
	}
	if err := e.Rollback.Ready(); err != nil {
		return err
	}
	execution := request.Status.Execution
	if execution == nil {
		execution = &operatorv1alpha1.RecoveryExecutionStatus{Phase: operatorv1alpha1.RollbackPhaseQuiescing, StartedAt: metav1.NewTime(time.Now().UTC())}
		if capturedAt, ok := destructiveRecoveryBackupTime(installation, plan); ok {
			execution.RPOSeconds = maxDurationSeconds(time.Since(capturedAt))
		}
		request.Status.Execution = execution
		if err := e.Client.Status().Update(ctx, request); err != nil {
			return fmt.Errorf("persist destructive recovery start: %w", err)
		}
		execution = request.Status.Execution
	}
	if execution.Phase == operatorv1alpha1.RollbackPhaseSucceeded {
		return nil
	}
	if execution.Phase == operatorv1alpha1.RollbackPhaseFailed {
		return errors.New(execution.Reason)
	}
	rollbackPlan := RollbackPlan{InstallationNamespace: plan.InstallationNamespace, InstallationName: plan.InstallationName, InstallationUID: plan.InstallationUID, InstallationGeneration: plan.InstallationGeneration, SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, BackupID: plan.BackupRef, DatabaseRefs: maps.Clone(plan.DatabaseRefs), Destination: installation.Spec.Upgrade.Backup.Destination, TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest, ApprovalRef: plan.ApprovalRef, RequestName: plan.RequestName, RequestUID: plan.RequestUID, RequestGeneration: plan.RequestGeneration, Mode: string(plan.Mode), OperationToken: plan.Digest}
	var next operatorv1alpha1.RollbackPhase
	err := e.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return e.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			var err error
			next, err = e.Rollback.Step(centralCtx, rollbackPlan, execution.Phase)
			return err
		})
	})
	if err != nil {
		if errors.Is(err, ErrRollbackInProgress) {
			execution.Reason = "destructive recovery phase accepted; waiting for convergence"
			if updateErr := e.Client.Status().Update(ctx, request); updateErr != nil {
				return errors.Join(err, updateErr)
			}
			return ErrRecoveryInProgress
		}
		execution.Phase, execution.Reason = operatorv1alpha1.RollbackPhaseFailed, err.Error()
		if updateErr := e.Client.Status().Update(ctx, request); updateErr != nil {
			return errors.Join(err, updateErr)
		}
		return err
	}
	execution.Phase, execution.Reason = next, "destructive recovery phase advanced"
	if next == operatorv1alpha1.RollbackPhaseSucceeded {
		now := time.Now().UTC()
		execution.CompletedAt = metav1.NewTime(now)
		execution.RTOSeconds = maxDurationSeconds(now.Sub(execution.StartedAt.Time))
		execution.Reason = "both databases, target workloads and verification gates converged"
	}
	if err := e.Client.Status().Update(ctx, request); err != nil {
		return fmt.Errorf("persist destructive recovery phase: %w", err)
	}
	if next != operatorv1alpha1.RollbackPhaseSucceeded {
		return ErrRecoveryInProgress
	}
	return nil
}

// destructiveRecoveryBackupTime resolves the exact verified pair that was
// admitted into a destructive recovery plan. The pair may live in
// status.lastBackup or in a still-valid upgrade checkpoint after a restart;
// using only lastBackup would silently report RPO=0 for the checkpoint
// fallback path.
func destructiveRecoveryBackupTime(installation *operatorv1alpha1.KubeOVNInstallation, plan RecoveryPlan) (time.Time, bool) {
	if installation == nil || strings.TrimSpace(plan.BackupRef) == "" {
		return time.Time{}, false
	}
	if backup := installation.Status.LastBackup; backup != nil && backup.ID == plan.BackupRef && backupPairIDMatches(backup) && !backup.CompletedAt.IsZero() {
		return backup.CompletedAt.Time, true
	}
	checkpoint := installation.Status.Upgrade.Checkpoint
	if checkpoint == nil || checkpoint.ID != plan.BackupRef || !rollbackCheckpointUsable(checkpoint.Phase) || checkpoint.CapturedAt.IsZero() {
		return time.Time{}, false
	}
	backup := backupFromUpgradeCheckpoint(checkpoint)
	if !backupPairIDMatches(backup) {
		return time.Time{}, false
	}
	return checkpoint.CapturedAt.Time, true
}

var _ RecoveryExecutor = (*LifecycleRecoveryExecutor)(nil)
