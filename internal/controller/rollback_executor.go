package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrRollbackInProgress keeps a durable rollback phase retryable after a
// workload or database operation has been accepted but has not converged.
var ErrRollbackInProgress = errors.New("version rollback is still converging")

type RollbackPlan struct {
	InstallationNamespace  string
	InstallationName       string
	InstallationUID        types.UID
	InstallationGeneration int64
	SourceVersion          string
	TargetVersion          string
	BackupID               string
	DatabaseRefs           map[string]string
	Destination            string
	TopologyDigest         string
	ChartDigest            string
	ApprovalRef            *operatorv1alpha1.RecoveryApprovalReference
	RequestName            string
	RequestUID             types.UID
	RequestGeneration      int64
	Mode                   string
	// OperationToken binds durable workload quiesce markers to this exact
	// admitted rollback/recovery operation. It must not be reused merely
	// because two operations happen to restore the same backup pair.
	OperationToken string
}

// RollbackStepExecutor exposes a restart-safe phase boundary. Every step is
// idempotent: a lost response leaves the same phase to be retried rather than
// inventing a second backup or a different target.
type RollbackStepExecutor interface {
	Ready() error
	Step(context.Context, RollbackPlan, operatorv1alpha1.RollbackPhase) (operatorv1alpha1.RollbackPhase, error)
}

type rollbackCapabilities interface {
	Capabilities() (restoreReady, workloadReady, fenceReady bool, err error)
}

// KubernetesRollbackExecutor is the production adapter for a version-aware
// rollback. It restores the verified NB/SB pair through lifecycle's native
// OVSDB adapters and applies the exact digest-pinned target Chart through the
// normal ownership-checked workload client.
type KubernetesRollbackExecutor struct {
	Client          client.Reader
	Lifecycle       lifecycleCoordinator
	Topology        topology.InventoryProvider
	RendererFactory func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer
	WorkloadFactory func(apply.InstallationIdentity) apply.ApplyClient
	OVN             health.OVNProvider
	Runtime         health.RuntimeProvider
	ApprovalFactory func(*operatorv1alpha1.KubeOVNInstallation) (approval.RecoveryApprover, error)
}

func (e *KubernetesRollbackExecutor) Ready() error {
	if e == nil || e.Client == nil || e.Lifecycle == nil || e.Topology == nil || e.RendererFactory == nil || e.WorkloadFactory == nil {
		return errors.New("rollback requires retained topology, restore, chart and workload adapters")
	}
	fenced, ok := e.Lifecycle.(interface{ RestoreFenceReady() error })
	if !ok {
		return errors.New("rollback requires an atomic restore fence capability")
	}
	if err := fenced.RestoreFenceReady(); err != nil {
		return err
	}
	return nil
}

func (e *KubernetesRollbackExecutor) Capabilities() (bool, bool, bool, error) {
	if err := e.Ready(); err != nil {
		return false, false, false, err
	}
	workload := e.WorkloadFactory(apply.InstallationIdentity{Namespace: "rollback", Name: "rollback", UID: types.UID("rollback")})
	if workload == nil {
		return true, false, false, errors.New("rollback workload factory returned nil")
	}
	_, workloadReady := workload.(apply.RollbackClient)
	return true, workloadReady, true, nil
}

func (e *KubernetesRollbackExecutor) Step(ctx context.Context, plan RollbackPlan, phase operatorv1alpha1.RollbackPhase) (operatorv1alpha1.RollbackPhase, error) {
	if err := e.Ready(); err != nil {
		return phase, err
	}
	operationToken, err := rollbackOperationToken(plan)
	if err != nil {
		return phase, err
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := e.Client.Get(ctx, client.ObjectKey{Namespace: plan.InstallationNamespace, Name: plan.InstallationName}, installation); err != nil {
		return phase, fmt.Errorf("read rollback installation: %w", err)
	}
	if installation.UID != plan.InstallationUID || installation.Generation != plan.InstallationGeneration || installation.Status.ObservedVersion != plan.SourceVersion || installation.Spec.Version != plan.TargetVersion {
		return phase, errors.New("rollback installation identity or version changed")
	}
	if installation.Spec.Chart.Digest != plan.ChartDigest {
		return phase, errors.New("rollback target Chart digest changed")
	}
	retained, err := e.Topology.Retained(ctx, installation)
	if err != nil {
		return phase, fmt.Errorf("read retained topology before rollback: %w", err)
	}
	if err := retained.Validate(); err != nil {
		return phase, fmt.Errorf("validate retained topology before rollback: %w", err)
	}
	if retainedDigest := installation.Status.Topology.Inventory; retainedDigest == nil || retainedDigest.Digest != plan.TopologyDigest {
		return phase, errors.New("rollback topology changed after admission")
	}
	workload := e.WorkloadFactory(apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID})
	if workload == nil {
		return phase, errors.New("rollback workload adapter is nil")
	}
	var rendered chart.Rendered
	if phase == operatorv1alpha1.RollbackPhaseWorkloads || phase == operatorv1alpha1.RollbackPhaseVerifying {
		rendered, err = e.renderRollbackTarget(ctx, installation, retained, plan.ChartDigest)
		if err != nil {
			return phase, err
		}
	}
	if phase == operatorv1alpha1.RollbackPhaseQuiescing {
		if preflight, ok := e.Lifecycle.(interface {
			RestoreFenceReadyFor(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology) error
		}); ok {
			if err := preflight.RestoreFenceReadyFor(ctx, installation, retained); err != nil {
				return phase, err
			}
		}
		if err := e.admitRestore(ctx, installation, plan); err != nil {
			return phase, err
		}
	}
	backup, err := rollbackBackupStatus(installation, plan)
	if err != nil {
		return phase, err
	}
	binding := lifecycle.BackupBinding{Destination: strings.TrimSpace(plan.Destination), Version: plan.TargetVersion, TopologyDigest: plan.TopologyDigest}
	switch phase {
	case operatorv1alpha1.RollbackPhaseQuiescing:
		quiescer, ok := workload.(apply.RollbackClient)
		if !ok {
			return phase, errors.New("rollback workload adapter does not support quiesce and unquiesce")
		}
		if err := quiescer.Quiesce(ctx, operationToken); err != nil {
			cleanupErr := quiescer.Unquiesce(ctx, operationToken, nil)
			return phase, errors.Join(fmt.Errorf("quiesce rollback workloads: %w", err), cleanupErr)
		}
		return operatorv1alpha1.RollbackPhaseRestoring, nil
	case operatorv1alpha1.RollbackPhaseRestoring:
		if err := e.Lifecycle.RestoreBackupPair(ctx, installation, retained, binding, backup); err != nil {
			return phase, fmt.Errorf("restore rollback database pair: %w", err)
		}
		return operatorv1alpha1.RollbackPhaseWorkloads, nil
	case operatorv1alpha1.RollbackPhaseWorkloads:
		if err := workload.Apply(ctx, rendered.Objects); err != nil {
			return phase, fmt.Errorf("apply rollback target workloads: %w", err)
		}
		quiescer, ok := workload.(apply.RollbackClient)
		if !ok {
			return phase, errors.New("rollback workload adapter lost quiesce capability")
		}
		if err := quiescer.Unquiesce(ctx, operationToken, rendered.Objects); err != nil {
			return phase, fmt.Errorf("resume rollback workloads: %w", err)
		}
		return operatorv1alpha1.RollbackPhaseVerifying, nil
	case operatorv1alpha1.RollbackPhaseVerifying:
		observer, ok := workload.(apply.Observer)
		if !ok {
			return phase, errors.New("rollback workload adapter does not support observation")
		}
		report, err := observer.Observe(ctx, rendered.Objects)
		if err != nil {
			return phase, fmt.Errorf("observe rollback workloads: %w", err)
		}
		if !report.Ready {
			return phase, ErrRollbackInProgress
		}
		if e.OVN != nil {
			ovnReport, err := e.OVN.Observe(ctx, installation)
			if err != nil {
				return phase, fmt.Errorf("observe OVN after rollback: %w", err)
			}
			if !ovnReport.Ready {
				return phase, ErrRollbackInProgress
			}
		}
		if e.Runtime != nil {
			runtimeReport := e.Runtime.Observe(ctx, installation)
			if runtimeReport.Northd.Status != metav1.ConditionTrue || runtimeReport.DataPlane.Status != metav1.ConditionTrue {
				return phase, ErrRollbackInProgress
			}
		}
		if plan.ApprovalRef != nil {
			approver, err := e.approval(installation)
			if err != nil {
				return phase, err
			}
			if err := approver.Complete(ctx, *plan.ApprovalRef, approvalBinding(plan)); err != nil {
				return phase, fmt.Errorf("complete lost-quorum recovery approval: %w", err)
			}
		}
		return operatorv1alpha1.RollbackPhaseSucceeded, nil
	case operatorv1alpha1.RollbackPhaseSucceeded:
		return phase, nil
	default:
		return phase, fmt.Errorf("unsupported rollback phase %q", phase)
	}
}

// rollbackOperationToken returns a stable, content-addressed token for the
// admitted operation. Recovery requests pass their immutable plan digest
// explicitly; version rollback derives the same binding from its immutable
// RollbackPlan. The fallback keeps direct executor tests and older callers
// safe while ensuring BackupID alone is never the operation identity.
func rollbackOperationToken(plan RollbackPlan) (string, error) {
	if token := strings.TrimSpace(plan.OperationToken); token != "" {
		if !validRollbackDigest(token) {
			return "", fmt.Errorf("rollback operation token must be a SHA-256 digest")
		}
		return token, nil
	}
	data, err := stdjson.Marshal(struct {
		InstallationNamespace  string
		InstallationName       string
		InstallationUID        types.UID
		InstallationGeneration int64
		SourceVersion          string
		TargetVersion          string
		BackupID               string
		DatabaseRefs           map[string]string
		Destination            string
		TopologyDigest         string
		ChartDigest            string
		ApprovalRef            *operatorv1alpha1.RecoveryApprovalReference
		RequestName            string
		RequestUID             types.UID
		RequestGeneration      int64
		Mode                   string
	}{
		InstallationNamespace:  plan.InstallationNamespace,
		InstallationName:       plan.InstallationName,
		InstallationUID:        plan.InstallationUID,
		InstallationGeneration: plan.InstallationGeneration,
		SourceVersion:          plan.SourceVersion,
		TargetVersion:          plan.TargetVersion,
		BackupID:               plan.BackupID,
		DatabaseRefs:           plan.DatabaseRefs,
		Destination:            plan.Destination,
		TopologyDigest:         plan.TopologyDigest,
		ChartDigest:            plan.ChartDigest,
		ApprovalRef:            plan.ApprovalRef,
		RequestName:            plan.RequestName,
		RequestUID:             plan.RequestUID,
		RequestGeneration:      plan.RequestGeneration,
		Mode:                   plan.Mode,
	}, stdjson.Deterministic(true))
	if err != nil {
		return "", fmt.Errorf("encode rollback operation binding: %w", err)
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

func validRollbackDigest(value string) bool {
	return len(value) == len("sha256:")+64 && strings.HasPrefix(value, "sha256:") && strings.Trim(value[len("sha256:"):], "0123456789abcdef") == ""
}

// renderRollbackTarget applies the same topology and probe transformations as
// the normal installation reconcile. A rollback target Chart may describe the
// original central replica count while independent lifecycle members are still
// retained outside its Deployment; applying the raw render would recreate those
// members and split the RAFT topology. The network probe objects are likewise
// part of the installation contract and must survive a rollback render.
func (e *KubernetesRollbackExecutor) renderRollbackTarget(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, retained health.CentralTopology, expectedDigest string) (chart.Rendered, error) {
	if e == nil || e.RendererFactory == nil {
		return chart.Rendered{}, errors.New("rollback Chart renderer is nil")
	}
	renderer := e.RendererFactory(installation)
	if renderer == nil {
		return chart.Rendered{}, errors.New("rollback Chart renderer is nil")
	}
	rendered, err := renderer.Render(ctx, installation.Spec)
	if err != nil {
		return chart.Rendered{}, fmt.Errorf("render rollback target Chart: %w", err)
	}
	if rendered.Digest != expectedDigest {
		return chart.Rendered{}, errors.New("rendered rollback Chart digest differs from admitted digest")
	}
	if installation.Spec.Central.DynamicMembership {
		if err := applyRetainedChartDeploymentReplicas(rendered.Objects, retained); err != nil {
			return chart.Rendered{}, fmt.Errorf("preserve retained Chart Deployment replicas during rollback: %w", err)
		}
	}
	probes, err := chart.NetworkProbeObjects(installation, rendered.Objects)
	if err != nil {
		return chart.Rendered{}, fmt.Errorf("render rollback network probes: %w", err)
	}
	rendered.Objects = append(rendered.Objects, probes...)
	return rendered, nil
}

func rollbackBackupStatus(installation *operatorv1alpha1.KubeOVNInstallation, plan RollbackPlan) (*operatorv1alpha1.BackupStatus, error) {
	if installation == nil {
		return nil, errors.New("rollback backup requires an installation")
	}
	if backup := installation.Status.LastBackup; backup != nil && backup.ID == plan.BackupID {
		if !backupPairIDMatches(backup) || backup.Version != plan.TargetVersion || strings.TrimSpace(backup.Destination) != strings.TrimSpace(plan.Destination) || backup.TopologyDigest != plan.TopologyDigest || backup.CompletedAt.IsZero() || !maps.Equal(backup.DatabaseRefs, plan.DatabaseRefs) {
			return nil, errors.New("rollback backup status is not bound to the admitted target version, topology and database references")
		}
		copy := new(operatorv1alpha1.BackupStatus)
		backup.DeepCopyInto(copy)
		return copy, nil
	}
	checkpoint := installation.Status.Upgrade.Checkpoint
	checkpointBackup := backupFromUpgradeCheckpoint(checkpoint)
	if checkpoint == nil || checkpointBackup == nil || !rollbackCheckpointUsable(checkpoint.Phase) || !backupPairIDMatches(checkpointBackup) || checkpoint.ID != plan.BackupID || checkpoint.SourceVersion != plan.TargetVersion || strings.TrimSpace(checkpoint.Destination) != strings.TrimSpace(plan.Destination) || checkpoint.TopologyDigest != plan.TopologyDigest || checkpoint.CapturedAt.IsZero() || !maps.Equal(checkpoint.DatabaseRefs, plan.DatabaseRefs) {
		return nil, errors.New("rollback backup pair is no longer available in the durable installation checkpoint")
	}
	return checkpointBackup, nil
}

func rollbackCheckpointUsable(phase operatorv1alpha1.UpgradeCheckpointPhase) bool {
	switch phase {
	case operatorv1alpha1.UpgradeCheckpointVerified, operatorv1alpha1.UpgradeCheckpointApplying, operatorv1alpha1.UpgradeCheckpointApplied:
		return true
	default:
		return false
	}
}

func (e *KubernetesRollbackExecutor) admitRestore(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan RollbackPlan) error {
	if plan.ApprovalRef != nil {
		approver, err := e.approval(installation)
		if err != nil {
			return err
		}
		if err := approver.Reserve(ctx, *plan.ApprovalRef, approvalBinding(plan)); err != nil {
			return fmt.Errorf("reserve lost-quorum recovery approval: %w", err)
		}
		return nil
	}
	if e.OVN == nil {
		return errors.New("rollback requires an OVN quorum observer when no recovery approval is supplied")
	}
	report, err := e.OVN.Observe(ctx, installation)
	if err == nil && report.Ready {
		return nil
	}
	if err != nil {
		return fmt.Errorf("rollback quorum is unavailable and no recovery approval was supplied: %w", err)
	}
	return fmt.Errorf("rollback quorum is unavailable and no recovery approval was supplied: %s", report.Message)
}

func (e *KubernetesRollbackExecutor) approval(installation *operatorv1alpha1.KubeOVNInstallation) (approval.RecoveryApprover, error) {
	if e.ApprovalFactory == nil {
		return nil, errors.New("lost-quorum recovery approval provider is not configured")
	}
	approver, err := e.ApprovalFactory(installation)
	if err != nil {
		return nil, fmt.Errorf("create recovery approval provider: %w", err)
	}
	if approver == nil {
		return nil, errors.New("lost-quorum recovery approval provider returned nil")
	}
	return approver, nil
}

func approvalBinding(plan RollbackPlan) approval.RecoveryBinding {
	return approval.RecoveryBinding{
		InstallationName: plan.InstallationName, InstallationUID: plan.InstallationUID, InstallationGeneration: plan.InstallationGeneration,
		RequestName: plan.RequestName, RequestUID: plan.RequestUID, RequestGeneration: plan.RequestGeneration,
		Mode: plan.Mode, SourceVersion: plan.SourceVersion, TargetVersion: plan.TargetVersion, BackupRef: plan.BackupID,
		TopologyDigest: plan.TopologyDigest, ChartDigest: plan.ChartDigest,
	}
}
