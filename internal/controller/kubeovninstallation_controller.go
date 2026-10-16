package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	planConfigMapPrefix = "kube-ovn-operator-plan-"
)

type KubeOVNInstallationReconciler struct {
	client.Client
	Scheme          *apiruntime.Scheme
	Clock           func() time.Time
	Renderer        chart.Renderer
	RendererFactory func(*operatorv1alpha1.KubeOVNInstallation) chart.Renderer
	Workload        apply.ApplyClient
	Leases          lease.Runner
	OVN             health.OVNProvider
	Runtime         health.RuntimeProvider
	Network         health.NetworkProvider
	Topology        topology.Provider
	Lifecycle       *lifecycle.Manager
	WorkloadFactory func(apply.InstallationIdentity) apply.ApplyClient
}

func (r *KubeOVNInstallationReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		For(&operatorv1alpha1.KubeOVNInstallation{}).
		Owns(&corev1.ConfigMap{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

func (r *KubeOVNInstallationReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithValues("installation", req.NamespacedName)
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := r.Get(ctx, req.NamespacedName, installation); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if !installation.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	now := time.Now
	if r.Clock != nil {
		now = r.Clock
	}
	plan := planner.Build(installation.Spec, installation.Status.ObservedVersion)
	if plan.Blocking != "" {
		if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
			return reconcile.Result{}, err
		}
		logger.Info("installation plan is blocked", "reason", plan.Blocking)
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// Recover a durable membership operation before rendering the chart. Node
	// changes can make bootstrap rendering fail, while the operation's pinned
	// inventory remains the only safe path to convergence.
	if localRaft(installation) && r.Lifecycle != nil {
		if r.Leases == nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "LifecycleLease", "lifecycle recovery requires operation leases"
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
		}
		var recovery *lifecycle.Recovery
		recoveryErr := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
			return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
				var err error
				recovery, err = r.Lifecycle.Resume(centralCtx, installation, installation.Status.Topology.Operation)
				return err
			})
		})
		if recoveryErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", recoveryErr.Error()
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
				return reconcile.Result{}, err
			}
			return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
		}
		if recovery != nil {
			if installation.Status.Topology.Operation == nil {
				installation.Status.Topology.Operation = newOperationReference(recovery, installation.Status.Topology.Inventory)
			}
			if recovery.State.Phase != lifecycle.Complete {
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", fmt.Sprintf("durable central operation is %s", recovery.State.Phase)
				condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleInProgress", Message: plan.Blocking}
				if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
			}
			operation := installation.Status.Topology.Operation
			if operation == nil {
				return reconcile.Result{}, errors.New("completed lifecycle has no operation reference")
			}
			if recovery.Completion == nil && operation.CompletionDigest == "" {
				return reconcile.Result{}, errors.New("completed lifecycle has no immutable completion receipt")
			}
			if operation.CompletionDigest == "" && recovery.Completion != nil {
				operation.CompletionDigest = recovery.Completion.Digest
			}
			if operation.HandoffInventory == nil {
				if r.Topology == nil {
					return reconcile.Result{}, errors.New("completed lifecycle requires a topology handoff provider")
				}
				handoff, ok := r.Topology.(topology.HandoffProvider)
				if !ok || installation.Status.Topology.Inventory == nil {
					return reconcile.Result{}, errors.New("completed lifecycle cannot hand off without the retained topology provider")
				}
				var next *operatorv1alpha1.CentralTopologyReference
				handoffErr := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
					return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
						var err error
						next, err = handoff.Handoff(centralCtx, installation, installation.Status.Topology.Inventory)
						return err
					})
				})
				if handoffErr != nil {
					plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "TopologyHandoff", handoffErr.Error()
					if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); err != nil {
						return reconcile.Result{}, err
					}
					return reconcile.Result{RequeueAfter: 15 * time.Second}, nil
				}
				if next == nil {
					return reconcile.Result{}, errors.New("topology handoff returned no reference")
				}
				operation.HandoffInventory = next
				operation.HandoffApplied = false
				installation.Status.Topology.Inventory = next
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "WorkloadHandoff", "central topology handed off; apply checkpoint authorization is pending"
				condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionTrue, Reason: "LifecycleComplete", Message: plan.Blocking}
				if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
					return reconcile.Result{}, err
				}
				return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
			}
			if installation.Status.Topology.Inventory == nil || *installation.Status.Topology.Inventory != *operation.HandoffInventory {
				return reconcile.Result{}, errors.New("completed lifecycle handoff does not match the retained topology")
			}
			if !operation.HandoffApplied {
				plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "WorkloadHandoff", "lifecycle completion authorizes the next Chart apply checkpoint"
			}
		}
	}
	if localRaft(installation) && r.Lifecycle != nil && (installation.Status.Topology.Operation == nil || installation.Status.Topology.Operation.HandoffApplied) && installation.Status.Topology.Inventory != nil && installation.Status.ApplyCheckpoint != nil {
		started, selectionErr := r.beginAutomaticLifecycle(ctx, installation, plan, now)
		if selectionErr != nil {
			plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseDegraded, "LifecycleSelection", selectionErr.Error()
			condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleSelectionBlocked", Message: selectionErr.Error()}
			if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
				return reconcile.Result{}, err
			}
			logger.Info("automatic lifecycle selection blocked", "reason", selectionErr.Error())
			return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if started {
			return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}
	renderer := r.Renderer
	if r.RendererFactory != nil {
		renderer = r.RendererFactory(installation)
	}
	if renderer == nil {
		renderer = chart.ValuesRenderer{}
	}
	rendered, err := renderer.Render(ctx, installation.Spec)
	if err == nil && len(rendered.Objects) > 0 {
		var probes []*unstructured.Unstructured
		probes, err = chart.NetworkProbeObjects(installation, rendered.Objects)
		rendered.Objects = append(rendered.Objects, probes...)
	}
	if err != nil {
		plan.Phase = operatorv1alpha1.PhaseDegraded
		plan.Step = "Render"
		plan.Blocking = err.Error()
		if statusErr := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, nil); statusErr != nil {
			return reconcile.Result{}, statusErr
		}
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}
	workload := r.Workload
	if r.WorkloadFactory != nil {
		workload = r.WorkloadFactory(apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID})
	}
	var observation *apply.Observation
	var topologyCondition *metav1.Condition
	if workload == nil || len(rendered.Objects) == 0 {
		if err := r.ensurePlanConfigMap(ctx, installation, plan, rendered); err != nil {
			return reconcile.Result{}, err
		}
	} else {
		plan.Step = "AcquireLease"
		operation := func(operationCtx context.Context) error {
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := r.ensurePlanConfigMap(operationCtx, installation, plan, rendered); err != nil {
				return err
			}
			plan.Step = "ApplyCheckpoint"
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := r.ensureApplyCheckpoint(operationCtx, installation, rendered); err != nil {
				return err
			}
			if localRaft(installation) && r.Topology != nil {
				plan.Step = "VerifyRetainedTopology"
				if err := r.ensureTopology(operationCtx, installation, false); err != nil {
					topologyCondition = &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "TopologyUnverified", Message: err.Error()}
					return err
				}
			}
			plan.Step = "Apply"
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := workload.Apply(operationCtx, rendered.Objects); err != nil {
				return err
			}
			if lifecycleHandoffAuthorizesApply(installation) && installation.Status.Topology.Operation != nil {
				// Consume the lifecycle authorization when the Chart write is
				// accepted. Later observation failures must not make it reusable.
				installation.Status.Topology.Operation.HandoffApplied = true
			}
			plan.Step = "Observe"
			if observer, ok := workload.(apply.Observer); ok {
				observed, err := observer.Observe(operationCtx, rendered.Objects)
				if err != nil {
					return err
				}
				observation = &observed
			}
			if observation != nil && observation.Ready && localRaft(installation) && r.Topology != nil {
				plan.Step = "RecordTopology"
				condition := metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionTrue, Reason: "IdentityVerified", Message: "Live central identities match the retained topology"}
				if err := r.ensureTopology(operationCtx, installation, true); err != nil {
					condition.Status, condition.Reason, condition.Message = metav1.ConditionFalse, "TopologyUnverified", err.Error()
				}
				topologyCondition = &condition
			}
			return nil
		}
		if r.Leases == nil {
			err = fmt.Errorf("workload management requires an operation lease manager")
		} else {
			err = r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
				if installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft && installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly {
					return r.Leases.With(operationCtx, installation, lease.Central, operation)
				}
				return operation(operationCtx)
			})
		}
		if err != nil {
			plan.Phase = operatorv1alpha1.PhaseDegraded
			plan.Blocking = err.Error()
			observation = nil
			logger.Info("installation operation blocked", "step", plan.Step, "reason", err.Error())
		}
	}
	// Health reads do not mutate workloads and must not extend the operation
	// Lease. Only a successful, complete rollout is eligible for verification.
	var ovnReport *health.OVNReport
	if err == nil && observation != nil && observation.Ready && r.OVN != nil {
		report, healthErr := r.OVN.Observe(ctx, installation)
		if healthErr != nil {
			report = health.OVNReport{Reason: "OVNObservationFailed", Message: healthErr.Error()}
		}
		ovnReport = &report
	}
	var runtimeReport *health.RuntimeReport
	if err == nil && observation != nil && observation.Ready && r.Runtime != nil {
		runtimeReport = new(r.Runtime.Observe(ctx, installation))
	}
	var networkReport *health.NetworkReport
	if ovnReport != nil && ovnReport.Ready && runtimeReport != nil && runtimeReport.Northd.Status == metav1.ConditionTrue && runtimeReport.DataPlane.Status == metav1.ConditionTrue && r.Network != nil {
		networkReport = new(r.Network.Observe(ctx, installation, runtimeReport.Nodes))
	}
	if err := r.updateStatus(ctx, installation, plan, now(), observation, ovnReport, runtimeReport, networkReport, topologyCondition); err != nil {
		return reconcile.Result{}, err
	}
	if errors.Is(err, lease.ErrHeld) {
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *KubeOVNInstallationReconciler) beginAutomaticLifecycle(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now func() time.Time) (bool, error) {
	provider, ok := r.Topology.(topology.InventoryProvider)
	if !ok {
		return false, errors.New("automatic lifecycle selection requires a retained topology provider")
	}
	if r.Leases == nil {
		return false, errors.New("automatic lifecycle selection requires operation leases")
	}
	var reference lifecycle.Reference
	var request *lifecycle.Request
	err := r.Leases.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return r.Leases.With(operationCtx, installation, lease.Central, func(centralCtx context.Context) error {
			retained, err := provider.Retained(centralCtx, installation)
			if err != nil {
				return err
			}
			request, err = lifecycle.SelectRemoval(centralCtx, r.Lifecycle.Client, installation, retained)
			if err != nil || request == nil {
				return err
			}
			replicaOnly, err := replicaOnlySpecChange(installation, len(retained.Members))
			if err != nil {
				return err
			}
			if !replicaOnly {
				return errors.New("automatic lifecycle selection requires the desired change to modify central replicas only")
			}
			verified, err := r.Topology.VerifyRetained(centralCtx, installation)
			if err != nil {
				return fmt.Errorf("verify retained topology before automatic selection: %w", err)
			}
			if verified == nil || installation.Status.Topology.Inventory == nil || *verified != *installation.Status.Topology.Inventory {
				return errors.New("automatic lifecycle selection requires a complete fresh topology matching the retained inventory")
			}
			reference, err = r.Lifecycle.Begin(centralCtx, installation, *request)
			return err
		})
	})
	if err != nil {
		return false, err
	}
	if request == nil {
		return false, nil
	}
	installation.Status.Topology.Operation = operationReference(reference, installation.Status.Topology.Inventory)
	plan.Phase, plan.Step, plan.Blocking = operatorv1alpha1.PhaseRecovering, "Lifecycle", "automatic central membership operation admitted; durable execution will resume before Chart apply"
	condition := &metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionFalse, Reason: "LifecycleInProgress", Message: plan.Blocking}
	if err := r.updateStatus(ctx, installation, plan, now(), nil, nil, nil, nil, condition); err != nil {
		return false, err
	}
	return true, nil
}

func (r *KubeOVNInstallationReconciler) ensurePlanConfigMap(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, rendered chart.Rendered) error {
	planBytes, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("marshal plan: %w", err)
	}
	configMap := new(corev1.ConfigMap)
	name := planConfigMapPrefix + installation.Name
	configMap.Name = name
	configMap.Namespace = installation.Namespace
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Name = name
		configMap.Namespace = installation.Namespace
		if configMap.Labels == nil {
			configMap.Labels = map[string]string{}
		}
		configMap.Labels[apply.ManagedByLabel] = apply.ManagedByValue
		configMap.Labels[apply.InstallationLabel] = installation.Name
		configMap.Labels[apply.InstallationUIDLabel] = string(installation.UID)
		configMap.Labels[apply.InstallationNamespaceLabel] = installation.Namespace
		if err := controllerutil.SetControllerReference(installation, configMap, r.Scheme); err != nil {
			return err
		}
		configMap.Data = map[string]string{
			"plan.json":        string(planBytes),
			"values.yaml":      string(rendered.ValuesYAML),
			"chart.source":     rendered.Source,
			"chart.digest":     rendered.Digest,
			"install-mode":     plan.Values["installMode"],
			"central-mode":     plan.Values["centralMode"],
			"central-replicas": plan.Values["centralReplicas"],
			"version":          plan.Values["version"],
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensure plan ConfigMap: %w", err)
	}
	return nil
}

func (r *KubeOVNInstallationReconciler) updateStatus(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, plan planner.Plan, now time.Time, observation *apply.Observation, ovnReport *health.OVNReport, runtimeReport *health.RuntimeReport, networkReport *health.NetworkReport, topologyCondition *metav1.Condition) error {
	status := installation.Status.DeepCopy()
	if status.ApplyCheckpoint == nil {
		status.ObservedVersion = ""
	}
	status.Phase = plan.Phase
	status.ObservedGeneration = installation.Generation
	status.Topology.DesiredCentralReplicas = installation.Spec.Central.Replicas
	status.Topology.ReadyCentralReplicas = 0
	status.Topology.Members = nil
	status.Topology.Databases = nil
	status.Topology.NorthdProcesses = nil
	status.Topology.Nodes = nil
	status.Network = nil
	status.HCP = operatorv1alpha1.HCPStatus{}
	status.Upgrade.Step = plan.Step
	status.Upgrade.CanRollback = false
	readyReason, message := "PlanOnly", "The installation plan is valid; workload readiness has not been observed."
	workloadStatus := metav1.ConditionUnknown
	workloadReason := "NotObserved"
	workloadMessage := message
	degraded := metav1.ConditionFalse
	degradedReason := "PlanValid"
	ovnStatus, ovnReason, ovnMessage := metav1.ConditionUnknown, "NotObserved", "OVN health has not been observed for this rollout"
	northd := metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Northd has not been observed for this rollout"}
	dataPlane := metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Data-plane connections have not been observed for this rollout"}
	network := metav1.Condition{Type: "NetworkReady", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Network probes await healthy workloads, databases and runtime"}
	topologyState := metav1.Condition{Type: "TopologyRecorded", Status: metav1.ConditionUnknown, Reason: "NotObserved", Message: "Complete central identity inventory has not been verified for this rollout"}
	if !localRaft(installation) {
		topologyState.Status, topologyState.Reason, topologyState.Message = metav1.ConditionTrue, "NotRequired", "Local RAFT topology inventory does not apply to this installation"
	} else if topologyCondition != nil {
		topologyState = *topologyCondition
	}
	readyStatus := metav1.ConditionFalse
	if observation != nil {
		status.Topology.ReadyCentralReplicas = observation.ReadyCentralReplicas
		workloadStatus = metav1.ConditionFalse
		workloadReason = "RolloutProgressing"
		readyReason, message = "WorkloadsProgressing", observation.Message
		workloadMessage = observation.Message
		if observation.Ready {
			workloadStatus, workloadReason = metav1.ConditionTrue, "RolloutComplete"
			status.ObservedVersion = installation.Spec.Version
			readyReason = "VerificationPending"
		}
	}
	if ovnReport != nil {
		ovnStatus, ovnReason, ovnMessage = metav1.ConditionFalse, ovnReport.Reason, ovnReport.Message
		status.Topology.Members = slices.Clone(ovnReport.Members)
		status.Topology.Databases = slices.Clone(ovnReport.Databases)
		for index := range status.Topology.Members {
			status.Topology.Members[index].LastObserved = metav1.NewTime(now)
		}
		if ovnReport.Ready {
			ovnStatus = metav1.ConditionTrue
			message = "Workloads and OVN databases are healthy; runtime and network verification are still required"
		} else {
			message = ovnMessage
			if ovnReason == "OVNDatabaseUnhealthy" || ovnReason == "OVNObservationFailed" {
				status.Phase = operatorv1alpha1.PhaseDegraded
				degraded, degradedReason = metav1.ConditionTrue, ovnReason
			}
		}
	}
	if runtimeReport != nil {
		northd, dataPlane = runtimeReport.Northd, runtimeReport.DataPlane
		status.Topology.NorthdProcesses = slices.Clone(runtimeReport.NorthdProcesses)
		status.Topology.Nodes = slices.Clone(runtimeReport.Nodes)
		for index := range status.Topology.NorthdProcesses {
			status.Topology.NorthdProcesses[index].LastObserved = metav1.NewTime(now)
		}
		for index := range status.Topology.Nodes {
			status.Topology.Nodes[index].LastObserved = metav1.NewTime(now)
		}
		for _, condition := range []metav1.Condition{northd, dataPlane} {
			if condition.Status == metav1.ConditionFalse {
				status.Phase = operatorv1alpha1.PhaseDegraded
				if degraded == metav1.ConditionFalse {
					degraded, degradedReason, message = metav1.ConditionTrue, condition.Reason, condition.Message
				}
			}
		}
		if ovnStatus == metav1.ConditionTrue && northd.Status == metav1.ConditionTrue && dataPlane.Status == metav1.ConditionTrue {
			message = "Workloads, OVN databases, northd and data-plane connections are healthy; network probes remain required"
		}
	}
	if plan.Blocking != "" {
		readyReason, message = "SafetyGateBlocked", plan.Blocking
		workloadMessage = message
		degraded, degradedReason = metav1.ConditionTrue, "SafetyGateBlocked"
	}
	if networkReport != nil {
		network = networkReport.Condition
		if network.Status == metav1.ConditionFalse {
			status.Phase = operatorv1alpha1.PhaseDegraded
			degraded, degradedReason, message = metav1.ConditionTrue, network.Reason, network.Message
		}
		if network.Status == metav1.ConditionTrue && networkReport.Evidence != nil {
			status.Network = new(*networkReport.Evidence)
			status.Network.AddressFamilies = slices.Clone(networkReport.Evidence.AddressFamilies)
			status.Network.LastObserved = metav1.NewTime(now)
		}
	}
	if topologyState.Status == metav1.ConditionFalse {
		status.Phase = operatorv1alpha1.PhaseDegraded
		degraded, degradedReason, readyReason, message = metav1.ConditionTrue, "TopologyUnverified", "TopologyUnverified", topologyState.Message
	}
	networkVerified := network.Status == metav1.ConditionTrue && (installation.Spec.Mode == operatorv1alpha1.InstallationModeControlPlaneOnly && network.Reason == "NotRequired" || status.Network != nil && status.Network.ObservedNodes > 0 && status.Network.SuccessfulProbes > 0 && len(status.Network.AddressFamilies) > 0)
	if topologyState.Status == metav1.ConditionTrue && plan.Blocking == "" && workloadStatus == metav1.ConditionTrue && ovnStatus == metav1.ConditionTrue && northd.Status == metav1.ConditionTrue && dataPlane.Status == metav1.ConditionTrue && networkVerified {
		readyStatus, readyReason, message = metav1.ConditionTrue, "VerificationComplete", "Workloads, OVN databases, runtime and configured network probes passed verification"
		status.Phase = operatorv1alpha1.PhaseReady
		status.Upgrade.Step = "Verified"
	}
	for _, condition := range []metav1.Condition{
		{Type: "Ready", Status: readyStatus, Reason: readyReason, Message: message},
		{Type: "Degraded", Status: degraded, Reason: degradedReason, Message: message},
		{Type: "WorkloadsReady", Status: workloadStatus, Reason: workloadReason, Message: workloadMessage},
		{Type: "OVNDBReady", Status: ovnStatus, Reason: ovnReason, Message: ovnMessage},
		northd, dataPlane,
		network, topologyState,
	} {
		condition.ObservedGeneration = installation.Generation
		condition.LastTransitionTime = metav1.NewTime(now)
		setCondition(&status.Conditions, condition)
	}
	return r.Status().Update(ctx, installationWithStatus(installation, *status))
}

func localRaft(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	return installation.Spec.Central.Mode == operatorv1alpha1.CentralModeRaft && installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly && !installation.Spec.HCP.Enabled
}

func (r *KubeOVNInstallationReconciler) ensureTopology(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, initialize bool) error {
	var ref *operatorv1alpha1.CentralTopologyReference
	var observationErr error
	if initialize {
		ref, observationErr = r.Topology.Ensure(ctx, installation)
	} else {
		ref, observationErr = r.Topology.VerifyRetained(ctx, installation)
	}
	if ref == nil {
		if observationErr != nil {
			return observationErr
		}
		if !initialize && installation.Status.Topology.Inventory == nil {
			return nil
		}
		return errors.New("topology provider returned no retained identity reference")
	}
	if ref.Name == "" || ref.UID == "" || ref.Digest == "" {
		return errors.New("topology provider returned an incomplete reference")
	}
	if existing := installation.Status.Topology.Inventory; existing != nil {
		if *existing != *ref {
			return errors.New("retained topology reference changed without lifecycle authorization")
		}
		return observationErr
	}
	if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
		return err
	}
	installation.Status.Topology.Inventory = ref
	if err := r.Status().Update(ctx, installation); err != nil {
		installation.Status.Topology.Inventory = nil
		return fmt.Errorf("persist central topology reference: %w", err)
	}
	return observationErr
}

func newOperationReference(recovery *lifecycle.Recovery, inventory *operatorv1alpha1.CentralTopologyReference) *operatorv1alpha1.CentralOperationReference {
	return operationReference(recovery.Reference, inventory)
}

func operationReference(reference lifecycle.Reference, inventory *operatorv1alpha1.CentralTopologyReference) *operatorv1alpha1.CentralOperationReference {
	result := &operatorv1alpha1.CentralOperationReference{JournalName: reference.Journal.Name, JournalUID: reference.Journal.UID, RequestName: reference.Request.Name, RequestUID: reference.Request.UID}
	if inventory != nil {
		result.Inventory = *inventory
	}
	return result
}

func replicaOnlySpecChange(installation *operatorv1alpha1.KubeOVNInstallation, retainedReplicas int) (bool, error) {
	if installation == nil || installation.Status.ApplyCheckpoint == nil || retainedReplicas < 1 {
		return false, errors.New("automatic lifecycle selection requires an existing apply checkpoint and retained member count")
	}
	spec := installation.Spec
	spec.Central.Replicas = int32(retainedReplicas)
	data, err := stdjson.Marshal(spec, stdjson.Deterministic(true))
	if err != nil {
		return false, fmt.Errorf("hash prior installation spec: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)) == installation.Status.ApplyCheckpoint.SpecHash, nil
}

func (r *KubeOVNInstallationReconciler) ensureApplyCheckpoint(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, rendered chart.Rendered) error {
	specJSON, err := stdjson.Marshal(installation.Spec, stdjson.Deterministic(true))
	if err != nil {
		return err
	}
	manifestJSON, err := stdjson.Marshal(rendered.Objects, stdjson.Deterministic(true))
	if err != nil {
		return err
	}
	checkpoint := operatorv1alpha1.ApplyCheckpointStatus{
		SpecHash:     fmt.Sprintf("%x", sha256.Sum256(specJSON)),
		ManifestHash: fmt.Sprintf("%x", sha256.Sum256(manifestJSON)),
		Version:      installation.Spec.Version,
	}
	if existing := installation.Status.ApplyCheckpoint; existing != nil {
		if *existing != checkpoint {
			if !lifecycleHandoffAuthorizesApply(installation) {
				return fmt.Errorf("installation inputs changed after the first apply checkpoint; a lifecycle executor is required")
			}
			previous := *existing
			installation.Status.ApplyCheckpoint = &checkpoint
			if err := r.Status().Update(ctx, installation); err != nil {
				installation.Status.ApplyCheckpoint = &previous
				return fmt.Errorf("persist lifecycle-authorized apply checkpoint: %w", err)
			}
			return nil
		}
		return nil
	}
	installation.Status.ApplyCheckpoint = &checkpoint
	// Persist before any workload write so a restart or partial apply cannot
	// turn a topology/version change into a fresh installation.
	if err := r.Status().Update(ctx, installation); err != nil {
		installation.Status.ApplyCheckpoint = nil
		return fmt.Errorf("persist apply checkpoint: %w", err)
	}
	return nil
}

func lifecycleHandoffAuthorizesApply(installation *operatorv1alpha1.KubeOVNInstallation) bool {
	if installation == nil || installation.Status.Topology.Operation == nil || installation.Status.Topology.Operation.HandoffApplied || installation.Status.Topology.Operation.CompletionDigest == "" || installation.Status.Topology.Operation.HandoffInventory == nil || installation.Status.Topology.Inventory == nil {
		return false
	}
	return *installation.Status.Topology.Operation.HandoffInventory == *installation.Status.Topology.Inventory
}

func installationWithStatus(in *operatorv1alpha1.KubeOVNInstallation, status operatorv1alpha1.KubeOVNInstallationStatus) *operatorv1alpha1.KubeOVNInstallation {
	copy := in.DeepCopy()
	copy.Status = status
	return copy
}

func setCondition(conditions *[]metav1.Condition, condition metav1.Condition) {
	for index := range *conditions {
		if (*conditions)[index].Type == condition.Type {
			if (*conditions)[index].Status == condition.Status && (*conditions)[index].Reason == condition.Reason && (*conditions)[index].Message == condition.Message {
				condition.LastTransitionTime = (*conditions)[index].LastTransitionTime
			}
			(*conditions)[index] = condition
			return
		}
	}
	*conditions = append(*conditions, condition)
}

type RecoveryRequestReconciler struct {
	client.Client
	Scheme *apiruntime.Scheme
}

func (r *RecoveryRequestReconciler) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).For(&operatorv1alpha1.KubeOVNRecoveryRequest{}).Complete(r)
}

func (r *RecoveryRequestReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	request := new(operatorv1alpha1.KubeOVNRecoveryRequest)
	if err := r.Get(ctx, req.NamespacedName, request); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	condition := metav1.Condition{Type: "Accepted", Status: metav1.ConditionFalse, Reason: "AwaitingSafetyReview", Message: "Recovery requests are recorded for an explicit central admin adapter; no database is wiped automatically.", ObservedGeneration: request.Generation, LastTransitionTime: metav1.Now()}
	request.Status.Phase = "Pending"
	request.Status.Conditions = []metav1.Condition{condition}
	if err := r.Status().Update(ctx, request); err != nil {
		if apierrors.IsConflict(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

var _ reconcile.Reconciler = (*KubeOVNInstallationReconciler)(nil)
var _ reconcile.Reconciler = (*RecoveryRequestReconciler)(nil)
