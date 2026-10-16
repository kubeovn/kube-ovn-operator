package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"fmt"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileWritesPlanWithoutClaimingOperationLeases(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{
		TypeMeta:   metav1.TypeMeta{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("uid-demo"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Clock: func() time.Time { return time.Unix(10, 0) }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	plan := new(corev1.ConfigMap)
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + "demo", Namespace: "default"}, plan); err != nil {
		t.Fatalf("plan ConfigMap was not written: %v", err)
	}
	if plan.Data["install-mode"] != "full" || plan.Data["central-replicas"] != "3" {
		t.Fatalf("unexpected plan data: %#v", plan.Data)
	}
	if !strings.Contains(plan.Data["values.yaml"], "installMode: full") {
		t.Fatalf("rendered chart values are missing from plan: %q", plan.Data["values.yaml"])
	}
	for _, name := range []string{lease.Name(lease.Installation, "demo"), lease.Name(lease.Central, "demo")} {
		if err := client.Get(t.Context(), types.NamespacedName{Name: name, Namespace: "default"}, new(coordinationv1.Lease)); err == nil {
			t.Fatalf("plan-only reconciliation unexpectedly claimed lease %s", name)
		}
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseInstalling || got.Status.ObservedVersion != "" {
		t.Fatalf("unexpected status: %#v", got.Status)
	}
}

func TestReconcileBlocksInvalidSpecBeforeWritingChildren(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "latest", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "invalid", Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + "invalid", Namespace: "default"}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("invalid spec unexpectedly wrote a plan")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: "invalid", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || len(got.Status.Conditions) != 8 {
		t.Fatalf("unexpected invalid status: %#v", got.Status)
	}
}

type ovnProviderFunc func(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error)

func (f ovnProviderFunc) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
	return f(ctx, installation)
}

func TestReconcileOVNHealthDoesNotReplaceNetworkVerificationOrKeepStaleEvidence(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "health", Namespace: "default", UID: "installation", Generation: 1,
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3}}
	observations := 0
	var providerErr error
	provider := ovnProviderFunc(func(ctx context.Context, _ *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
		observations++
		if lease.Check(ctx, lease.Installation) == nil {
			t.Fatal("health observation should run after releasing operation Leases")
		}
		return health.OVNReport{Ready: true, Reason: "OVNDatabasesHealthy", Members: []operatorv1alpha1.CentralMemberStatus{{Database: "OVN_Northbound", ServerID: "server-id", AppliedIndex: 19}}, Databases: []operatorv1alpha1.CentralDatabaseStatus{{Database: "OVN_Northbound", ClusterID: "cluster-id"}}}, providerErr
	})
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload, OVN: provider}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}
	run := func() *operatorv1alpha1.KubeOVNInstallation {
		t.Helper()
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
		got := new(operatorv1alpha1.KubeOVNInstallation)
		if err := kube.Get(t.Context(), req.NamespacedName, got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := run()
	if !meta.IsStatusConditionTrue(got.Status.Conditions, "OVNDBReady") || meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || meta.IsStatusConditionTrue(got.Status.Conditions, "NetworkReady") {
		t.Fatalf("OVN health was confused with network health: %+v", got.Status.Conditions)
	}
	if len(got.Status.Topology.Members) != 1 || got.Status.Topology.Members[0].AppliedIndex != 19 || got.Status.Topology.Members[0].LastObserved.IsZero() {
		t.Fatalf("missing OVN evidence: %+v", got.Status.Topology)
	}
	providerErr = fmt.Errorf("OVN transport unavailable")
	got = run()
	if meta.IsStatusConditionTrue(got.Status.Conditions, "OVNDBReady") || !meta.IsStatusConditionTrue(got.Status.Conditions, "Degraded") || len(got.Status.Topology.Members) != 0 || len(got.Status.Topology.Databases) != 0 {
		t.Fatalf("failed observation preserved healthy evidence: %+v", got.Status)
	}
	providerErr = nil
	workload.observation.Ready = false
	got = run()
	if observations != 2 || meta.FindStatusCondition(got.Status.Conditions, "OVNDBReady").Status != metav1.ConditionUnknown {
		t.Fatalf("incomplete rollout was verified: calls=%d, status=%+v", observations, got.Status)
	}
}

type failingRenderer struct{}

type runtimeProviderFunc func(context.Context, *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport

func (f runtimeProviderFunc) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport {
	return f(ctx, installation)
}

func TestReconcileRuntimeHealthInvalidatesEvidenceWithoutClaimingNetworkReadiness(t *testing.T) {
	for _, failure := range []string{"northd", "data plane", "rollout", "apply", "missing provider"} {
		t.Run(failure, func(t *testing.T) {
			scheme := controllerScheme(t)
			installation := &operatorv1alpha1.KubeOVNInstallation{Name: "runtime", Namespace: "default", UID: "installation", Generation: 1,
				Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
			workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3}}
			report := health.RuntimeReport{
				Northd:          metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue, Reason: "NorthdHealthy"},
				DataPlane:       metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionTrue, Reason: "DataPlaneConnected"},
				NorthdProcesses: []operatorv1alpha1.NorthdProcessStatus{{Pod: "central", State: "active", NBConnected: true, SBConnected: true}},
				Nodes:           []operatorv1alpha1.DataPlaneNodeStatus{{Node: "node", Pod: "ovs", ChassisID: "chassis", SBConnected: true, IntegrationBridgeReady: true}},
			}
			observations := 0
			provider := runtimeProviderFunc(func(ctx context.Context, _ *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport {
				observations++
				if lease.Check(ctx, lease.Installation) == nil {
					t.Fatal("runtime observation extended the operation Lease")
				}
				return report
			})
			reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload, Runtime: provider}
			req := reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}
			run := func() *operatorv1alpha1.KubeOVNInstallation {
				t.Helper()
				if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
					t.Fatal(err)
				}
				got := new(operatorv1alpha1.KubeOVNInstallation)
				if err := kube.Get(t.Context(), req.NamespacedName, got); err != nil {
					t.Fatal(err)
				}
				return got
			}
			got := run()
			if !meta.IsStatusConditionTrue(got.Status.Conditions, "NorthdReady") || !meta.IsStatusConditionTrue(got.Status.Conditions, "DataPlaneReady") || meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || meta.IsStatusConditionTrue(got.Status.Conditions, "NetworkReady") {
				t.Fatalf("runtime health was confused with network health: %+v", got.Status.Conditions)
			}
			if len(got.Status.Topology.NorthdProcesses) != 1 || len(got.Status.Topology.Nodes) != 1 || got.Status.Topology.NorthdProcesses[0].LastObserved.IsZero() || got.Status.Topology.Nodes[0].LastObserved.IsZero() {
				t.Fatalf("missing runtime evidence: %+v", got.Status.Topology)
			}
			switch failure {
			case "northd":
				report.Northd.Status, report.Northd.Reason = metav1.ConditionFalse, "NorthdUnhealthy"
				report.NorthdProcesses = nil
			case "data plane":
				report.DataPlane.Status, report.DataPlane.Reason = metav1.ConditionFalse, "DataPlaneUnhealthy"
				report.Nodes = nil
			case "rollout":
				workload.observation.Ready = false
			case "apply":
				workload.err = fmt.Errorf("apply failed")
			case "missing provider":
				reconciler.Runtime = nil
			}
			got = run()
			if failure == "northd" || failure == "data plane" {
				condition := "NorthdReady"
				stale := len(got.Status.Topology.NorthdProcesses) != 0
				if failure == "data plane" {
					condition, stale = "DataPlaneReady", len(got.Status.Topology.Nodes) != 0
				}
				if observations != 2 || !meta.IsStatusConditionFalse(got.Status.Conditions, condition) || !meta.IsStatusConditionTrue(got.Status.Conditions, "Degraded") || stale {
					t.Fatalf("runtime failure retained healthy evidence: %+v", got.Status)
				}
			} else if observations != 1 || meta.FindStatusCondition(got.Status.Conditions, "NorthdReady").Status != metav1.ConditionUnknown || meta.FindStatusCondition(got.Status.Conditions, "DataPlaneReady").Status != metav1.ConditionUnknown || len(got.Status.Topology.NorthdProcesses) != 0 || len(got.Status.Topology.Nodes) != 0 {
				t.Fatalf("unobserved runtime retained evidence: calls=%d, status=%+v", observations, got.Status)
			}
		})
	}
}

func (failingRenderer) Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return chart.Rendered{}, fmt.Errorf("chart dry-run rejected rendered objects")
}

func TestReconcileBlocksWhenRendererFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "render-failure", Namespace: "default", Generation: 1}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Renderer: failingRenderer{}}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + installation.Name, Namespace: installation.Namespace}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("renderer failure unexpectedly wrote a plan")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || got.Status.Conditions[0].Reason != "SafetyGateBlocked" {
		t.Fatalf("unexpected renderer failure status: %#v", got.Status)
	}
}

type objectRenderer struct{}

func (objectRenderer) Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return chart.Rendered{
		ValuesYAML: []byte("installMode: full\n"),
		Objects: []*unstructured.Unstructured{{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "managed-workload",
				"namespace": "default",
			},
		}}},
	}, nil
}

type recordingWorkload struct {
	dryRunCalls int
	applyCalls  int
	err         error
}

func (r *recordingWorkload) DryRun(context.Context, []*unstructured.Unstructured) error {
	r.dryRunCalls++
	return nil
}

func (r *recordingWorkload) Apply(context.Context, []*unstructured.Unstructured) error {
	r.applyCalls++
	return r.err
}

var _ apply.ApplyClient = (*recordingWorkload)(nil)

func TestReconcileUsesInjectedWorkloadAdapter(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "managed", Namespace: "default", UID: types.UID("uid-managed"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := new(recordingWorkload)
	var gotIdentity apply.InstallationIdentity
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Leases: lease.New(client), Renderer: objectRenderer{}, WorkloadFactory: func(identity apply.InstallationIdentity) apply.ApplyClient {
		gotIdentity = identity
		return workload
	}}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if workload.applyCalls != 1 || workload.dryRunCalls != 0 {
		t.Fatalf("unexpected workload adapter calls: %#v", workload)
	}
	if gotIdentity.Namespace != installation.Namespace || gotIdentity.Name != installation.Name || gotIdentity.UID != installation.UID {
		t.Fatalf("unexpected workload identity: %#v", gotIdentity)
	}
}

func TestReconcileRecordsWorkloadApplyFailure(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "apply-failure", Namespace: "default", UID: types.UID("uid-apply-failure"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &recordingWorkload{err: fmt.Errorf("ownership conflict")}
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Leases: lease.New(client), Renderer: objectRenderer{}, Workload: workload}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || got.Status.Upgrade.Step != "Apply" {
		t.Fatalf("unexpected apply failure status: %#v", got.Status)
	}
}

func controllerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

type observedWorkload struct {
	recordingWorkload
	observation apply.Observation
}

func (w *observedWorkload) Observe(context.Context, []*unstructured.Unstructured) (apply.Observation, error) {
	return w.observation, nil
}

type failingObserveWorkload struct {
	recordingWorkload
	observeErr error
}

func (w *failingObserveWorkload) Observe(context.Context, []*unstructured.Unstructured) (apply.Observation, error) {
	return apply.Observation{Ready: true}, w.observeErr
}

func TestReconcileObservesRolloutAndPinsFirstApplyAcrossRestart(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "checkpoint", Namespace: "default", UID: "installation-uid", Generation: 1,
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3, Message: "Rollout completed"}}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ApplyCheckpoint == nil || got.Status.ObservedVersion != "v1.17.0" || got.Status.Topology.ReadyCentralReplicas != 3 {
		t.Fatalf("missing observed evidence: %+v", got.Status)
	}
	if meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || !meta.IsStatusConditionTrue(got.Status.Conditions, "WorkloadsReady") {
		t.Fatalf("rollout confused with network readiness: %+v", got.Status.Conditions)
	}
	// A new reconciler must recover the checkpoint from Kubernetes, including
	// when no process-local state survives.
	reconciler = &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if workload.applyCalls != 2 {
		t.Fatal("same inputs were not reconciled after restart")
	}
	if err := kube.Get(t.Context(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Central.Replicas = 5
	if err := kube.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if workload.applyCalls != 2 {
		t.Fatal("topology change bypassed checkpoint")
	}
	if err := kube.Get(t.Context(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Upgrade.Step != "ApplyCheckpoint" || !meta.IsStatusConditionTrue(got.Status.Conditions, "Degraded") {
		t.Fatalf("checkpoint rejection not reported: %+v", got.Status)
	}
}

func TestLifecycleHandoffAuthorizesOnlyTheNextApplyCheckpoint(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "handoff", Namespace: "default", UID: "installation-uid", Generation: 2}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5}}}
	previous := installation.Spec
	previous.Central.Replicas = 3
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "topology", UID: types.UID("topology-uid"), Digest: "sha256:" + strings.Repeat("a", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), ManifestHash: "old-manifest", Version: previous.Version}
	installation.Status.Topology.Inventory = ref
	installation.Status.Topology.Operation = &operatorv1alpha1.CentralOperationReference{JournalName: "journal", JournalUID: types.UID("journal-uid"), RequestName: "request", RequestUID: types.UID("request-uid"), Inventory: *ref, HandoffInventory: ref, CompletionDigest: "sha256:" + strings.Repeat("b", 64)}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	rendered := chart.Rendered{Objects: []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "next", "namespace": "default"}}}}}
	if err := reconciler.ensureApplyCheckpoint(t.Context(), installation, rendered); err != nil {
		t.Fatal(err)
	}
	if installation.Status.ApplyCheckpoint.SpecHash == fmt.Sprintf("%x", sha256.Sum256(previousJSON)) {
		t.Fatal("lifecycle handoff did not replace the old checkpoint")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ApplyCheckpoint == nil || got.Status.ApplyCheckpoint.SpecHash != installation.Status.ApplyCheckpoint.SpecHash {
		t.Fatalf("checkpoint replacement was not persisted: %+v", got.Status.ApplyCheckpoint)
	}
	installation.Status.Topology.Operation = nil
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: "old", ManifestHash: "old", Version: previous.Version}
	if err := reconciler.ensureApplyCheckpoint(t.Context(), installation, rendered); err == nil {
		t.Fatal("checkpoint changed without a lifecycle handoff")
	}
	installation.Status.Topology.Operation = &operatorv1alpha1.CentralOperationReference{HandoffInventory: ref, CompletionDigest: "sha256:" + strings.Repeat("b", 64), HandoffApplied: true}
	if err := reconciler.ensureApplyCheckpoint(t.Context(), installation, rendered); err == nil {
		t.Fatal("checkpoint changed after the lifecycle handoff was already applied")
	}
}

func TestLifecycleHandoffIsConsumedAfterApplyBeforeObservation(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "handoff-observe", Namespace: "default", UID: types.UID("installation-uid"), Generation: 2}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5}}}
	previous := installation.Spec
	previous.Central.Replicas = 3
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "topology", UID: types.UID("topology-uid"), Digest: "sha256:" + strings.Repeat("a", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), ManifestHash: "old-manifest", Version: previous.Version}
	installation.Status.Topology.Inventory = ref
	installation.Status.Topology.Operation = &operatorv1alpha1.CentralOperationReference{Inventory: *ref, HandoffInventory: ref, CompletionDigest: "sha256:" + strings.Repeat("b", 64)}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &failingObserveWorkload{observeErr: fmt.Errorf("observation failed")}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)}); err != nil {
		t.Fatal(err)
	}
	if workload.applyCalls != 1 {
		t.Fatalf("expected one accepted Chart apply, got %d", workload.applyCalls)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Topology.Operation == nil || !got.Status.Topology.Operation.HandoffApplied {
		t.Fatalf("successful apply did not consume handoff authorization: %+v", got.Status.Topology.Operation)
	}
}

func TestUpgradeAuthorizationBindsTheCurrentVersionAndTopology(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "upgrade-checkpoint", Namespace: "default", UID: types.UID("upgrade-uid"), Generation: 2}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.18.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
	}}
	installation.Status.ObservedVersion = "v1.17.0"
	previous := installation.Spec
	previous.Version = installation.Status.ObservedVersion
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "topology", UID: types.UID("topology-uid"), Digest: "sha256:" + strings.Repeat("a", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version}
	installation.Status.Topology.Inventory = ref
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{ID: "pair-id", Version: previous.Version, TopologyDigest: ref.Digest}
	installation.Status.Upgrade = operatorv1alpha1.UpgradeStatus{BackupID: "pair-id", TargetVersion: installation.Spec.Version, CheckpointAuthorized: true}
	if !upgradeAuthorizesApply(installation) {
		t.Fatal("valid upgrade backup pair did not authorize the version checkpoint")
	}

	installation.Status.Upgrade.TargetVersion = "v1.19.0"
	if upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was reused for a different target version")
	}
	installation.Status.Upgrade.TargetVersion = installation.Spec.Version
	installation.Status.LastBackup.TopologyDigest = "sha256:" + strings.Repeat("b", 64)
	if upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was reused for a different retained topology")
	}
	installation.Status.LastBackup.TopologyDigest = ref.Digest
	installation.Status.Upgrade.CheckpointAuthorized = false
	if upgradeAuthorizesApply(installation) {
		t.Fatal("consumed authorization remained usable")
	}
}

func TestUpgradeCheckpointAcceptsOnlyVersionChange(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "upgrade-checkpoint", Namespace: "default", UID: types.UID("upgrade-uid"), Generation: 2}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.18.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
	}}
	installation.Status.ObservedVersion = "v1.17.0"
	previous := installation.Spec
	previous.Version = installation.Status.ObservedVersion
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "topology", UID: types.UID("topology-uid"), Digest: "sha256:" + strings.Repeat("a", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), ManifestHash: "old-manifest", Version: previous.Version}
	installation.Status.Topology.Inventory = ref
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{ID: "pair-id", Version: previous.Version, TopologyDigest: ref.Digest}
	installation.Status.Upgrade = operatorv1alpha1.UpgradeStatus{BackupID: "pair-id", TargetVersion: installation.Spec.Version, CheckpointAuthorized: true}
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t)}
	rendered := chart.Rendered{Objects: []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "next", "namespace": "default"}}}}}
	if err := reconciler.ensureApplyCheckpoint(t.Context(), installation, rendered); err != nil {
		t.Fatal(err)
	}
	if installation.Status.ApplyCheckpoint.Version != installation.Spec.Version {
		t.Fatalf("upgrade checkpoint retained the old version: %+v", installation.Status.ApplyCheckpoint)
	}
	installation.Spec.Central.Replicas = 5
	if upgradeOnlySpecChange(installation) {
		t.Fatal("member change was accepted as a version-only upgrade")
	}
}

func TestRecordLifecycleBackupStatusRequiresAVerifiedPair(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	first := time.Unix(10, 0).UTC()
	recovery := &lifecycle.Recovery{
		Request: lifecycle.Request{Databases: []lifecycle.Inventory{{Database: "OVN_Northbound"}, {Database: "OVN_Southbound"}}},
		State:   lifecycle.State{Databases: [2]lifecycle.DatabaseState{{BackupRef: "sha256:" + strings.Repeat("a", 64), CapturedAt: first}, {BackupRef: "sha256:" + strings.Repeat("b", 64), CapturedAt: first.Add(time.Second)}}},
	}
	recordLifecycleBackupStatus(installation, recovery)
	if installation.Status.LastBackup == nil || len(installation.Status.LastBackup.DatabaseRefs) != 2 || installation.Status.LastBackup.CompletedAt.Time != first.Add(time.Second) {
		t.Fatalf("verified backup pair was not recorded: %+v", installation.Status.LastBackup)
	}
	if installation.Status.LastBackup.ID == "" {
		t.Fatal("backup pair has no stable ID")
	}
	copy := installation.Status.DeepCopy()
	copy.LastBackup.DatabaseRefs["OVN_Northbound"] = "sha256:" + strings.Repeat("c", 64)
	if installation.Status.LastBackup.DatabaseRefs["OVN_Northbound"] == copy.LastBackup.DatabaseRefs["OVN_Northbound"] {
		t.Fatal("backup status deep copy shares database references")
	}
	installation.Status.LastBackup = nil
	recovery.State.Databases[1].BackupRef = ""
	recordLifecycleBackupStatus(installation, recovery)
	if installation.Status.LastBackup != nil {
		t.Fatal("partial backup pair was exposed as a completed checkpoint")
	}
}

func TestReplicaOnlySpecChangePinsTheRetainedMemberCount(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5}}}
	previous := installation.Spec
	previous.Central.Replicas = 3
	data, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(data))}
	ok, err := replicaOnlySpecChange(installation, 3)
	if err != nil || !ok {
		t.Fatalf("replica-only transition was rejected: %v", err)
	}
	installation.Spec.Mode = operatorv1alpha1.InstallationModeControlPlaneOnly
	ok, err = replicaOnlySpecChange(installation, 3)
	if err != nil || ok {
		t.Fatalf("unrelated spec change was admitted: ok=%v err=%v", ok, err)
	}
}

func clientKey(object client.Object) client.ObjectKey {
	return client.ObjectKeyFromObject(object)
}
