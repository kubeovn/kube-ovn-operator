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
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
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

func TestReconcileBlocksInitialMemberRequestsBeforeChartRender(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "initial-members", Namespace: "default", UID: types.UID("initial-members-uid"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{
				Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5, DynamicMembership: true,
				MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
					Name: "member-new", NodeName: "node-new", ClaimName: "claim-new", VolumeName: "volume-new",
					Image:             "example/central@sha256:" + strings.Repeat("a", 64),
					NorthboundAddress: "tcp:192.0.2.99:6643", SouthboundAddress: "tcp:192.0.2.99:6644",
					NorthboundClientAddress: "tcp:192.0.2.99:6641", SouthboundClientAddress: "tcp:192.0.2.99:6642",
				}},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return time.Unix(10, 0) }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + installation.Name, Namespace: installation.Namespace}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("initial member requests unexpectedly rendered a Chart plan")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || !strings.Contains(got.Status.Conditions[0].Message, "existing retained topology") {
		t.Fatalf("initial member requests were not fenced clearly: %+v", got.Status)
	}

	got.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Name: "inventory", UID: types.UID("inventory-uid"), Digest: "sha256:" + strings.Repeat("b", 64)}
	if err := validateMemberRequestLifecycle(got); err != nil {
		t.Fatalf("member requests remained blocked after retaining a topology: %v", err)
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

type adoptingWorkload struct {
	recordingWorkload
	adoptCalls int
	adoptCount int
	selector   map[string]string
	adoptErr   error
}

func (w *adoptingWorkload) Adopt(_ context.Context, objects []*unstructured.Unstructured, selector map[string]string) error {
	w.adoptCalls++
	w.adoptCount = len(objects)
	w.selector = selector
	return w.adoptErr
}

var _ apply.Adopter = (*adoptingWorkload)(nil)

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

func TestReconcileAdoptsBeforeRegularApply(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "adopt", Namespace: "default", UID: types.UID("uid-adopt"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central:  operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
			Adoption: operatorv1alpha1.AdoptionSpec{Enabled: true, Selector: map[string]string{"app": "kube-ovn"}},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := new(adoptingWorkload)
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Leases: lease.New(client), Renderer: objectRenderer{}, WorkloadFactory: func(apply.InstallationIdentity) apply.ApplyClient {
		return workload
	}}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if workload.adoptCalls != 1 || workload.adoptCount != 1 || workload.applyCalls != 1 || workload.selector["app"] != "kube-ovn" {
		t.Fatalf("unexpected adoption/apply calls: %#v", workload)
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
	installation.Spec.Upgrade.Backup.Destination = "file:///var/lib/kube-ovn/backups"
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
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("b", 64), "OVN_Southbound": "sha256:" + strings.Repeat("c", 64)}
	refsJSON, err := stdjson.Marshal(refs, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	pairID := fmt.Sprintf("sha256:%x", sha256.Sum256(refsJSON))
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{ID: pairID, DatabaseRefs: refs, Version: previous.Version, TopologyDigest: ref.Digest, CompletedAt: metav1.NewTime(time.Unix(10, 0))}
	installation.Status.LastBackup.Destination = installation.Spec.Upgrade.Backup.Destination
	installation.Status.Upgrade = operatorv1alpha1.UpgradeStatus{BackupID: pairID, TargetVersion: installation.Spec.Version, CheckpointAuthorized: true, Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{ID: pairID, DatabaseRefs: refs, SourceVersion: previous.Version, TargetVersion: installation.Spec.Version, Destination: installation.Spec.Upgrade.Backup.Destination, TopologyDigest: ref.Digest, CapturedAt: metav1.NewTime(time.Unix(10, 0)), Phase: operatorv1alpha1.UpgradeCheckpointVerified}}
	if !upgradeAuthorizesApply(installation) {
		t.Fatal("valid upgrade backup pair did not authorize the version checkpoint")
	}

	installation.Status.Upgrade.Checkpoint.TargetVersion = "v1.19.0"
	if upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was reused for a different target version")
	}
	installation.Status.Upgrade.Checkpoint.TargetVersion = installation.Spec.Version
	installation.Spec.Upgrade.Backup.Destination = "file:///var/lib/kube-ovn/other"
	if upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was reused for a different backup destination")
	}
	installation.Spec.Upgrade.Backup.Destination = "file:///var/lib/kube-ovn/backups"
	installation.Status.LastBackup.TopologyDigest = "sha256:" + strings.Repeat("d", 64)
	if !upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was coupled to a mutable LastBackup summary")
	}
	installation.Status.Upgrade.Checkpoint.TopologyDigest = "sha256:" + strings.Repeat("e", 64)
	if upgradeAuthorizesApply(installation) {
		t.Fatal("authorization was reused for a different retained topology")
	}
	installation.Status.Upgrade.Checkpoint.TopologyDigest = ref.Digest
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
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("b", 64), "OVN_Southbound": "sha256:" + strings.Repeat("c", 64)}
	refsJSON, err := stdjson.Marshal(refs, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	pairID := fmt.Sprintf("sha256:%x", sha256.Sum256(refsJSON))
	installation.Status.LastBackup = &operatorv1alpha1.BackupStatus{ID: pairID, DatabaseRefs: refs, Version: previous.Version, TopologyDigest: ref.Digest, CompletedAt: metav1.NewTime(time.Unix(10, 0))}
	installation.Status.Upgrade = operatorv1alpha1.UpgradeStatus{BackupID: pairID, TargetVersion: installation.Spec.Version, CheckpointAuthorized: true, Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{ID: pairID, DatabaseRefs: refs, SourceVersion: previous.Version, TargetVersion: installation.Spec.Version, Destination: "", TopologyDigest: ref.Digest, CapturedAt: metav1.NewTime(time.Unix(10, 0)), Phase: operatorv1alpha1.UpgradeCheckpointVerified}}
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

func TestUpgradeCheckpointPersistsCaptureAuthorizationAndApplyPhases(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "upgrade-phases", Namespace: "default", UID: types.UID("upgrade-phases-uid"), Generation: 2}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.18.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
	}}
	installation.Spec.Upgrade.Backup.Destination = "file:///var/lib/kube-ovn/backups"
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
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("b", 64), "OVN_Southbound": "sha256:" + strings.Repeat("c", 64)}
	refsJSON, err := stdjson.Marshal(refs, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	pairID := fmt.Sprintf("sha256:%x", sha256.Sum256(refsJSON))
	backup := &operatorv1alpha1.BackupStatus{ID: pairID, DatabaseRefs: refs, Destination: "file:///var/lib/kube-ovn/backups", Version: previous.Version, TopologyDigest: ref.Digest, CompletedAt: metav1.NewTime(time.Unix(10, 0))}
	checkpoint := upgradeCheckpointFromBackup(backup, installation.Spec.Version)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t)}

	if err := reconciler.persistUpgradeCheckpoint(t.Context(), installation, checkpoint, backup, false); err != nil {
		t.Fatal(err)
	}
	if got := installation.Status.Upgrade.Checkpoint; got == nil || got.Phase != operatorv1alpha1.UpgradeCheckpointVerified || installation.Status.Upgrade.CheckpointAuthorized {
		t.Fatalf("capture phase was not persisted as an un-authorized verified pair: %+v", installation.Status.Upgrade)
	}
	copy := installation.Status.DeepCopy()
	copy.Upgrade.Checkpoint.DatabaseRefs["OVN_Northbound"] = "changed"
	if installation.Status.Upgrade.Checkpoint.DatabaseRefs["OVN_Northbound"] == "changed" {
		t.Fatal("upgrade checkpoint deep copy shares database references")
	}
	if upgradeAuthorizesApply(installation) {
		t.Fatal("verified pair without explicit authorization was usable")
	}
	if err := reconciler.persistUpgradeCheckpoint(t.Context(), installation, checkpoint, backup, true); err != nil {
		t.Fatal(err)
	}
	if !upgradeAuthorizesApply(installation) {
		t.Fatal("authorized verified pair was not usable")
	}
	if err := reconciler.beginUpgradeApply(t.Context(), installation); err != nil {
		t.Fatal(err)
	}
	if installation.Status.Upgrade.Checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointApplying || installation.Status.Upgrade.CheckpointAuthorized {
		t.Fatalf("apply phase was not durably fenced: %+v", installation.Status.Upgrade)
	}
	if !upgradeAuthorizesApply(installation) {
		t.Fatal("in-flight apply could not recover its idempotent target")
	}
	if err := reconciler.markUpgradeCheckpointApplied(t.Context(), installation, time.Unix(20, 0)); err != nil {
		t.Fatal(err)
	}
	if installation.Status.Upgrade.Checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointApplied || upgradeAuthorizesApply(installation) {
		t.Fatalf("applied checkpoint remained reusable: %+v", installation.Status.Upgrade)
	}
	if err := reconciler.failUpgradeCheckpoint(t.Context(), installation, "verification failed"); err != nil {
		t.Fatal(err)
	}
	if installation.Status.Upgrade.Checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointFailed || installation.Status.Upgrade.Checkpoint.Reason != "verification failed" || upgradeAuthorizesApply(installation) {
		t.Fatalf("failed checkpoint was not terminally fenced: %+v", installation.Status.Upgrade)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Upgrade.Checkpoint == nil || got.Status.Upgrade.Checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointFailed {
		t.Fatalf("checkpoint phase was not persisted in the API: %+v", got.Status.Upgrade)
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

func TestReplicaOnlySpecChangeAllowsASecondTransitionAfterTheFirstHandoff(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}}}
	data, err := stdjson.Marshal(installation.Spec, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(data))}
	// The desired spec has already been accepted by the first handoff, while
	// the retained topology still has one intermediate member to retire.
	ok, err := replicaOnlySpecChange(installation, 4)
	if err != nil || !ok {
		t.Fatalf("second replica transition was rejected: %v", err)
	}
	installation.Spec.Version = "v1.18.0"
	ok, err = replicaOnlySpecChange(installation, 4)
	if err != nil || ok {
		t.Fatalf("version change was admitted during a second transition: ok=%v err=%v", ok, err)
	}
}

func TestReplicaOnlySpecChangeAllowsPreparedSlotsWithTheFirstReplicaIncrease(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{
			Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5, DynamicMembership: true,
			MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member-new"}},
		},
	}}
	previous := installation.Spec
	previous.Central.Replicas = 3
	previous.Central.MemberRequests = nil
	data, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(data))}
	allowed, err := replicaOnlySpecChange(installation, 3)
	if err != nil || !allowed {
		t.Fatalf("prepared slots plus the first replica increase were rejected: allowed=%v err=%v", allowed, err)
	}
}

func TestValidateLifecycleMemberRequestPinsCurrentSpec(t *testing.T) {
	image := "example/central@sha256:" + strings.Repeat("a", 64)
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{Central: operatorv1alpha1.CentralSpec{MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
		Name: "member-new", NodeName: "node-new", ClaimName: "claim-new", VolumeName: "volume-new", Image: image,
		NorthboundAddress: "tcp:192.0.2.99:6643", SouthboundAddress: "tcp:192.0.2.99:6644",
		NorthboundClientAddress: "tcp:192.0.2.99:6641", SouthboundClientAddress: "tcp:192.0.2.99:6642",
	}}}}}
	request := lifecycle.Request{Add: &lifecycle.Addition{
		Workload: workload.Request{
			Node: workload.Reference{Name: "node-new"}, Claim: workload.Reference{Name: "claim-new"}, Volume: workload.Reference{Name: "volume-new"}, Image: image,
			Preparation: prepare.Plan{MemberName: "member-new", Databases: []prepare.Database{{Name: "OVN_Northbound", LocalAddress: "tcp:192.0.2.99:6643"}, {Name: "OVN_Southbound", LocalAddress: "tcp:192.0.2.99:6644"}}},
		}, NorthboundAddress: "tcp:192.0.2.99:6641", SouthboundAddress: "tcp:192.0.2.99:6642",
	}}
	if err := validateLifecycleMemberRequest(installation, request); err != nil {
		t.Fatalf("matching lifecycle member request was rejected: %v", err)
	}
	installation.Spec.Central.MemberRequests[0].NodeName = "other-node"
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "changed after operation admission") {
		t.Fatalf("member identity drift was not rejected: %v", err)
	}
	installation.Spec.Central.MemberRequests = nil
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "was removed") {
		t.Fatalf("member request removal was not rejected: %v", err)
	}
}

type lifecycleCoordinatorFake struct {
	reader    client.Reader
	begin     int
	request   lifecycle.Request
	reference lifecycle.Reference
}

func (f *lifecycleCoordinatorFake) Reader() client.Reader { return f.reader }

func (f *lifecycleCoordinatorFake) SetStoreFactory(func(*operatorv1alpha1.KubeOVNInstallation) (backup.Store, error)) {
}

func (f *lifecycleCoordinatorFake) Begin(_ context.Context, _ *operatorv1alpha1.KubeOVNInstallation, request lifecycle.Request) (lifecycle.Reference, error) {
	f.begin++
	f.request = request
	return f.reference, nil
}

func (f *lifecycleCoordinatorFake) Resume(context.Context, *operatorv1alpha1.KubeOVNInstallation, *operatorv1alpha1.CentralOperationReference) (*lifecycle.Recovery, error) {
	return nil, nil
}

func (f *lifecycleCoordinatorFake) ProtectedBackupRefs(context.Context, *operatorv1alpha1.KubeOVNInstallation, *operatorv1alpha1.CentralOperationReference) ([]string, error) {
	return nil, nil
}

func (f *lifecycleCoordinatorFake) CleanupBackups(context.Context, *operatorv1alpha1.KubeOVNInstallation, backup.RetentionPolicy, []string, time.Time) (backup.RetentionResult, error) {
	return backup.RetentionResult{}, nil
}

func (f *lifecycleCoordinatorFake) CaptureBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*operatorv1alpha1.BackupStatus, error) {
	return nil, nil
}

func (f *lifecycleCoordinatorFake) VerifyBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *operatorv1alpha1.BackupStatus) error {
	return nil
}

var _ lifecycleCoordinator = (*lifecycleCoordinatorFake)(nil)

type lifecycleTopologyFake struct {
	ref      *operatorv1alpha1.CentralTopologyReference
	retained health.CentralTopology
}

func (f *lifecycleTopologyFake) Ensure(context.Context, *operatorv1alpha1.KubeOVNInstallation) (*operatorv1alpha1.CentralTopologyReference, error) {
	return f.ref, nil
}

func (f *lifecycleTopologyFake) VerifyRetained(context.Context, *operatorv1alpha1.KubeOVNInstallation) (*operatorv1alpha1.CentralTopologyReference, error) {
	return f.ref, nil
}

func (f *lifecycleTopologyFake) Retained(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.CentralTopology, error) {
	return f.retained, nil
}

var _ topology.Provider = (*lifecycleTopologyFake)(nil)
var _ topology.InventoryProvider = (*lifecycleTopologyFake)(nil)

func TestReconcileSelectsAndPersistsAnAdditionFromLiveMemberObjects(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "addition", Namespace: "default", UID: types.UID("addition-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
				Name: "member-new", NodeName: "node-new", ClaimName: "claim-new", VolumeName: "volume-new",
				Image:             "example/central@sha256:" + strings.Repeat("a", 64),
				NorthboundAddress: "tcp:192.0.2.99:6643", SouthboundAddress: "tcp:192.0.2.99:6644",
				NorthboundClientAddress: "tcp:192.0.2.99:6641", SouthboundClientAddress: "tcp:192.0.2.99:6642",
			}}},
		},
	}
	previous := installation.Spec
	previous.Central.Replicas = 3
	previous.Central.MemberRequests = nil
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "inventory", UID: types.UID("inventory-uid"), Digest: "sha256:" + strings.Repeat("b", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version}
	installation.Status.Topology.Inventory = ref

	retained := additionTopologyFixture()
	objects := make([]client.Object, 0, 4)
	objects = append(objects,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: types.UID("node-new-uid")}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.99"}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-new", Namespace: "default", UID: types.UID("claim-new-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "volume-new"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "volume-new", UID: types.UID("volume-new-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "claim-new", Namespace: "default", UID: types.UID("claim-new-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	)
	objects = append(objects, installation)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(objects...).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube, reference: lifecycle.Reference{Journal: workload.Reference{Name: "journal", UID: types.UID("journal-uid")}, Request: workload.Reference{Name: "request", UID: types.UID("request-uid")}}}
	topologyFake := &lifecycleTopologyFake{ref: ref, retained: retained}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Leases: lease.New(kube), Topology: topologyFake, Lifecycle: lifecycleFake}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 10*time.Second || lifecycleFake.begin != 1 || lifecycleFake.request.Add == nil || lifecycleFake.request.Add.Workload.Preparation.MemberName != "member-new" {
		t.Fatalf("default controller did not admit the expected addition: result=%+v begin=%d request=%+v", result, lifecycleFake.begin, lifecycleFake.request)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Topology.Operation == nil || got.Status.Topology.Operation.JournalUID != types.UID("journal-uid") || got.Status.Topology.Operation.RequestUID != types.UID("request-uid") {
		t.Fatalf("addition operation was not durably recorded: %+v", got.Status.Topology.Operation)
	}
}

func additionTopologyFixture() health.CentralTopology {
	topology := health.CentralTopology{Members: make([]health.CentralIdentity, 0, 3)}
	for index := 0; index < 3; index++ {
		member := health.CentralIdentity{
			Node:        health.ObjectIdentity{Name: fmt.Sprintf("node-%d", index), UID: types.UID(fmt.Sprintf("node-uid-%d", index))},
			Workload:    health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: fmt.Sprintf("central-%d", index), UID: types.UID(fmt.Sprintf("workload-uid-%d", index))}, Kind: "Deployment"},
			Storage:     []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: fmt.Sprintf("/var/lib/ovn/%d", index)}},
			Pod:         health.ObjectIdentity{Name: fmt.Sprintf("central-%d-pod", index), UID: types.UID(fmt.Sprintf("pod-uid-%d", index))},
			ContainerID: fmt.Sprintf("containerd://central-%d", index),
		}
		for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
			member.Databases[database] = health.DatabaseIdentity{
				Database:      name,
				ClusterID:     fmt.Sprintf("1111111%d-2222-3333-4444-555555555555", database),
				ServerID:      fmt.Sprintf("0000000%d-1111-2222-3333-444444444444", index+1),
				Address:       fmt.Sprintf("tcp:192.0.2.%d:%d", index+1, 6643+database),
				ClientAddress: fmt.Sprintf("tcp:192.0.2.%d:%d", index+1, 6641+database),
			}
		}
		topology.Members = append(topology.Members, member)
	}
	return topology
}

func clientKey(object client.Object) client.ObjectKey {
	return client.ObjectKeyFromObject(object)
}
