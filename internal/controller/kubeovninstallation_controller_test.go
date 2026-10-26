package controller

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"fmt"
	"slices"
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
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/topology"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileWritesPlanWithoutClaimingOperationLeases(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
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

func TestUpdateStatusEmitsPhaseAndReadyEvents(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "events", Namespace: "default", UID: types.UID("events-uid"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeSingle, Replicas: 1, Storage: operatorv1alpha1.StorageSpec{Size: "10Gi"}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	recorder := record.NewFakeRecorder(4)
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Recorder: recorder}
	if err := reconciler.updateStatus(t.Context(), installation, planner.Plan{Phase: operatorv1alpha1.PhaseInstalling, Step: "Render"}, time.Unix(10, 0), nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	events := []string{<-recorder.Events, <-recorder.Events}
	if !slices.ContainsFunc(events, func(event string) bool { return strings.Contains(event, "PhaseChanged") }) || !slices.ContainsFunc(events, func(event string) bool { return strings.Contains(event, "ReadyChanged") }) {
		t.Fatalf("status transition events were not emitted: %v", events)
	}
}

func TestMapNodeToInstallationsWakesMatchingPlacements(t *testing.T) {
	scheme := controllerScheme(t)
	central := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("central-uid")},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, Placement: operatorv1alpha1.PlacementSpec{NodeSelector: map[string]string{"role": "control"}}},
		},
	}
	dataPlane := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant", UID: types.UID("data-uid")},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
			Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 1},
			DataPlane: operatorv1alpha1.DataPlaneSpec{NodeSelector: map[string]string{"role": "worker"}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(central, dataPlane).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	control := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-1", Labels: map[string]string{"role": "control"}}}
	requests := reconciler.mapNodeToInstallations(t.Context(), control)
	if len(requests) != 1 || requests[0].Namespace != "default" || requests[0].Name != "central" {
		t.Fatalf("unexpected control-plane mapping: %#v", requests)
	}
	worker := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Labels: map[string]string{"role": "worker"}}}
	requests = reconciler.mapNodeToInstallations(t.Context(), worker)
	if len(requests) != 1 || requests[0].Namespace != "tenant" || requests[0].Name != "data" {
		t.Fatalf("unexpected data-plane mapping: %#v", requests)
	}
}

func TestMapNodeToInstallationsWakesRetainedNodeAfterPlacementChange(t *testing.T) {
	scheme := controllerScheme(t)
	central := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("central-uid")},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, Placement: operatorv1alpha1.PlacementSpec{NodeSelector: map[string]string{"role": "control"}}},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{Topology: operatorv1alpha1.TopologyStatus{
			Members: []operatorv1alpha1.CentralMemberStatus{{Node: "control-1"}},
		}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(central).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	// The node no longer matches the selector, as happens when it is drained or
	// relabeled. Its retained status identity must still enqueue the install.
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-1", Labels: map[string]string{"role": "worker"}}}
	requests := reconciler.mapNodeToInstallations(t.Context(), node)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: "default", Name: "central"}) {
		t.Fatalf("retained central node did not wake installation: %#v", requests)
	}
}

func TestMapNodeToInstallationsWakesRetainedDataPlaneNodeAfterPlacementChange(t *testing.T) {
	scheme := controllerScheme(t)
	dataPlane := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant", UID: types.UID("data-uid")},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
			Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 1},
			DataPlane: operatorv1alpha1.DataPlaneSpec{NodeSelector: map[string]string{"role": "worker"}},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{Topology: operatorv1alpha1.TopologyStatus{
			Nodes: []operatorv1alpha1.DataPlaneNodeStatus{{Node: "worker-1"}},
		}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(dataPlane).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1", Labels: map[string]string{"role": "control"}}}
	requests := reconciler.mapNodeToInstallations(t.Context(), node)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: "tenant", Name: "data"}) {
		t.Fatalf("retained data-plane node did not wake installation: %#v", requests)
	}
}

func TestMapInstallationObjectWakesOnlyMatchingInstallation(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("central-uid")}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	object := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "ovn-central", Namespace: "default", Labels: map[string]string{
		apply.InstallationLabel: "central", apply.InstallationNamespaceLabel: "default", apply.InstallationUIDLabel: "central-uid",
	}}}
	requests := reconciler.mapInstallationObject(t.Context(), object)
	if len(requests) != 1 || requests[0].NamespacedName != (types.NamespacedName{Namespace: "default", Name: "central"}) {
		t.Fatalf("unexpected labeled workload mapping: %#v", requests)
	}
	object.Labels[apply.InstallationUIDLabel] = "recreated-central-uid"
	if requests := reconciler.mapInstallationObject(t.Context(), object); len(requests) != 0 {
		t.Fatalf("stale workload UID redirected an installation: %#v", requests)
	}
	object.Labels = nil
	controller := true
	object.OwnerReferences = []metav1.OwnerReference{{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: "central", UID: installation.UID, Controller: &controller}}
	if requests := reconciler.mapInstallationObject(t.Context(), object); len(requests) != 1 {
		t.Fatalf("owner-bound workload was not mapped: %#v", requests)
	}
}

func TestMapSecretToInstallationsWakesHCPRotation(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "hcp", Namespace: "default", UID: types.UID("hcp-uid")}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		HCP: operatorv1alpha1.HCPConfig{Enabled: true, TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"}},
	}}
	other := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", UID: types.UID("other-uid")}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		HCP: operatorv1alpha1.HCPConfig{Enabled: true, TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "other-tls"}},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation, other).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "default"}, Data: map[string][]byte{"tls.crt": []byte("redacted")}}
	requests := reconciler.mapSecretToInstallations(t.Context(), secret)
	if len(requests) != 1 || requests[0].Name != installation.Name {
		t.Fatalf("unexpected HCP Secret mapping: %#v", requests)
	}
}

func TestMapSecretToInstallationsWakesMemberTLSRotation(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "member-tls", Namespace: "default", UID: types.UID("member-tls-uid")}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Central: operatorv1alpha1.CentralSpec{MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "member-tls-secret"}}}},
	}}
	other := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", UID: types.UID("other-uid")}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Central: operatorv1alpha1.CentralSpec{MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "other-tls-secret"}}}},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation, other).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "member-tls-secret", Namespace: "default"}, Data: map[string][]byte{"tls.crt": []byte("redacted")}}
	requests := reconciler.mapSecretToInstallations(t.Context(), secret)
	if len(requests) != 1 || requests[0].Name != installation.Name {
		t.Fatalf("unexpected member TLS Secret mapping: %#v", requests)
	}
}

func TestMapSecretToInstallationsDeduplicatesHCPAndMemberReference(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "shared-tls", Namespace: "default", UID: types.UID("shared-tls-uid")}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		HCP:     operatorv1alpha1.HCPConfig{Enabled: true, TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "shared-secret"}},
		Central: operatorv1alpha1.CentralSpec{MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "shared-secret"}}}},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared-secret", Namespace: "default"}}
	requests := reconciler.mapSecretToInstallations(t.Context(), secret)
	if len(requests) != 1 || requests[0].Name != installation.Name {
		t.Fatalf("shared Secret produced duplicate reconcile requests: %#v", requests)
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
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || len(got.Status.Conditions) != 9 {
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

func TestUpgradeCentralReadyRequiresWorkloadDatabaseAndNorthdEvidence(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "upgrade", Namespace: "default"}}
	workload := &observedWorkload{observation: apply.Observation{Ready: true}}
	ovn := ovnProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
		return health.OVNReport{Ready: true}, nil
	})
	runtimeProvider := runtimeProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport {
		return health.RuntimeReport{Northd: metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue}}
	})
	reconciler := &KubeOVNInstallationReconciler{OVN: ovn, Runtime: runtimeProvider}
	central := []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central"}}}}
	ready, err := reconciler.upgradeCentralReady(t.Context(), installation, workload, central)
	if err != nil || !ready {
		t.Fatalf("healthy central rollout was not admitted: ready=%t err=%v", ready, err)
	}
	workload.observation.Ready = false
	ready, err = reconciler.upgradeCentralReady(t.Context(), installation, workload, central)
	if err != nil || ready {
		t.Fatalf("unready central workload was admitted: ready=%t err=%v", ready, err)
	}
	workload.observation.Ready = true
	reconciler.OVN = nil
	if _, err := reconciler.upgradeCentralReady(t.Context(), installation, workload, central); err == nil {
		t.Fatal("central rollout was admitted without an OVN readiness provider")
	}
}

func TestUpgradeCRDsReadyWaitsForEstablishedAndNamesAccepted(t *testing.T) {
	scheme := controllerScheme(t)
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "kubeovninstallations.operator.kubeovn.io"}, Status: apiextensionsv1.CustomResourceDefinitionStatus{Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionFalse}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(crd).WithObjects(crd).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube}
	objects := []*unstructured.Unstructured{{Object: map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": crd.Name}}}}
	ready, err := reconciler.upgradeCRDsReady(t.Context(), objects)
	if err != nil || ready {
		t.Fatalf("unready CRD was admitted: ready=%t err=%v", ready, err)
	}
	crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{
		{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
		{Type: apiextensionsv1.NamesAccepted, Status: apiextensionsv1.ConditionTrue},
	}
	if err := kube.Status().Update(t.Context(), crd); err != nil {
		t.Fatal(err)
	}
	ready, err = reconciler.upgradeCRDsReady(t.Context(), objects)
	if err != nil || !ready {
		t.Fatalf("ready CRD was not admitted: ready=%t err=%v", ready, err)
	}
}

func TestReconcileUpgradeAppliesCRDsCentralAndDataPlaneInOrder(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "upgrade-phases", Namespace: "default", UID: types.UID("upgrade-phases-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.18.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	previous := installation.Spec
	previous.Version = "v1.17.0"
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "topology", UID: types.UID("topology-uid"), Digest: "sha256:" + strings.Repeat("a", 64)}
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("b", 64), "OVN_Southbound": "sha256:" + strings.Repeat("c", 64)}
	refsJSON, err := stdjson.Marshal(refs, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	pairID := fmt.Sprintf("sha256:%x", sha256.Sum256(refsJSON))
	installation.Status = operatorv1alpha1.KubeOVNInstallationStatus{
		ApplyCheckpoint: &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version},
		ObservedVersion: previous.Version,
		Topology:        operatorv1alpha1.TopologyStatus{Inventory: ref},
		Upgrade: operatorv1alpha1.UpgradeStatus{CheckpointAuthorized: true, Step: "Backup", Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
			ID: pairID, DatabaseRefs: refs, SourceVersion: previous.Version, TargetVersion: installation.Spec.Version,
			TopologyDigest: ref.Digest, CapturedAt: metav1.NewTime(time.Unix(90, 0).UTC()), Phase: operatorv1alpha1.UpgradeCheckpointVerified,
		}},
	}

	scheme := controllerScheme(t)
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "kubeovninstallations.operator.kubeovn.io"}, Status: apiextensionsv1.CustomResourceDefinitionStatus{Conditions: []apiextensionsv1.CustomResourceDefinitionCondition{
		{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue},
		{Type: apiextensionsv1.NamesAccepted, Status: apiextensionsv1.ConditionTrue},
	}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation, crd).Build()
	topologyFake := &lifecycleTopologyFake{ref: ref, retained: additionTopologyFixture()}
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube}
	workload := new(upgradeRecordingWorkload)
	ovn := ovnProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
		return health.OVNReport{Ready: true}, nil
	})
	runtimeProvider := runtimeProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport {
		return health.RuntimeReport{Northd: metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue}}
	})
	reconciler := &KubeOVNInstallationReconciler{
		Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: upgradeRenderer{}, Workload: workload,
		Topology: topologyFake, Lifecycle: lifecycleFake, OVN: ovn, Runtime: runtimeProvider,
		Clock: func() time.Time { return time.Unix(100, 0).UTC() },
	}
	req := reconcile.Request{NamespacedName: clientKey(installation)}
	for range 3 {
		if _, err := reconciler.Reconcile(t.Context(), req); err != nil {
			t.Fatal(err)
		}
	}
	wantApplied := [][]string{{"CustomResourceDefinition"}, {"Deployment"}, {"DaemonSet"}}
	if got := workload.applied; !slices.EqualFunc(got, wantApplied, func(left, right []string) bool { return slices.Equal(left, right) }) {
		t.Fatalf("upgrade phases applied out of order or as one batch: %#v", got)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Upgrade.Checkpoint == nil || got.Status.Upgrade.Checkpoint.Phase != operatorv1alpha1.UpgradeCheckpointApplied || got.Status.Upgrade.Step != "Verification" {
		t.Fatalf("upgrade phase was not durably advanced: %+v", got.Status.Upgrade)
	}
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
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
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

type upgradeRenderer struct{}

func (upgradeRenderer) Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return chart.Rendered{ValuesYAML: []byte("installMode: full\n"), Objects: []*unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "kubeovninstallations.operator.kubeovn.io"}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central", "namespace": "default"}, "spec": map[string]any{"replicas": int64(3)}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": map[string]any{"name": "ovs-ovn", "namespace": "default"}}},
	}}, nil
}

type upgradeRecordingWorkload struct {
	applied      [][]string
	observations int
}

func (w *upgradeRecordingWorkload) DryRun(context.Context, []*unstructured.Unstructured) error {
	return nil
}

func (w *upgradeRecordingWorkload) Apply(_ context.Context, objects []*unstructured.Unstructured) error {
	kinds := make([]string, 0, len(objects))
	for _, object := range objects {
		kinds = append(kinds, object.GetKind())
	}
	w.applied = append(w.applied, kinds)
	return nil
}

func (w *upgradeRecordingWorkload) Observe(context.Context, []*unstructured.Unstructured) (apply.Observation, error) {
	w.observations++
	return apply.Observation{Ready: true, ReadyCentralReplicas: 1}, nil
}

var _ apply.ApplyClient = (*upgradeRecordingWorkload)(nil)
var _ apply.Observer = (*upgradeRecordingWorkload)(nil)

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

func TestUpgradePauseOnDegradedBlocksVersionRollout(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.18.0",
			Upgrade: operatorv1alpha1.UpgradeSpec{PauseOnDegraded: true},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Conditions:      []metav1.Condition{{Type: "Degraded", Status: metav1.ConditionTrue}},
		},
	}
	if !upgradePaused(installation) {
		t.Fatal("degraded version rollout was not paused")
	}
	installation.Spec.Upgrade.PauseOnDegraded = false
	if upgradePaused(installation) {
		t.Fatal("pause flag did not control the upgrade gate")
	}
	if !installationDegraded(installation) {
		t.Fatal("degraded status detection unexpectedly depended on the pause flag")
	}
	installation.Status.Conditions[0].Status = metav1.ConditionFalse
	if installationDegraded(installation) {
		t.Fatal("healthy installation was treated as degraded")
	}
	installation.Spec.Version = "v1.16.0"
	installation.Status.Conditions[0].Status = metav1.ConditionTrue
	if upgradePaused(installation) {
		t.Fatal("rollback was classified as a degraded forward upgrade pause")
	}
}

func TestReconcilePausesDegradedUpgradeBeforeBackupOrRender(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "paused-upgrade", Namespace: "default", UID: types.UID("paused-upgrade-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.18.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
			Upgrade: operatorv1alpha1.UpgradeSpec{PauseOnDegraded: true},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Conditions:      []metav1.Condition{{Type: "Degraded", Status: metav1.ConditionTrue}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return time.Unix(10, 0) }}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 0 {
		t.Fatalf("paused upgrade was not requeued: %+v", result)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Upgrade.Step != "Paused" || got.Status.Phase != operatorv1alpha1.PhaseUpgrading {
		t.Fatalf("upgrade was not durably paused: phase=%s step=%q", got.Status.Phase, got.Status.Upgrade.Step)
	}
	if got.Status.Upgrade.Checkpoint != nil {
		t.Fatalf("pause unexpectedly captured an upgrade checkpoint: %+v", got.Status.Upgrade.Checkpoint)
	}
}

func TestReconcileBlocksVersionRollbackWithoutRestoreExecutor(t *testing.T) {
	for _, test := range []struct {
		name          string
		allowRollback bool
		wantMessage   string
	}{
		{name: "explicit permission required", wantMessage: "requires spec.upgrade.allowRollback=true"},
		{name: "permission still needs executor", allowRollback: true, wantMessage: "version-aware database restore and workload rollback executor"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := controllerScheme(t)
			installation := &operatorv1alpha1.KubeOVNInstallation{
				ObjectMeta: metav1.ObjectMeta{Name: "rollback", Namespace: "default", UID: types.UID("rollback-uid"), Generation: 2},
				Spec: operatorv1alpha1.KubeOVNInstallationSpec{
					Version: "v1.16.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
					Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeSingle, Replicas: 1, Storage: operatorv1alpha1.StorageSpec{Size: "10Gi"}},
					Upgrade: operatorv1alpha1.UpgradeSpec{AllowRollback: test.allowRollback},
				},
				Status: operatorv1alpha1.KubeOVNInstallationStatus{ObservedVersion: "v1.17.0"},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
			reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return time.Unix(10, 0) }}
			result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
			if err != nil {
				t.Fatal(err)
			}
			if result.RequeueAfter <= 0 {
				t.Fatalf("rollback safety gate was not requeued: %+v", result)
			}
			got := new(operatorv1alpha1.KubeOVNInstallation)
			if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != operatorv1alpha1.PhaseUpgrading || got.Status.Upgrade.Step != "Rollback" {
				t.Fatalf("rollback did not stop in the durable rollback phase: phase=%s step=%q", got.Status.Phase, got.Status.Upgrade.Step)
			}
			if got.Status.Upgrade.Rollback == nil || got.Status.Upgrade.Rollback.Phase != operatorv1alpha1.RollbackPhaseBlocked || got.Status.Upgrade.Rollback.SourceVersion != "v1.17.0" || got.Status.Upgrade.Rollback.TargetVersion != "v1.16.0" {
				t.Fatalf("rollback readiness was not persisted: %+v", got.Status.Upgrade.Rollback)
			}
			condition := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if condition == nil || !strings.Contains(condition.Message, test.wantMessage) {
				t.Fatalf("rollback reason was not exposed: %+v", got.Status.Conditions)
			}
		})
	}
}

func TestRollbackStatusPersistsVerifiedPairAge(t *testing.T) {
	scheme := controllerScheme(t)
	now := time.Unix(20, 0).UTC()
	pairID := testBackupPairID()
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "rollback-rpo", Namespace: "default", UID: types.UID("rollback-rpo-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.16.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeSingle, Replicas: 1, Storage: operatorv1alpha1.StorageSpec{Size: "10Gi"}},
			Upgrade: operatorv1alpha1.UpgradeSpec{AllowRollback: true},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Topology:        operatorv1alpha1.TopologyStatus{Inventory: &operatorv1alpha1.CentralTopologyReference{Digest: "sha256:" + strings.Repeat("a", 64)}},
			LastBackup: &operatorv1alpha1.BackupStatus{
				ID: pairID, Version: "v1.17.0", TopologyDigest: "sha256:" + strings.Repeat("a", 64),
				DatabaseRefs: testBackupPairRefs(), CompletedAt: metav1.NewTime(time.Unix(10, 0).UTC()),
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Clock: func() time.Time { return now }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	rollback := got.Status.Upgrade.Rollback
	if got.Status.ObservedVersion != "v1.17.0" || rollback == nil || rollback.BackupID != pairID || rollback.TopologyDigest != "sha256:"+strings.Repeat("a", 64) || rollback.RPOSeconds != 10 {
		t.Fatalf("rollback pair age was not persisted: %+v", rollback)
	}
}

func TestRollbackBlockedStatusIgnoresIncompleteUpgradeCheckpoint(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	pairID := testBackupPairID()
	topologyDigest := "sha256:" + strings.Repeat("a", 64)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.16.0",
			Upgrade: operatorv1alpha1.UpgradeSpec{Backup: operatorv1alpha1.BackupSpec{Destination: "file:///var/lib/kube-ovn/backups"}},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Topology:        operatorv1alpha1.TopologyStatus{Inventory: &operatorv1alpha1.CentralTopologyReference{Digest: topologyDigest}},
			Upgrade: operatorv1alpha1.UpgradeStatus{Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
				ID: pairID, DatabaseRefs: testBackupPairRefs(), SourceVersion: "v1.16.0", TargetVersion: "v1.17.0",
				Destination: "file:///var/lib/kube-ovn/backups", TopologyDigest: topologyDigest,
				CapturedAt: metav1.NewTime(time.Unix(10, 0).UTC()), Phase: operatorv1alpha1.UpgradeCheckpointFailed,
			}},
		},
	}
	status := rollbackBlockedStatus(installation, now, "restore is unavailable")
	if status.BackupID != "" || status.RPOSeconds != 0 || status.TopologyDigest != "" {
		t.Fatalf("failed checkpoint was exposed as rollback evidence: %+v", status)
	}
}

func TestRollbackAdmissionRejectsMismatchedBackupPairDigest(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	topologyDigest := "sha256:" + strings.Repeat("a", 64)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "rollback-pair", Namespace: "default", UID: types.UID("rollback-pair-uid"), Generation: 2},
		Spec:       operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.16.0", Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeSingle}},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Topology:        operatorv1alpha1.TopologyStatus{Inventory: &operatorv1alpha1.CentralTopologyReference{Digest: topologyDigest}},
			Upgrade: operatorv1alpha1.UpgradeStatus{Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
				ID: "sha256:" + strings.Repeat("b", 64), DatabaseRefs: testBackupPairRefs(), SourceVersion: "v1.16.0", TargetVersion: "v1.17.0", TopologyDigest: topologyDigest, CapturedAt: metav1.NewTime(now.Add(-10 * time.Second)), Phase: operatorv1alpha1.UpgradeCheckpointVerified,
			}},
		},
	}
	reconciler := &KubeOVNInstallationReconciler{}
	if _, _, _, err := reconciler.rollbackAdmission(installation, now); err == nil || !strings.Contains(err.Error(), "backup checkpoint is incomplete") {
		t.Fatalf("mismatched backup pair digest was accepted: %v", err)
	}
}

func TestReconcileExecutesDurableRollbackPhases(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	pairID := testBackupPairID()
	refs := testBackupPairRefs()
	topologyDigest := "sha256:" + strings.Repeat("a", 64)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "rollback-exec", Namespace: "default", UID: types.UID("rollback-exec-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.16.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Chart:   operatorv1alpha1.ChartSpec{Digest: "sha256:" + strings.Repeat("c", 64)},
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeSingle, Replicas: 1, Storage: operatorv1alpha1.StorageSpec{Size: "10Gi"}},
			Upgrade: operatorv1alpha1.UpgradeSpec{AllowRollback: true},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Topology:        operatorv1alpha1.TopologyStatus{Inventory: &operatorv1alpha1.CentralTopologyReference{Digest: topologyDigest}},
			Upgrade: operatorv1alpha1.UpgradeStatus{Checkpoint: &operatorv1alpha1.UpgradeCheckpointStatus{
				ID: pairID, DatabaseRefs: refs,
				SourceVersion: "v1.16.0", TargetVersion: "v1.17.0", TopologyDigest: topologyDigest, CapturedAt: metav1.NewTime(now.Add(-10 * time.Second)), Phase: operatorv1alpha1.UpgradeCheckpointApplied,
			}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(installation).Build()
	executor := &rollbackExecutorFake{}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Clock: func() time.Time { return now }, Leases: lease.New(kube), Rollback: executor}
	key := reconcile.Request{NamespacedName: clientKey(installation)}
	for range 4 {
		if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(executor.phases, []operatorv1alpha1.RollbackPhase{operatorv1alpha1.RollbackPhaseQuiescing, operatorv1alpha1.RollbackPhaseRestoring, operatorv1alpha1.RollbackPhaseWorkloads, operatorv1alpha1.RollbackPhaseVerifying}) {
		t.Fatalf("rollback phases were not resumed in order: %+v", executor.phases)
	}
	if got.Status.ObservedVersion != "v1.16.0" || got.Status.Upgrade.CanRollback || got.Status.Upgrade.Rollback == nil || got.Status.Upgrade.Rollback.Phase != operatorv1alpha1.RollbackPhaseSucceeded || got.Status.Upgrade.Rollback.RTOSeconds != 0 {
		t.Fatalf("rollback completion was not persisted: %+v", got.Status)
	}
}

type rollbackExecutorFake struct {
	phases    []operatorv1alpha1.RollbackPhase
	failPhase operatorv1alpha1.RollbackPhase
	failErr   error
}

func (f *rollbackExecutorFake) Ready() error { return nil }

func (f *rollbackExecutorFake) Capabilities() (bool, bool, bool, error) { return true, true, true, nil }

func (f *rollbackExecutorFake) Step(_ context.Context, _ RollbackPlan, phase operatorv1alpha1.RollbackPhase) (operatorv1alpha1.RollbackPhase, error) {
	f.phases = append(f.phases, phase)
	if phase == f.failPhase {
		if f.failErr != nil {
			return phase, f.failErr
		}
		return phase, fmt.Errorf("rollback phase %q failed", phase)
	}
	switch phase {
	case operatorv1alpha1.RollbackPhaseQuiescing:
		return operatorv1alpha1.RollbackPhaseRestoring, nil
	case operatorv1alpha1.RollbackPhaseRestoring:
		return operatorv1alpha1.RollbackPhaseWorkloads, nil
	case operatorv1alpha1.RollbackPhaseWorkloads:
		return operatorv1alpha1.RollbackPhaseVerifying, nil
	case operatorv1alpha1.RollbackPhaseVerifying:
		return operatorv1alpha1.RollbackPhaseSucceeded, nil
	default:
		return phase, fmt.Errorf("unexpected rollback phase %q", phase)
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
	if !upgradeOnlySpecChange(installation) {
		t.Fatal("target apply checkpoint lost the version-only upgrade identity")
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

func TestUpgradeCheckpointMatchesRequiresAuthenticatedPair(t *testing.T) {
	binding := lifecycle.BackupBinding{Destination: "s3://backups", Version: "v1.17.0", TopologyDigest: "sha256:" + strings.Repeat("a", 64)}
	refs := testBackupPairRefs()
	checkpoint := &operatorv1alpha1.UpgradeCheckpointStatus{
		ID:             testBackupPairID(),
		DatabaseRefs:   refs,
		SourceVersion:  binding.Version,
		TargetVersion:  "v1.18.0",
		Destination:    binding.Destination,
		TopologyDigest: binding.TopologyDigest,
		CapturedAt:     metav1.NewTime(time.Unix(10, 0).UTC()),
		Phase:          operatorv1alpha1.UpgradeCheckpointApplying,
	}
	if !upgradeCheckpointMatches(checkpoint, binding, "v1.18.0") {
		t.Fatal("authenticated Applying checkpoint was rejected")
	}
	checkpoint.ID = "sha256:" + strings.Repeat("f", 64)
	if upgradeCheckpointMatches(checkpoint, binding, "v1.18.0") {
		t.Fatal("checkpoint with a mismatched pair digest was accepted")
	}
	checkpoint.ID = testBackupPairID()
	checkpoint.CapturedAt = metav1.Time{}
	if upgradeCheckpointMatches(checkpoint, binding, "v1.18.0") {
		t.Fatal("checkpoint without a capture time was accepted")
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

func TestReplicaOnlySpecChangeAllowsLifecycleMemberRequestAfterHandoff(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true,
			MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "replacement", ReplaceEndpoint: "chart-0-0"}}},
	}}
	fullHash, baseHash, err := installationSpecHashes(installation.Spec)
	if err != nil {
		t.Fatal(err)
	}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fullHash, BaseSpecHash: baseHash}
	installation.Spec.Central.Replicas = 2
	installation.Spec.Central.MemberRequests = []operatorv1alpha1.CentralMemberSpec{{Name: "retire", RetireEndpoint: "chart-1-0"}}
	allowed, err := replicaOnlySpecChange(installation, 3)
	if err != nil || !allowed {
		t.Fatalf("lifecycle member request after handoff was rejected: allowed=%v err=%v", allowed, err)
	}
	installation.Spec.Version = "v1.18.0"
	allowed, err = replicaOnlySpecChange(installation, 3)
	if err != nil || allowed {
		t.Fatalf("unrelated version change was admitted with lifecycle baseline: allowed=%v err=%v", allowed, err)
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
	installation.Spec.Central.MemberRequests[0].NorthboundClientAddress = "tcp:192.0.2.99:6651"
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "changed after operation admission") {
		t.Fatalf("client listener drift was not rejected: %v", err)
	}
	installation.Spec.Central.MemberRequests[0].NorthboundClientAddress = "tcp:192.0.2.99:6641"
	installation.Spec.Central.MemberRequests[0].TLSSecretRef = &operatorv1alpha1.LocalObjectReference{Name: "member-tls"}
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "changed after operation admission") {
		t.Fatalf("TLS Secret reference drift was not rejected: %v", err)
	}
	installation.Spec.Central.MemberRequests[0].TLSSecretRef = nil
	installation.Spec.Central.MemberRequests[0].NodeName = "other-node"
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "changed after operation admission") {
		t.Fatalf("member identity drift was not rejected: %v", err)
	}
	installation.Spec.Central.MemberRequests = nil
	if err := validateLifecycleMemberRequest(installation, request); err == nil || !strings.Contains(err.Error(), "was removed") {
		t.Fatalf("member request removal was not rejected: %v", err)
	}
}

func TestValidateRecoveryTargetMemberTLSContract(t *testing.T) {
	member := operatorv1alpha1.CentralMemberSpec{
		Name: "member", NodeName: "node", ClaimName: "claim", VolumeName: "volume",
		Image:             "example/central@sha256:" + strings.Repeat("a", 64),
		NorthboundAddress: "ssl:192.0.2.10:6643", SouthboundAddress: "ssl:192.0.2.10:6644",
		NorthboundClientAddress: "ssl:192.0.2.10:6641", SouthboundClientAddress: "ssl:192.0.2.10:6642",
	}
	if err := validateRecoveryTargetMember(member); err == nil || !strings.Contains(err.Error(), "tlsSecretRef") {
		t.Fatalf("SSL recovery target without TLS Secret was accepted: %v", err)
	}
	member.TLSSecretRef = &operatorv1alpha1.LocalObjectReference{Name: "member-tls"}
	if err := validateRecoveryTargetMember(member); err != nil {
		t.Fatalf("valid SSL recovery target was rejected: %v", err)
	}
	member.SouthboundClientAddress = "tcp:192.0.2.10:6642"
	if err := validateRecoveryTargetMember(member); err == nil || !strings.Contains(err.Error(), "one protocol") {
		t.Fatalf("mixed-protocol recovery target was accepted: %v", err)
	}
}

type lifecycleCoordinatorFake struct {
	reader           client.Reader
	begin            int
	resume           int
	cleanup          int
	cleanupResult    backup.RetentionResult
	cleanupErr       error
	cleanupProtected []string
	resumeReferences []operatorv1alpha1.CentralOperationReference
	request          lifecycle.Request
	reference        lifecycle.Reference
	recovery         *lifecycle.Recovery
	restoreErr       error
}

func (f *lifecycleCoordinatorFake) Reader() client.Reader { return f.reader }

func (f *lifecycleCoordinatorFake) SetStoreFactory(func(*operatorv1alpha1.KubeOVNInstallation) (backup.Store, error)) {
}

func (f *lifecycleCoordinatorFake) Begin(_ context.Context, _ *operatorv1alpha1.KubeOVNInstallation, request lifecycle.Request) (lifecycle.Reference, error) {
	f.begin++
	f.request = request
	return f.reference, nil
}

func (f *lifecycleCoordinatorFake) Resume(_ context.Context, _ *operatorv1alpha1.KubeOVNInstallation, operation *operatorv1alpha1.CentralOperationReference) (*lifecycle.Recovery, error) {
	f.resume++
	if operation != nil {
		f.resumeReferences = append(f.resumeReferences, *operation)
	}
	return f.recovery, nil
}

func (f *lifecycleCoordinatorFake) ProtectedBackupRefs(context.Context, *operatorv1alpha1.KubeOVNInstallation, *operatorv1alpha1.CentralOperationReference) ([]string, error) {
	return nil, nil
}

func (f *lifecycleCoordinatorFake) CleanupBackups(_ context.Context, _ *operatorv1alpha1.KubeOVNInstallation, _ backup.RetentionPolicy, protected []string, _ time.Time) (backup.RetentionResult, error) {
	f.cleanup++
	f.cleanupProtected = append([]string(nil), protected...)
	return f.cleanupResult, f.cleanupErr
}

func (f *lifecycleCoordinatorFake) CaptureBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding) (*operatorv1alpha1.BackupStatus, error) {
	return nil, nil
}

func (f *lifecycleCoordinatorFake) VerifyBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *operatorv1alpha1.BackupStatus) error {
	return nil
}

func (f *lifecycleCoordinatorFake) RestoreBackupPair(context.Context, *operatorv1alpha1.KubeOVNInstallation, health.CentralTopology, lifecycle.BackupBinding, *operatorv1alpha1.BackupStatus) error {
	return f.restoreErr
}

func (f *lifecycleCoordinatorFake) RestoreFenceReady() error { return nil }

var _ lifecycleCoordinator = (*lifecycleCoordinatorFake)(nil)

type lifecycleTopologyFake struct {
	ref               *operatorv1alpha1.CentralTopologyReference
	retained          health.CentralTopology
	replacement       health.CentralTopology
	replacementTarget string
	replacementErr    error
	verifyErr         error
}

func (f *lifecycleTopologyFake) Ensure(context.Context, *operatorv1alpha1.KubeOVNInstallation) (*operatorv1alpha1.CentralTopologyReference, error) {
	return f.ref, nil
}

func (f *lifecycleTopologyFake) VerifyRetained(context.Context, *operatorv1alpha1.KubeOVNInstallation) (*operatorv1alpha1.CentralTopologyReference, error) {
	return f.ref, f.verifyErr
}

func (f *lifecycleTopologyFake) Retained(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.CentralTopology, error) {
	return f.retained, nil
}

func (f *lifecycleTopologyFake) VerifyRetainedReplacement(_ context.Context, _ *operatorv1alpha1.KubeOVNInstallation, target string) (health.CentralTopology, error) {
	if f.replacementTarget != "" && target != f.replacementTarget {
		return health.CentralTopology{}, fmt.Errorf("unexpected replacement target %q", target)
	}
	return f.replacement, f.replacementErr
}

var _ topology.Provider = (*lifecycleTopologyFake)(nil)
var _ topology.InventoryProvider = (*lifecycleTopologyFake)(nil)
var _ topology.ReplacementProvider = (*lifecycleTopologyFake)(nil)

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

func TestReconcileSelectsAndPersistsExplicitChartDeploymentRetirement(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "retirement", Namespace: "default", UID: types.UID("retirement-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "retire-chart", RetireEndpoint: "chart-3-0"}}},
		},
	}
	previous := installation.Spec
	previous.Central.Replicas = 4
	previous.Central.MemberRequests = nil
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "inventory", UID: types.UID("inventory-uid"), Digest: "sha256:" + strings.Repeat("b", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version}
	installation.Status.Topology.Inventory = ref

	retained := deploymentRetirementTopologyFixture()
	objects := []client.Object{installation}
	objects = append(objects, deploymentRetirementObjects(installation)...)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(objects...).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube, reference: lifecycle.Reference{Journal: workload.Reference{Name: "journal", UID: types.UID("journal-uid")}, Request: workload.Reference{Name: "request", UID: types.UID("request-uid")}}}
	topologyFake := &lifecycleTopologyFake{ref: ref, retained: retained}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Leases: lease.New(kube), Topology: topologyFake, Lifecycle: lifecycleFake}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 10*time.Second || lifecycleFake.begin != 1 || lifecycleFake.request.Add != nil || lifecycleFake.request.RemoveEndpoint != "chart-3-0" || lifecycleFake.request.Retirement == nil || lifecycleFake.request.Retirement.Kind != "Deployment" {
		t.Fatalf("default controller did not admit the expected Deployment retirement: result=%+v begin=%d request=%+v", result, lifecycleFake.begin, lifecycleFake.request)
	}
	retirement := lifecycleFake.request.Retirement
	if retirement.Endpoint.UID != types.UID("chart-3-pod-0") || retirement.ReplicaSet.UID != types.UID("chart-3-rs-uid") || len(retirement.Survivors) != 1 || retirement.Survivors[0].UID != types.UID("chart-3-pod-1") {
		t.Fatalf("Deployment retirement did not pin Pod and ReplicaSet identities: %+v", retirement)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Topology.Operation == nil || got.Status.Topology.Operation.JournalUID != types.UID("journal-uid") || got.Status.Topology.Operation.RequestUID != types.UID("request-uid") {
		t.Fatalf("retirement operation was not durably recorded: %+v", got.Status.Topology.Operation)
	}
}

func TestReconcileAdmitsApprovedLostChartDeploymentReplacement(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "lost-replacement", Namespace: "default", UID: types.UID("lost-replacement-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{
				Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5, DynamicMembership: true,
				MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
					Name: "member-replacement", NodeName: "node-new", ClaimName: "claim-new", VolumeName: "volume-new",
					Image:             "example/central@sha256:" + strings.Repeat("a", 64),
					NorthboundAddress: "tcp:192.0.2.99:6643", SouthboundAddress: "tcp:192.0.2.99:6644",
					NorthboundClientAddress: "tcp:192.0.2.99:6641", SouthboundClientAddress: "tcp:192.0.2.99:6642",
					RemovalApprovalRef: &operatorv1alpha1.MemberRemovalApprovalReference{Name: "lost-member-approval", UID: types.UID("approval-uid")},
				}},
			},
		},
	}
	previous := installation.Spec
	previous.Central.MemberRequests = nil
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "inventory", UID: types.UID("inventory-uid"), Digest: "sha256:" + strings.Repeat("b", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version}
	installation.Status.Topology.Inventory = ref

	retained := deploymentRetirementTopologyFixture()
	// The Chart Deployment occupies two retained topology slots. The second
	// slot is the healthy survivor; only chart-3-0 is intentionally absent from
	// the live objects below.
	retained.Members[2].Workload = retained.Members[3].Workload
	retained.Members[2].Node = health.ObjectIdentity{Name: "node-4", UID: types.UID("node-uid-4")}
	retained.Members[2].Pod = health.ObjectIdentity{Name: "chart-3-1", UID: types.UID("chart-3-pod-1")}
	retained.Members = append(retained.Members, controllerTopologyMember(4))
	objects := []client.Object{installation, removalApprovalObjectForController(installation)}
	for _, object := range deploymentRetirementObjects(installation) {
		if pod, ok := object.(*corev1.Pod); ok && pod.Name == "chart-3-0" {
			continue
		}
		objects = append(objects, object)
	}
	objects = append(objects, memberObjectsForController(installation, retained, "chart-3-0")...)
	objects = append(objects, newMemberObjectsForController(installation)...)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(objects...).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube, reference: lifecycle.Reference{Journal: workload.Reference{Name: "journal", UID: types.UID("journal-uid")}, Request: workload.Reference{Name: "request", UID: types.UID("request-uid")}}}
	topologyFake := &lifecycleTopologyFake{ref: ref, retained: retained, verifyErr: fmt.Errorf("old endpoint UID is gone")}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Leases: lease.New(kube), Topology: topologyFake, Lifecycle: lifecycleFake}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 10*time.Second || lifecycleFake.begin != 1 {
		t.Fatalf("approved lost endpoint replacement was not admitted: result=%+v begin=%d request=%+v", result, lifecycleFake.begin, lifecycleFake.request)
	}
	request := lifecycleFake.request
	if !request.AutomaticReplacement || request.Add == nil || request.RemoveEndpoint != "chart-3-0" || request.Retirement == nil || !request.Retirement.EndpointMissing {
		t.Fatalf("unexpected automatic replacement request: %+v", request)
	}
	if request.OperationID != controllerRemovalApprovalOperationID {
		t.Fatalf("replacement did not reuse the approval operation UUID: %q", request.OperationID)
	}
	if request.Retirement.Endpoint.UID != types.UID("chart-3-pod-0") || request.Retirement.ReplicaSet.UID != types.UID("chart-3-rs-uid") || len(request.Retirement.Survivors) != 1 || request.Retirement.Survivors[0].UID != types.UID("chart-3-pod-1") {
		t.Fatalf("lost endpoint retirement did not pin the retained identities: %+v", request.Retirement)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), clientKey(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Topology.Operation == nil || got.Status.Topology.Operation.JournalUID != types.UID("journal-uid") || got.Status.Topology.Operation.RequestUID != types.UID("request-uid") {
		t.Fatalf("replacement operation was not durably recorded: %+v", got.Status.Topology.Operation)
	}
}

func TestReconcileRebindsAutomaticReplacementAfterRetainedNodeAddressDrift(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "address-drift", Namespace: "default", UID: types.UID("address-drift-uid"), Generation: 2},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{
				Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true,
				MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
					Name: "member-replacement", NodeName: "node-new", ClaimName: "claim-new", VolumeName: "volume-new",
					Image:             "example/central@sha256:" + strings.Repeat("a", 64),
					NorthboundAddress: "tcp:192.0.2.99:6643", SouthboundAddress: "tcp:192.0.2.99:6644",
					NorthboundClientAddress: "tcp:192.0.2.99:6641", SouthboundClientAddress: "tcp:192.0.2.99:6642",
					RemovalApprovalRef: &operatorv1alpha1.MemberRemovalApprovalReference{Name: "lost-member-approval", UID: types.UID("approval-uid")},
				}},
			},
		},
	}
	previous := installation.Spec
	previous.Central.MemberRequests = nil
	previousJSON, err := stdjson.Marshal(previous, stdjson.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	ref := &operatorv1alpha1.CentralTopologyReference{Name: "inventory", UID: types.UID("inventory-uid"), Digest: "sha256:" + strings.Repeat("b", 64)}
	installation.Status.ApplyCheckpoint = &operatorv1alpha1.ApplyCheckpointStatus{SpecHash: fmt.Sprintf("%x", sha256.Sum256(previousJSON)), Version: previous.Version}
	installation.Status.Topology.Inventory = ref

	retained := additionTopologyFixture()
	retained.Members[0].Workload = health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "central-0", UID: types.UID("workload-uid-0")}, Kind: "StatefulSet"}
	retained.Members[0].Pod = health.ObjectIdentity{Name: "central-0-0", UID: types.UID("pod-uid-0")}
	observed := retained
	observed.Members = slices.Clone(retained.Members)
	observed.Members[0].Databases = [2]health.DatabaseIdentity{}
	for database, identity := range retained.Members[0].Databases {
		identity.Address = fmt.Sprintf("tcp:192.0.2.10:%d", 6643+database)
		identity.ClientAddress = fmt.Sprintf("tcp:192.0.2.10:%d", 6641+database)
		observed.Members[0].Databases[database] = identity
	}

	controller := true
	installationOwner := metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: &controller}
	labels := map[string]string{
		apply.ManagedByLabel:             apply.ManagedByValue,
		apply.InstallationLabel:          installation.Name,
		apply.InstallationNamespaceLabel: installation.Namespace,
		apply.InstallationUIDLabel:       string(installation.UID),
	}
	replicas := int32(1)
	retainedSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "central-0", Namespace: installation.Namespace, UID: types.UID("workload-uid-0"), Labels: labels, OwnerReferences: []metav1.OwnerReference{installationOwner}},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: "example/central@sha256:" + strings.Repeat("a", 64)}}}},
		},
	}
	objects := []client.Object{installation, removalApprovalObjectForController(installation), retainedSet}
	for index, member := range retained.Members {
		address := fmt.Sprintf("192.0.2.%d", index+1)
		if index == 0 {
			address = "192.0.2.10"
		}
		objects = append(objects,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: member.Node.Name, UID: member.Node.UID}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: member.Pod.Name, Namespace: installation.Namespace, UID: member.Pod.UID}, Spec: corev1.PodSpec{NodeName: member.Node.Name}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		)
	}
	objects = append(objects,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: types.UID("node-new-uid")}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.99"}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "volume-new"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "volume-new", UID: types.UID("volume-new-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	)
	kube := fake.NewClientBuilder().WithScheme(controllerScheme(t)).WithStatusSubresource(installation).WithObjects(objects...).Build()
	lifecycleFake := &lifecycleCoordinatorFake{reader: kube, reference: lifecycle.Reference{Journal: workload.Reference{Name: "journal", UID: types.UID("journal-uid")}, Request: workload.Reference{Name: "request", UID: types.UID("request-uid")}}}
	topologyFake := &lifecycleTopologyFake{ref: ref, retained: retained, replacement: observed, replacementTarget: "central-0-0"}
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: controllerScheme(t), Leases: lease.New(kube), Topology: topologyFake, Lifecycle: lifecycleFake}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: clientKey(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 10*time.Second || lifecycleFake.begin != 1 {
		got := new(operatorv1alpha1.KubeOVNInstallation)
		_ = kube.Get(t.Context(), clientKey(installation), got)
		t.Fatalf("automatic address-drift replacement was not admitted: result=%+v begin=%d request=%+v status=%+v", result, lifecycleFake.begin, lifecycleFake.request, got.Status)
	}
	request := lifecycleFake.request
	if !request.AutomaticReplacement || request.RemoveEndpoint != "central-0-0" || request.Retirement == nil || request.Retirement.Kind != "StatefulSet" {
		t.Fatalf("unexpected automatic address-drift request: %+v", request)
	}
	for database, inventory := range request.Databases {
		if inventory.Database != []string{"OVN_Northbound", "OVN_Southbound"}[database] {
			t.Fatalf("unexpected database order: %+v", request.Databases)
		}
		for _, peer := range inventory.Peers {
			if peer.Name == "central-0-0" {
				want := observed.Members[0].Databases[database]
				if peer.Address != want.Address || peer.ClientAddress != want.ClientAddress {
					t.Fatalf("target database %d was not rebound: got=%+v want=%+v", database, peer, want)
				}
			}
		}
		if !slices.Contains(request.Add.Workload.Preparation.Databases[database].RemoteAddresses, observed.Members[0].Databases[database].Address) {
			t.Fatalf("preparation database %d did not include the rebound target address: %+v", database, request.Add.Workload.Preparation.Databases[database])
		}
	}
	if got := request.Databases[0].Peers[1].Address; got != retained.Members[1].Databases[0].Address {
		t.Fatalf("non-target peer address changed during rebind: got=%q want=%q", got, retained.Members[1].Databases[0].Address)
	}
	if got := request.Add.NorthboundAddress; got != "tcp:192.0.2.99:6641" {
		t.Fatalf("new member client address changed during rebind: %q", got)
	}
}

const controllerRemovalApprovalOperationID = "eeee0000-1111-2222-3333-444444444444"

func removalApprovalObjectForController(installation *operatorv1alpha1.KubeOVNInstallation) *operatorv1alpha1.KubeOVNMemberRemovalApproval {
	return &operatorv1alpha1.KubeOVNMemberRemovalApproval{
		ObjectMeta: metav1.ObjectMeta{Name: "lost-member-approval", Namespace: installation.Namespace, UID: types.UID("approval-uid")},
		Spec: operatorv1alpha1.MemberRemovalApprovalSpec{Decision: operatorv1alpha1.MemberRemovalDecision{
			InstallationRef: operatorv1alpha1.InstallationReference{Name: installation.Name, UID: installation.UID},
			OperationID:     controllerRemovalApprovalOperationID,
		}},
	}
}

func memberObjectsForController(installation *operatorv1alpha1.KubeOVNInstallation, topology health.CentralTopology, missingEndpoint string) []client.Object {
	objects := make([]client.Object, 0, len(topology.Members)*2)
	for _, member := range topology.Members {
		if member.Pod.Name == missingEndpoint || member.Pod.Name == "chart-3-1" {
			continue
		}
		objects = append(objects,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: member.Node.Name, UID: member.Node.UID}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
			&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: member.Pod.Name, Namespace: installation.Namespace, UID: member.Pod.UID}, Spec: corev1.PodSpec{NodeName: member.Node.Name}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
		)
	}
	return objects
}

func newMemberObjectsForController(installation *operatorv1alpha1.KubeOVNInstallation) []client.Object {
	return []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: types.UID("node-new-uid")}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.99"}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "volume-new"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "volume-new", UID: types.UID("volume-new-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	}
}

func controllerTopologyMember(index int) health.CentralIdentity {
	member := health.CentralIdentity{
		Node:        health.ObjectIdentity{Name: fmt.Sprintf("node-%d", index), UID: types.UID(fmt.Sprintf("node-uid-%d", index))},
		Workload:    health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: fmt.Sprintf("central-%d", index), UID: types.UID(fmt.Sprintf("workload-uid-%d", index))}, Kind: "Deployment"},
		Storage:     []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: fmt.Sprintf("/var/lib/ovn/%d", index)}},
		Pod:         health.ObjectIdentity{Name: fmt.Sprintf("central-%d-pod", index), UID: types.UID(fmt.Sprintf("pod-uid-%d", index))},
		ContainerID: fmt.Sprintf("containerd://central-%d", index),
	}
	for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		member.Databases[database] = health.DatabaseIdentity{
			Database: name, ClusterID: fmt.Sprintf("1111111%d-2222-3333-4444-555555555555", database),
			ServerID: fmt.Sprintf("0000000%d-1111-2222-3333-444444444444", index+2),
			Address:  fmt.Sprintf("tcp:192.0.2.%d:%d", index+1, 6643+database), ClientAddress: fmt.Sprintf("tcp:192.0.2.%d:%d", index+1, 6641+database),
		}
	}
	return member
}

func deploymentRetirementTopologyFixture() health.CentralTopology {
	topology := additionTopologyFixture()
	member := health.CentralIdentity{
		Node:        health.ObjectIdentity{Name: "node-3", UID: types.UID("node-uid-3")},
		Workload:    health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "chart-3", UID: types.UID("chart-3-workload-uid")}, Kind: "Deployment"},
		Storage:     []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: "/var/lib/ovn/3"}},
		Pod:         health.ObjectIdentity{Name: "chart-3-0", UID: types.UID("chart-3-pod-0")},
		ContainerID: "containerd://chart-3-0",
	}
	for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		member.Databases[database] = health.DatabaseIdentity{
			Database: name, ClusterID: fmt.Sprintf("1111111%d-2222-3333-4444-555555555555", database),
			ServerID: fmt.Sprintf("0000000%d-1111-2222-3333-444444444444", database+4),
			Address:  fmt.Sprintf("tcp:192.0.2.4:%d", 6643+database), ClientAddress: fmt.Sprintf("tcp:192.0.2.4:%d", 6641+database),
		}
	}
	topology.Members = append(topology.Members, member)
	return topology
}

func deploymentRetirementObjects(installation *operatorv1alpha1.KubeOVNInstallation) []client.Object {
	controller := true
	labels := map[string]string{"app": "ovn-central"}
	installationOwner := metav1.OwnerReference{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: &controller}
	deploymentOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "chart-3", UID: types.UID("chart-3-workload-uid"), Controller: &controller}
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: "example/central@sha256:" + strings.Repeat("a", 64)}}}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "chart-3", Namespace: installation.Namespace, UID: types.UID("chart-3-workload-uid"), Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)}, OwnerReferences: []metav1.OwnerReference{installationOwner}}, Spec: appsv1.DeploymentSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: template}}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "chart-3-rs", Namespace: installation.Namespace, UID: types.UID("chart-3-rs-uid"), OwnerReferences: []metav1.OwnerReference{deploymentOwner}}, Spec: appsv1.ReplicaSetSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: template}}
	podOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}
	ready := corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	pods := []*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "chart-3-0", Namespace: installation.Namespace, UID: types.UID("chart-3-pod-0"), Labels: labels, OwnerReferences: []metav1.OwnerReference{podOwner}}, Spec: corev1.PodSpec{NodeName: "node-3", HostNetwork: true, Containers: template.Spec.Containers}, Status: ready},
		{ObjectMeta: metav1.ObjectMeta{Name: "chart-3-1", Namespace: installation.Namespace, UID: types.UID("chart-3-pod-1"), Labels: labels, OwnerReferences: []metav1.OwnerReference{podOwner}}, Spec: corev1.PodSpec{NodeName: "node-4", HostNetwork: true, Containers: template.Spec.Containers}, Status: ready},
	}
	objects := []client.Object{deployment, replicaSet}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	return objects
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
