package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type topologyProviderFunc func(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error)

func (f topologyProviderFunc) Ensure(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	return f(ctx, installation)
}
func (f topologyProviderFunc) VerifyRetained(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	if installation.Status.Topology.Inventory == nil {
		return nil, nil
	}
	return f(ctx, installation)
}

func verifiedTopology(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
		return nil, err
	}
	return &api.CentralTopologyReference{Name: "inventory", UID: "inventory-uid", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, nil
}

type retainedTopologyProvider struct {
	topologyProviderFunc
	retained health.CentralTopology
}

func (p retainedTopologyProvider) Retained(context.Context, *api.KubeOVNInstallation) (health.CentralTopology, error) {
	return p.retained, nil
}

func TestLifecycleRenderSpecKeepsLegalRaftInputAndReturnsRetainedChartTopology(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &api.KubeOVNInstallation{Name: "dynamic", Namespace: "default", UID: "installation", Generation: 2, Spec: api.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3, DynamicMembership: true}}, Status: api.KubeOVNInstallationStatus{Topology: api.TopologyStatus{Inventory: &api.CentralTopologyReference{Name: "inventory", UID: "inventory-uid", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	retained := health.CentralTopology{Members: []health.CentralIdentity{{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "controller-retirement"}, Kind: "Deployment"}}, {Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "controller-retirement"}, Kind: "Deployment"}}, {Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "replacement"}, Kind: "StatefulSet"}}}}
	rendered, retainedTopology, err := (&KubeOVNInstallationReconciler{Client: kube, Leases: lease.New(kube), Topology: retainedTopologyProvider{retained: retained}}).lifecycleRenderSpec(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if rendered.Central.Replicas != 3 || retainedTopology == nil {
		t.Fatalf("dynamic rendering lost the legal Raft input or retained topology: replicas=%d topology=%v", rendered.Central.Replicas, retainedTopology)
	}
}

func TestApplyRetainedChartDeploymentReplicas(t *testing.T) {
	deployment := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "controller-retirement"},
		"spec":       map[string]any{"replicas": int64(3)},
	}}
	retained := health.CentralTopology{Members: []health.CentralIdentity{
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "controller-retirement"}, Kind: "Deployment"}},
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "controller-retirement"}, Kind: "Deployment"}},
		{Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "replacement"}, Kind: "StatefulSet"}},
	}}
	if err := applyRetainedChartDeploymentReplicas([]*unstructured.Unstructured{deployment}, retained); err != nil {
		t.Fatal(err)
	}
	got, found, err := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
	if err != nil || !found || got != 2 {
		t.Fatalf("retained Chart Deployment replicas=%d found=%t err=%v", got, found, err)
	}
}

func TestLifecycleRenderSpecLeavesNonDynamicInstallationUnchanged(t *testing.T) {
	installation := &api.KubeOVNInstallation{Spec: api.KubeOVNInstallationSpec{Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 5}}}
	rendered, retained, err := (&KubeOVNInstallationReconciler{}).lifecycleRenderSpec(t.Context(), installation)
	if err != nil || retained != nil || rendered.Central.Replicas != 5 || rendered.Central.DynamicMembership {
		t.Fatalf("non-dynamic installation changed during lifecycle rendering: spec=%+v retained=%v err=%v", rendered.Central, retained, err)
	}
}

func TestReconcileRetainsTopologyOnFailureAndNeverUsesItAsHealth(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &api.KubeOVNInstallation{Name: "network", Namespace: "default", UID: "installation", Generation: 1, Spec: api.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: api.InstallationModeFull, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3}}
	var failure error
	provider := topologyProviderFunc(func(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
		ref, err := verifiedTopology(ctx, installation)
		return ref, errors.Join(err, failure)
	})
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload, Topology: provider}
	run := func() *api.KubeOVNInstallation {
		t.Helper()
		if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(installation)}); err != nil {
			t.Fatal(err)
		}
		got := new(api.KubeOVNInstallation)
		if err := kube.Get(t.Context(), client.ObjectKeyFromObject(installation), got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := run()
	if got.Status.Topology.Inventory == nil || !meta.IsStatusConditionTrue(got.Status.Conditions, "TopologyRecorded") || meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") {
		t.Fatalf("missing retained identity or false health: %+v", got.Status)
	}
	ref := *got.Status.Topology.Inventory
	applied := workload.applyCalls
	failure = errors.New("central member is unavailable")
	got = run()
	if workload.applyCalls != applied {
		t.Fatal("identity drift did not stop workload reapplication")
	}
	if got.Status.Topology.Inventory == nil || *got.Status.Topology.Inventory != ref || !meta.IsStatusConditionFalse(got.Status.Conditions, "TopologyRecorded") || !meta.IsStatusConditionTrue(got.Status.Conditions, "Degraded") {
		t.Fatalf("failure lost identity or retained health: %+v", got.Status)
	}
	// Rollout failure prevents new discovery, but cannot erase the retained ref.
	workload.observation.Ready = false
	got = run()
	if got.Status.Topology.Inventory == nil || *got.Status.Topology.Inventory != ref || meta.IsStatusConditionTrue(got.Status.Conditions, "TopologyRecorded") {
		t.Fatalf("stale rollout reused identity as health: %+v", got.Status)
	}
}

func TestReconcileCordonedCentralNodeFailsClosedBeforeWorkloadApply(t *testing.T) {
	scheme := controllerScheme(t)
	ref := &api.CentralTopologyReference{Name: "inventory", UID: "inventory-uid", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	installation := &api.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "cordoned", Namespace: "default", UID: "cordoned-uid", Generation: 1},
		Spec: api.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: api.InstallationModeControlPlaneOnly,
			Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3},
		},
		Status: api.KubeOVNInstallationStatus{Topology: api.TopologyStatus{Inventory: ref}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3}}
	provider := topologyProviderFunc(func(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
		return ref, &health.NodeLifecycleError{NodeName: "node-0", Unschedulable: true}
	})
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload, Topology: provider}
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(installation)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 30*time.Second || workload.applyCalls != 0 {
		t.Fatalf("cordoned central node was not fail-closed: result=%+v applyCalls=%d", result, workload.applyCalls)
	}
	got := new(api.KubeOVNInstallation)
	if err := kube.Get(t.Context(), client.ObjectKeyFromObject(installation), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Topology.Inventory == nil || *got.Status.Topology.Inventory != *ref {
		t.Fatalf("cordoned node failure discarded retained topology: %+v", got.Status.Topology)
	}
	topologyCondition := meta.FindStatusCondition(got.Status.Conditions, "TopologyRecorded")
	if topologyCondition == nil || topologyCondition.Status != metav1.ConditionFalse || topologyCondition.Reason != "CentralNodeDraining" {
		t.Fatalf("cordoned node did not produce CentralNodeDraining: %+v", got.Status.Conditions)
	}
	if got.Status.Phase != api.PhaseDegraded || meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") {
		t.Fatalf("cordoned node was reported ready: %+v", got.Status)
	}
}
