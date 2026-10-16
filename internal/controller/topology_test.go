package controller

import (
	"context"
	"errors"
	"testing"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/api/meta"
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

func TestLifecycleRenderSpecKeepsChartDeploymentAtItsRetainedCount(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &api.KubeOVNInstallation{Name: "dynamic", Namespace: "default", UID: "installation", Generation: 2, Spec: api.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 5, DynamicMembership: true}}, Status: api.KubeOVNInstallationStatus{Topology: api.TopologyStatus{Inventory: &api.CentralTopologyReference{Name: "inventory", UID: "inventory-uid", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	retained := health.CentralTopology{Members: []health.CentralIdentity{{Workload: health.WorkloadIdentity{Kind: "Deployment"}}, {Workload: health.WorkloadIdentity{Kind: "Deployment"}}, {Workload: health.WorkloadIdentity{Kind: "Deployment"}}, {Workload: health.WorkloadIdentity{Kind: "StatefulSet"}}, {Workload: health.WorkloadIdentity{Kind: "StatefulSet"}}}}
	rendered, err := (&KubeOVNInstallationReconciler{Client: kube, Leases: lease.New(kube), Topology: retainedTopologyProvider{retained: retained}}).lifecycleRenderSpec(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if rendered.Central.Replicas != 3 {
		t.Fatalf("dynamic rendering would scale the Chart Deployment to %d replicas", rendered.Central.Replicas)
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
