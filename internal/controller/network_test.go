package controller

import (
	"context"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type networkProviderFunc func(context.Context, *operatorv1alpha1.KubeOVNInstallation, []operatorv1alpha1.DataPlaneNodeStatus) health.NetworkReport

func (f networkProviderFunc) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, nodes []operatorv1alpha1.DataPlaneNodeStatus) health.NetworkReport {
	return f(ctx, installation, nodes)
}

func TestNetworkVerificationGatesReadyAndClearsOldEvidence(t *testing.T) {
	for _, failure := range []string{"network", "database", "runtime", "rollout", "missing evidence", "missing provider"} {
		t.Run(failure, func(t *testing.T) {
			scheme := controllerScheme(t)
			installation := &operatorv1alpha1.KubeOVNInstallation{Name: "network", Namespace: "default", UID: "installation", Generation: 1, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}}}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
			workload := &observedWorkload{observation: apply.Observation{Ready: true, ReadyCentralReplicas: 3}}
			ovn := health.OVNReport{Ready: true, Reason: "OVNDatabasesHealthy"}
			runtime := health.RuntimeReport{Northd: metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue, Reason: "NorthdHealthy"}, DataPlane: metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionTrue, Reason: "DataPlaneConnected"}, Nodes: []operatorv1alpha1.DataPlaneNodeStatus{{Node: "node", NodeUID: "node-uid"}}}
			network := health.NetworkReport{Condition: metav1.Condition{Type: "NetworkReady", Status: metav1.ConditionTrue, Reason: "NetworkProbesPassed"}, Evidence: &operatorv1alpha1.NetworkStatus{ObservedNodes: 1, AddressFamilies: []string{"IPv4"}, SuccessfulProbes: 3}}
			calls := 0
			reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Leases: lease.New(kube), Renderer: objectRenderer{}, Workload: workload,
				OVN: ovnProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) (health.OVNReport, error) {
					return ovn, nil
				}),
				Runtime: runtimeProviderFunc(func(context.Context, *operatorv1alpha1.KubeOVNInstallation) health.RuntimeReport { return runtime }),
				Network: networkProviderFunc(func(ctx context.Context, _ *operatorv1alpha1.KubeOVNInstallation, nodes []operatorv1alpha1.DataPlaneNodeStatus) health.NetworkReport {
					calls++
					if lease.Check(ctx, lease.Installation) == nil || len(nodes) != 1 {
						t.Fatal("invalid network observation context")
					}
					return network
				}),
			}
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
			if !meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || got.Status.Phase != operatorv1alpha1.PhaseReady || got.Status.Network == nil || got.Status.Network.LastObserved.IsZero() {
				t.Fatalf("verified network not ready: %+v", got.Status)
			}
			switch failure {
			case "network":
				network.Condition.Status, network.Condition.Reason = metav1.ConditionFalse, "NetworkUnhealthy"
				network.Evidence = nil
			case "database":
				ovn.Ready = false
				ovn.Reason = "OVNDatabaseUnhealthy"
			case "runtime":
				runtime.DataPlane.Status = metav1.ConditionFalse
				runtime.DataPlane.Reason = "DataPlaneUnhealthy"
			case "rollout":
				workload.observation.Ready = false
			case "missing evidence":
				network.Evidence = nil
			case "missing provider":
				reconciler.Network = nil
			}
			got = run()
			if meta.IsStatusConditionTrue(got.Status.Conditions, "Ready") || got.Status.Network != nil {
				t.Fatalf("stale network proof retained: %+v", got.Status)
			}
			expected := 1
			if failure == "network" || failure == "missing evidence" {
				expected = 2
			}
			if calls != expected {
				t.Fatalf("network checked before prerequisites: calls=%d", calls)
			}
		})
	}
}
