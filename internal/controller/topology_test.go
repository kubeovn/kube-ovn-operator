package controller

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
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

func TestRetainedChartUsesCurrentPeersAndStorageNodes(t *testing.T) {
	for _, hosts := range [][]string{{"192.0.2.1", "192.0.2.4", "192.0.2.5"}, {"2001:db8::1", "2001:db8::4", "2001:db8::5"}} {
		for _, chartMembers := range []int{1, 2} {
			t.Run(hosts[0]+"/chart-members-"+strconv.Itoa(chartMembers), func(t *testing.T) {
				retained := additionTopologyFixture()
				for i := range retained.Members {
					retained.Members[i].Workload.Name = "ovn-central"
					retained.Members[i].Workload.Kind = "Deployment"
					if i >= chartMembers {
						retained.Members[i].Workload.Kind = "StatefulSet"
					}
					for j := range retained.Members[i].Databases {
						retained.Members[i].Databases[j].ClientAddress = "tcp:" + net.JoinHostPort(hosts[i], strconv.Itoa(6641+j))
					}
				}
				deployment := &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central"},
					"spec": map[string]any{"replicas": int64(3), "template": map[string]any{"spec": map[string]any{
						"containers": []any{map[string]any{"name": "ovn-central", "env": []any{map[string]any{"name": "NODE_IPS", "value": "retired-bootstrap-peers"}}}},
						"affinity": map[string]any{"nodeAffinity": map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
							"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{map[string]any{"key": "zone", "operator": "In", "values": []any{"original-zone"}}}}},
						}}},
					}}},
				}}
				if err := applyRetainedChartDeploymentReplicas([]*unstructured.Unstructured{deployment}, retained); err != nil {
					t.Fatal(err)
				}
				containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
				env := containers[0].(map[string]any)["env"].([]any)[0].(map[string]any)
				if env["value"] != strings.Join(hosts, ",") {
					t.Fatalf("northd retained bootstrap peers: %v", env)
				}
				terms, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
				if len(terms) != chartMembers {
					t.Fatalf("expected one field-selector OR term per retained Chart node: %v", terms)
				}
				for i, value := range terms {
					term := value.(map[string]any)
					fields, ok := term["matchFields"].([]any)
					if !ok || len(fields) != 1 || !reflect.DeepEqual(fields[0].(map[string]any)["values"], []any{"node-" + strconv.Itoa(i)}) || len(term["matchExpressions"].([]any)) != 1 {
						t.Fatalf("field selector must have one value and retain original affinity: %v", term)
					}
				}
				stable := deployment.DeepCopy()
				if err := applyRetainedChartDeploymentReplicas([]*unstructured.Unstructured{deployment}, retained); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(stable.Object, deployment.Object) {
					t.Fatal("retained Chart transformation is not idempotent")
				}
				strategy, _, _ := unstructured.NestedMap(deployment.Object, "spec", "strategy")
				if strategy["type"] != "RollingUpdate" || strategy["rollingUpdate"].(map[string]any)["maxSurge"] != int64(0) || strategy["rollingUpdate"].(map[string]any)["maxUnavailable"] != int64(1) {
					t.Fatalf("central rollout is not serial: %v", strategy)
				}
			})
		}
	}
}

func TestRetainedChartLeavesUnchangedBootstrapTemplate(t *testing.T) {
	retained := additionTopologyFixture()
	for i := range retained.Members {
		retained.Members[i].Workload.Name = "ovn-central"
	}
	deployment := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central"}, "spec": map[string]any{"replicas": int64(3), "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "ovn-central", "env": []any{map[string]any{"name": "NODE_IPS", "value": "192.0.2.1,192.0.2.2,192.0.2.3"}}}}}}}}}
	original := deployment.DeepCopy()
	if err := applyRetainedChartDeploymentReplicas([]*unstructured.Unstructured{deployment}, retained); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deployment.Object, original.Object) {
		t.Fatal("retaining an unchanged bootstrap topology introduced an unauthorized manifest change")
	}
}

func TestRetainedChartNodeTermsIntersectExistingConstraints(t *testing.T) {
	for _, tc := range []struct {
		name, operator, value string
		empty                 bool
		want                  []string
	}{
		{name: "existing include", operator: "In", value: "node-1", want: []string{"node-1"}},
		{name: "existing exclude", operator: "NotIn", value: "node-1", want: []string{"node-0"}},
		{name: "disjoint include", operator: "In", value: "outside"},
		{name: "unrelated exclude", operator: "NotIn", value: "outside", want: []string{"node-0", "node-1"}},
		{name: "explicit empty term", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			term := map[string]any{}
			if !tc.empty {
				term["matchExpressions"] = []any{map[string]any{"key": "zone", "operator": "In", "values": []any{"original"}}}
				term["matchFields"] = []any{map[string]any{"key": "metadata.name", "operator": tc.operator, "values": []any{tc.value}}}
			}
			original := (&unstructured.Unstructured{Object: term}).DeepCopy().Object
			got, err := retainedChartNodeTerms([]any{term}, []string{"node-0", "node-1"})
			if !reflect.DeepEqual(original, term) {
				t.Fatal("existing affinity was mutated")
			}
			if len(tc.want) == 0 {
				if err == nil {
					t.Fatalf("affinity admitting no retained storage node was broadened: %v", got)
				}
				return
			}
			if err != nil || len(got) != len(tc.want) {
				t.Fatalf("intersection=%v err=%v", got, err)
			}
			for i, value := range got {
				current := value.(map[string]any)
				if !reflect.DeepEqual(current["matchExpressions"], term["matchExpressions"]) {
					t.Fatal("original label constraint changed")
				}
				include := ""
				for _, value := range current["matchFields"].([]any) {
					field := value.(map[string]any)
					values := field["values"].([]any)
					if len(values) != 1 {
						t.Fatal("API-invalid field selector")
					}
					if field["operator"] == "In" {
						include = values[0].(string)
					}
				}
				if include != tc.want[i] {
					t.Fatalf("term matches %q, expected %q", include, tc.want[i])
				}
			}
			again, err := retainedChartNodeTerms(got, []string{"node-0", "node-1"})
			if err != nil || !reflect.DeepEqual(got, again) {
				t.Fatalf("intersection not idempotent: %v %v", again, err)
			}
		})
	}
}
