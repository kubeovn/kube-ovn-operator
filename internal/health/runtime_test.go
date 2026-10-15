package health

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestNorthdRuntimeRequiresOneActiveAndBothConnections(t *testing.T) {
	for _, scenario := range []string{"healthy", "all standby", "two active", "paused", "NB disconnected", "SB disconnected", "pod restarted"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, _ := healthFixture(t)
			installation.Spec.Mode = operatorv1alpha1.InstallationModeControlPlaneOnly
			observer := RuntimeObserver{Reader: kube, Runner: runnerFunc(func(_ context.Context, ns, pod, container string, command []string) ([]byte, error) {
				if container != "ovn-central" || command[0] != "ovn-appctl" || command[3] != "ovn-northd" {
					return nil, fmt.Errorf("unexpected command target")
				}
				if command[4] == "status" {
					if scenario == "all standby" {
						return []byte("Status: standby\n"), nil
					}
					if scenario == "two active" || pod == "ovn-central-0" {
						return []byte("Status: active\n"), nil
					}
					if scenario == "paused" {
						return []byte("Status: paused\n"), nil
					}
					return []byte("Status: standby\n"), nil
				}
				if scenario == "NB disconnected" && command[4] == "nb-connection-status" || scenario == "SB disconnected" && command[4] == "sb-connection-status" {
					return []byte("not connected\n"), nil
				}
				if scenario == "pod restarted" {
					current := new(corev1.Pod)
					if err := kube.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: pod}, current); err != nil {
						return nil, err
					}
					current.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "new-container", RestartCount: 1, Ready: true}}
					if err := kube.Status().Update(t.Context(), current); err != nil {
						return nil, err
					}
				}
				return []byte("connected\n"), nil
			})}
			report := observer.Observe(t.Context(), installation)
			if report.DataPlane.Status != metav1.ConditionTrue || report.DataPlane.Reason != "NotRequired" {
				t.Fatalf("control-plane-only data plane: %+v", report)
			}
			if scenario == "healthy" {
				if report.Northd.Status != metav1.ConditionTrue || len(report.NorthdProcesses) != 3 {
					t.Fatalf("healthy northd: %+v", report)
				}
			} else if report.Northd.Status != metav1.ConditionFalse || len(report.NorthdProcesses) != 0 {
				t.Fatalf("unsafe northd accepted: %+v", report)
			}
		})
	}
}

func dataPlaneFixture(t *testing.T) (*operatorv1alpha1.KubeOVNInstallation, client.Client) {
	t.Helper()
	installation, kube, objects := healthFixture(t)
	installation.Spec.Mode = operatorv1alpha1.InstallationModeDataPlaneOnly
	workload := objects[0].(*appsv1.Deployment)
	ds := &appsv1.DaemonSet{Name: "ovs-ovn", Namespace: installation.Namespace, UID: "ovs-daemonset", Generation: 1, Labels: workload.Labels, OwnerReferences: workload.OwnerReferences,
		Spec:   appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ovs"}}},
		Status: appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 3, UpdatedNumberScheduled: 3, NumberReady: 3, NumberAvailable: 3},
	}
	if err := kube.Create(t.Context(), ds); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		name := fmt.Sprintf("node-%d", i)
		node := &corev1.Node{Name: name, UID: types.UID(name + "-uid"), Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
		pod := &corev1.Pod{Name: fmt.Sprintf("ovs-%d", i), Namespace: installation.Namespace, UID: types.UID(fmt.Sprintf("ovs-pod-%d", i)), Labels: map[string]string{"app": "ovs"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: new(true)}}, Spec: corev1.PodSpec{NodeName: name}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		for _, object := range []client.Object{node, pod} {
			if err := kube.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
		}
	}
	return installation, kube
}

func TestDataPlaneRuntimeRequiresCompleteUniqueAndConnectedNodes(t *testing.T) {
	for _, scenario := range []string{"healthy", "SB disconnected", "bridge missing", "invalid chassis", "duplicate chassis", "node unready", "missing pod", "duplicate node", "pod replaced", "node replaced", "daemonset changed", "transport failure"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube := dataPlaneFixture(t)
			if scenario == "node unready" {
				node := new(corev1.Node)
				if err := kube.Get(t.Context(), types.NamespacedName{Name: "node-0"}, node); err != nil {
					t.Fatal(err)
				}
				node.Status.Conditions[0].Status = corev1.ConditionFalse
				if err := kube.Status().Update(t.Context(), node); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "missing pod" || scenario == "duplicate node" {
				pod := new(corev1.Pod)
				if err := kube.Get(t.Context(), types.NamespacedName{Namespace: installation.Namespace, Name: "ovs-1"}, pod); err != nil {
					t.Fatal(err)
				}
				if scenario == "missing pod" {
					pod.OwnerReferences[0].UID = "foreign-owner"
				} else {
					pod.Spec.NodeName = "node-0"
				}
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			var change sync.Once
			observer := RuntimeObserver{Reader: kube, Runner: runnerFunc(func(_ context.Context, ns, pod, container string, command []string) ([]byte, error) {
				if container != "openvswitch" {
					return nil, fmt.Errorf("unexpected container")
				}
				var changeErr error
				change.Do(func() {
					switch scenario {
					case "pod replaced":
						current := new(corev1.Pod)
						if err := kube.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: pod}, current); err != nil {
							changeErr = err
							return
						}
						current.UID = "replacement-pod"
						changeErr = kube.Update(t.Context(), current)
					case "node replaced":
						current := new(corev1.Node)
						if err := kube.Get(t.Context(), types.NamespacedName{Name: "node-0"}, current); err != nil {
							changeErr = err
							return
						}
						current.UID = "replacement-node"
						changeErr = kube.Update(t.Context(), current)
					case "daemonset changed":
						current := new(appsv1.DaemonSet)
						if err := kube.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: "ovs-ovn"}, current); err != nil {
							changeErr = err
							return
						}
						current.Generation++
						changeErr = kube.Update(t.Context(), current)
					}
				})
				if changeErr != nil {
					return nil, changeErr
				}
				if scenario == "transport failure" {
					return nil, fmt.Errorf("exec unavailable")
				}
				switch command[0] {
				case "ovn-appctl":
					if scenario == "SB disconnected" {
						return []byte("not connected"), nil
					}
					return []byte("connected"), nil
				case "ovs-appctl":
					if scenario == "bridge missing" {
						return []byte("br-ex"), nil
					}
					return []byte("br-ex\nbr-int\n"), nil
				case "ovs-vsctl":
					if scenario == "invalid chassis" {
						return []byte("[]"), nil
					}
					id := strings.TrimPrefix(pod, "ovs-")
					if scenario == "duplicate chassis" {
						id = "0"
					}
					return []byte(fmt.Sprintf(`"1234567%s-1122-3344-5566-778899aabbcc"`, id)), nil
				}
				return nil, fmt.Errorf("unexpected command")
			})}
			report := observer.Observe(t.Context(), installation)
			if report.Northd.Status != metav1.ConditionTrue || report.Northd.Reason != "NotRequired" {
				t.Fatalf("data-plane-only northd: %+v", report)
			}
			if scenario == "healthy" {
				if report.DataPlane.Status != metav1.ConditionTrue || len(report.Nodes) != 3 || report.Nodes[0].NodeUID != "node-0-uid" {
					t.Fatalf("healthy data plane: %+v", report)
				}
			} else if report.DataPlane.Status != metav1.ConditionFalse || len(report.Nodes) != 0 {
				t.Fatalf("unsafe data plane accepted: %+v", report)
			}
		})
	}
}

func TestRuntimeResponsesRequireExactPositiveEvidence(t *testing.T) {
	for _, value := range []string{"", "not connected", "connected\nnot connected", "connection pending"} {
		if connectionReady([]byte(value)) {
			t.Fatalf("accepted connection %q", value)
		}
	}
	for _, value := range []string{"", "Status: paused", "Status: active\nStatus: standby", "Status: unknown"} {
		if _, err := parseNorthdState([]byte(value)); err == nil {
			t.Fatalf("accepted northd state %q", value)
		}
	}
	for _, value := range []string{"", "br-internal", "br-int br-int"} {
		if integrationBridgeReady([]byte(value)) {
			t.Fatalf("accepted bridge response %q", value)
		}
	}
	for _, value := range []string{"", "[]", "bad-id", "00000000-0000-0000-0000-000000000000"} {
		if _, err := parseChassisID([]byte(value)); err == nil {
			t.Fatalf("accepted chassis %q", value)
		}
	}
}
