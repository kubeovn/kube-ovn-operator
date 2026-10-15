package health

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"strings"
	"sync"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/probe"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func networkFixture(t *testing.T, dual bool) (*operatorv1alpha1.KubeOVNInstallation, client.Client, []operatorv1alpha1.DataPlaneNodeStatus) {
	t.Helper()
	installation, kube := dataPlaneFixture(t)
	installation.Spec.NetworkProbe = &operatorv1alpha1.NetworkProbeSpec{Image: "operator:test"}
	ovs := new(appsv1.DaemonSet)
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: installation.Namespace, Name: "ovs-ovn"}, ovs); err != nil {
		t.Fatal(err)
	}
	ovs.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"}
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ovs)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := chart.NetworkProbeObjects(installation, []*unstructured.Unstructured{{Object: data}})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range objects {
		if object.GetKind() == "Service" {
			service := new(corev1.Service)
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, service); err != nil {
				t.Fatal(err)
			}
			service.UID = "probe-service"
			service.Labels, service.OwnerReferences = ovs.Labels, ovs.OwnerReferences
			service.Spec.ClusterIPs = []string{"10.96.0.9"}
			if dual {
				service.Spec.ClusterIPs = append(service.Spec.ClusterIPs, "fd00:96::9")
			}
			service.Spec.Ports[0].Protocol = corev1.ProtocolTCP
			if err := kube.Create(t.Context(), service); err != nil {
				t.Fatal(err)
			}
			continue
		}
		ds := new(appsv1.DaemonSet)
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, ds); err != nil {
			t.Fatal(err)
		}
		ds.UID = types.UID(ds.Name)
		ds.Labels, ds.OwnerReferences = ovs.Labels, ovs.OwnerReferences
		ds.Generation = 1
		ds.Status = ovs.Status
		if err := kube.Create(t.Context(), ds); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			pod := &corev1.Pod{Name: fmt.Sprintf("%s-%d", ds.Name, i), Namespace: installation.Namespace, UID: types.UID(fmt.Sprintf("%s-pod-%d", ds.Name, i)), Labels: ds.Spec.Template.Labels,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: ds.Name, UID: ds.UID, Controller: new(true)}}, Spec: *ds.Spec.Template.Spec.DeepCopy(),
				Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: probe.Container, ContainerID: "container", Ready: true}}},
			}
			pod.Spec.NodeName = fmt.Sprintf("node-%d", i)
			offset := 10
			if strings.HasSuffix(ds.Name, "-server") {
				offset = 20
			}
			pod.Status.PodIPs = []corev1.PodIP{{IP: fmt.Sprintf("10.10.0.%d", i+offset)}}
			if dual {
				pod.Status.PodIPs = append(pod.Status.PodIPs, corev1.PodIP{IP: fmt.Sprintf("fd00:10::%d", i+offset)})
			}
			if err := kube.Create(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
	}
	var nodes []operatorv1alpha1.DataPlaneNodeStatus
	for i := range 3 {
		nodes = append(nodes, operatorv1alpha1.DataPlaneNodeStatus{Node: fmt.Sprintf("node-%d", i), NodeUID: fmt.Sprintf("node-%d-uid", i)})
	}
	return installation, kube, nodes
}

func TestNetworkObservationRequiresFreshFullCoverage(t *testing.T) {
	for _, scenario := range []string{"healthy", "dual stack", "wrong nonce", "foreign backend", "wrong source", "wrong address", "transport failure", "pod restarted", "pod IP changed", "node replaced", "Service changed", "missing pod", "missing IPv6"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, nodes := networkFixture(t, scenario == "dual stack" || scenario == "missing IPv6")
			if scenario == "missing pod" || scenario == "missing IPv6" {
				pod := new(corev1.Pod)
				key := types.NamespacedName{Namespace: installation.Namespace, Name: probe.Name(string(installation.UID)) + "-client-0"}
				if err := kube.Get(t.Context(), key, pod); err != nil {
					t.Fatal(err)
				}
				if scenario == "missing pod" {
					pod.OwnerReferences[0].UID = "foreign"
					if err := kube.Update(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
				} else {
					pod.Status.PodIPs = pod.Status.PodIPs[:1]
					if err := kube.Status().Update(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
				}
			}
			var change sync.Once
			observer := NetworkObserver{Reader: kube, Runner: runnerFunc(func(ctx context.Context, namespace, podName, container string, command []string) ([]byte, error) {
				if container != probe.Container || len(command) != 3 || command[0] != "/network-probe" || command[1] != "check" {
					return nil, fmt.Errorf("unexpected command")
				}
				if scenario == "transport failure" {
					return nil, fmt.Errorf("connection refused")
				}
				var request probe.Request
				if err := json.Unmarshal([]byte(command[2]), &request); err != nil {
					return nil, err
				}
				source := new(corev1.Pod)
				if err := kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, source); err != nil {
					return nil, err
				}
				servers, err := ownedPods(ctx, kube, installation, probe.ServerApp)
				if err != nil {
					return nil, err
				}
				target := servers[0]
				for _, server := range servers {
					for _, ip := range server.Status.PodIPs {
						if ip.IP == request.Host {
							target = server
						}
					}
				}
				response := probe.Response{Version: probe.ProtocolVersion, Nonce: request.Nonce, Address: request.ExpectedAddress, Source: probe.Identity{Pod: source.Name, PodUID: string(source.UID), Node: source.Spec.NodeName}, Destination: probe.Identity{Pod: target.Name, PodUID: string(target.UID), Node: target.Spec.NodeName}}
				switch scenario {
				case "wrong nonce":
					response.Nonce = "stale"
				case "foreign backend":
					response.Destination.PodUID = "foreign"
				case "wrong source":
					response.Source.PodUID = "foreign"
				case "wrong address":
					response.Address = "10.0.0.99"
				}
				var changeErr error
				change.Do(func() {
					switch scenario {
					case "pod restarted":
						source.Status.ContainerStatuses[0].RestartCount++
						changeErr = kube.Status().Update(ctx, source)
					case "pod IP changed":
						source.Status.PodIPs[0].IP = "10.10.0.99"
						changeErr = kube.Status().Update(ctx, source)
					case "node replaced":
						node := new(corev1.Node)
						if changeErr = kube.Get(ctx, types.NamespacedName{Name: "node-0"}, node); changeErr == nil {
							node.UID = "replacement"
							changeErr = kube.Update(ctx, node)
						}
					case "Service changed":
						service := new(corev1.Service)
						if changeErr = kube.Get(ctx, types.NamespacedName{Namespace: namespace, Name: probe.Name(string(installation.UID))}, service); changeErr == nil {
							service.Spec.ClusterIPs[0] = "10.96.0.10"
							changeErr = kube.Update(ctx, service)
						}
					}
				})
				if changeErr != nil {
					return nil, changeErr
				}
				return json.Marshal(response)
			})}
			report := observer.Observe(t.Context(), installation, nodes)
			if scenario == "healthy" || scenario == "dual stack" {
				expected := int32(12)
				if scenario == "dual stack" {
					expected = 24
				}
				if report.Condition.Status != metav1.ConditionTrue || report.Evidence == nil || report.Evidence.ObservedNodes != 3 || report.Evidence.SuccessfulProbes != expected {
					t.Fatalf("healthy network: %+v", report)
				}
			} else if report.Condition.Status != metav1.ConditionFalse || report.Evidence != nil {
				t.Fatalf("unsafe network accepted: %+v", report)
			}
		})
	}
}

func TestNetworkObservationCannotInferUnconfiguredDataPlane(t *testing.T) {
	observer := new(NetworkObserver)
	installation := &operatorv1alpha1.KubeOVNInstallation{Spec: operatorv1alpha1.KubeOVNInstallationSpec{Mode: operatorv1alpha1.InstallationModeFull}}
	if report := observer.Observe(t.Context(), installation, nil); report.Condition.Status != metav1.ConditionUnknown {
		t.Fatal(report)
	}
	installation.Spec.Mode = operatorv1alpha1.InstallationModeControlPlaneOnly
	if report := observer.Observe(t.Context(), installation, nil); report.Condition.Status != metav1.ConditionTrue || report.Condition.Reason != "NotRequired" {
		t.Fatal(report)
	}
}
