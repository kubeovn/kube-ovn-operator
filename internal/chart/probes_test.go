package chart

import (
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/probe"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestNetworkProbeResourcesKeepOVSPlacementWithoutHostPrivileges(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "test", Namespace: "network", UID: "installation", Spec: operatorv1alpha1.KubeOVNInstallationSpec{NetworkProbe: &operatorv1alpha1.NetworkProbeSpec{Image: "operator:test"}}}
	ovs := &appsv1.DaemonSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"}, Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ovs"}}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		HostNetwork: true, HostPID: true, ServiceAccountName: "privileged", NodeSelector: map[string]string{"kubernetes.io/os": "linux"}, Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
		Volumes: []corev1.Volume{{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}},
	}}}}
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ovs)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := NetworkProbeObjects(installation, []*unstructured.Unstructured{{Object: data}})
	if err != nil || len(objects) != 3 {
		t.Fatalf("probe resources: %d %v", len(objects), err)
	}
	for _, object := range objects[:2] {
		var ds appsv1.DaemonSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &ds); err != nil {
			t.Fatal(err)
		}
		pod := ds.Spec.Template.Spec
		if pod.HostNetwork || pod.HostPID || len(pod.Volumes) != 0 || pod.ServiceAccountName != "" || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
			t.Fatalf("inherited credentials or host privileges: %+v", pod)
		}
		if pod.NodeSelector["kubernetes.io/os"] != "linux" || len(pod.Tolerations) != 1 || pod.Containers[0].Image != "operator:test" || pod.Containers[0].Command[0] != "/network-probe" {
			t.Fatalf("lost probe placement/image: %+v", pod)
		}
		if ds.Spec.Selector.MatchLabels["operator.kubeovn.io/probe-installation"] != string(installation.UID) {
			t.Fatal("selector not scoped to installation UID")
		}
	}
	if objects[2].GetName() != probe.Name(string(installation.UID)) {
		t.Fatal("unexpected Service name")
	}
	if _, err := NetworkProbeObjects(installation, nil); err == nil {
		t.Fatal("missing OVS scheduling source accepted")
	}
}
