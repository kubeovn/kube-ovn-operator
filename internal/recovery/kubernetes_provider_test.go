package recovery

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKubernetesFenceEndpointResolverBindsPodAndNode(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "central-0", Namespace: "ovn", UID: types.UID("pod-uid")}, Spec: corev1.PodSpec{NodeName: "node-0"}, Status: corev1.PodStatus{PodIP: "192.0.2.10"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0", UID: types.UID("node-uid")}}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod, node).
		WithIndex(&corev1.Pod{}, "metadata.uid", func(object client.Object) []string { return []string{string(object.GetUID())} }).Build()
	member := Member{Name: "central-0", NodeName: "node-0", Storage: StorageIdentity{PodUID: "pod-uid", NodeUID: "node-uid"}}
	endpoint, err := (&KubernetesFenceEndpointResolver{Reader: kube, Namespace: "ovn"}).Resolve(t.Context(), member)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Namespace != "ovn" || endpoint.Endpoint != "central-0" || endpoint.Node != "192.0.2.10" || endpoint.Container != "ovn-central" {
		t.Fatalf("unexpected fence endpoint: %+v", endpoint)
	}
}

func TestKubernetesFenceEndpointResolverRejectsNodeDrift(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "central-0", Namespace: "ovn", UID: types.UID("pod-uid")}, Spec: corev1.PodSpec{NodeName: "node-0"}, Status: corev1.PodStatus{PodIP: "192.0.2.10"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0", UID: types.UID("replacement-node")}}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod, node).
		WithIndex(&corev1.Pod{}, "metadata.uid", func(object client.Object) []string { return []string{string(object.GetUID())} }).Build()
	member := Member{Name: "central-0", NodeName: "node-0", Storage: StorageIdentity{PodUID: "pod-uid", NodeUID: "original-node"}}
	if _, err := (&KubernetesFenceEndpointResolver{Reader: kube, Namespace: "ovn"}).Resolve(t.Context(), member); err == nil {
		t.Fatal("node identity drift was accepted")
	}
}
