package recovery

import (
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

func TestKubernetesStorageOwnershipVerifierBindsLiveIdentities(t *testing.T) {
	root := t.TempDir()
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "central-0", Namespace: "ovn", UID: types.UID("pod-uid"), OwnerReferences: []metav1.OwnerReference{{UID: types.UID("workload-uid"), Controller: &controller}}},
		Spec: corev1.PodSpec{
			NodeName: "node-0",
			Volumes:  []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "central-data"}}}},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://target"}}},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0", UID: types.UID("node-uid")}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "central-data", Namespace: "ovn", UID: types.UID("volume-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-target"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	scheme := testScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, node, pvc).Build()
	verifier := &KubernetesStorageOwnershipVerifier{Reader: kube, Namespace: "ovn"}
	member := Member{Name: "target", NodeName: "node-0", Storage: StorageIdentity{NodeUID: "node-uid", WorkloadUID: "workload-uid", PodUID: "pod-uid", ContainerID: "containerd://target", VolumeUID: "volume-uid"}}
	if err := verifier.Verify(t.Context(), member, filepath.Join(root, "target")); err == nil {
		t.Fatal("missing target root was accepted")
	}
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(t.Context(), member, target); err != nil {
		t.Fatal(err)
	}
}

func TestKubernetesStorageOwnershipVerifierRejectsIdentityDrift(t *testing.T) {
	root := t.TempDir()
	controller := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "central-0", Namespace: "ovn", UID: types.UID("pod-uid"), OwnerReferences: []metav1.OwnerReference{{UID: types.UID("wrong-workload"), Controller: &controller}}},
		Spec: corev1.PodSpec{
			NodeName: "node-0",
			Volumes:  []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "central-data"}}}},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://target"}}},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0", UID: types.UID("node-uid")}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "central-data", Namespace: "ovn", UID: types.UID("volume-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-target"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod, node, pvc).Build()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	member := Member{Name: "target", NodeName: "node-0", Storage: StorageIdentity{NodeUID: "node-uid", WorkloadUID: "workload-uid", PodUID: "pod-uid", ContainerID: "containerd://target", VolumeUID: "volume-uid"}}
	if err := (&KubernetesStorageOwnershipVerifier{Reader: kube, Namespace: "ovn"}).Verify(t.Context(), member, target); err == nil {
		t.Fatal("workload owner drift was accepted")
	}
}

func TestPodOwnershipLookupRejectsWrongNamespaceAndUID(t *testing.T) {
	pod := &corev1.Pod{Name: "central-0", Namespace: "ovn", UID: "pod-uid"}
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pod).Build()
	for _, tc := range []struct{ namespace, uid string }{
		{"", "pod-uid"}, {"ovn", ""}, {"other", "pod-uid"}, {"ovn", "replacement-uid"},
	} {
		t.Run(tc.namespace+"/"+tc.uid, func(t *testing.T) {
			if _, err := podByUID(t.Context(), kube, tc.namespace, tc.uid); err == nil {
				t.Fatal("out-of-scope or stale Pod identity was accepted")
			}
		})
	}
}

func TestTargetPVCRejectsReplacementAndUnreferencedClaims(t *testing.T) {
	for _, tc := range []struct {
		name, namespace, uid string
		deleting             bool
	}{
		{name: "replacement", namespace: "ovn", uid: "new-uid"},
		{name: "other-namespace", namespace: "other", uid: "planned-uid"},
		{name: "deleting", namespace: "ovn", uid: "planned-uid", deleting: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pvc := &corev1.PersistentVolumeClaim{Name: "data", Namespace: tc.namespace, UID: types.UID(tc.uid)}
			if tc.deleting {
				pvc.DeletionTimestamp = new(metav1.Now())
				pvc.Finalizers = []string{"kubernetes.io/pvc-protection"}
			}
			unreferenced := &corev1.PersistentVolumeClaim{Name: "unreferenced", Namespace: "ovn", UID: "planned-uid"}
			if tc.uid == "planned-uid" {
				unreferenced.UID = "unrelated-uid"
			}
			kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(pvc, unreferenced).Build()
			pod := &corev1.Pod{Namespace: "ovn", Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}}
			if _, err := (&KubernetesStorageOwnershipVerifier{Reader: kube}).targetPVC(t.Context(), pod, "planned-uid"); err == nil {
				t.Fatal("stale, unreferenced, cross-namespace or deleting PVC was accepted")
			}
		})
	}
}
