package recovery

import (
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestLiveKubernetesRecoveryIdentityLookups reads the member-smoke fixtures.
// It exercises real API query semantics without writing resources or data.
func TestLiveKubernetesRecoveryIdentityLookups(t *testing.T) {
	namespace := os.Getenv("RECOVERY_TEST_NAMESPACE")
	if namespace == "" {
		t.Skip("set RECOVERY_TEST_NAMESPACE after the Kind member smoke test")
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	pods := new(corev1.PodList)
	if err := kube.List(t.Context(), pods, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range pods.Items {
		if candidate.DeletionTimestamp != nil || candidate.Spec.NodeName == "" {
			continue
		}
		for _, volume := range candidate.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			pod, err := podByUID(t.Context(), kube, namespace, string(candidate.UID))
			if err != nil {
				t.Fatal(err)
			}
			claim := new(corev1.PersistentVolumeClaim)
			if err := kube.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: volume.PersistentVolumeClaim.ClaimName}, claim); err != nil {
				t.Fatal(err)
			}
			verified, err := (&KubernetesStorageOwnershipVerifier{Reader: kube, Namespace: namespace}).targetPVC(t.Context(), pod, string(claim.UID))
			if err != nil || verified.UID != claim.UID {
				t.Fatalf("live referenced PVC lookup failed: %v", err)
			}
			node := new(corev1.Node)
			if err := kube.Get(t.Context(), client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
				t.Fatal(err)
			}
			member := Member{NodeName: node.Name, Storage: StorageIdentity{PodUID: string(pod.UID), NodeUID: string(node.UID)}}
			if _, err := (&KubernetesFenceEndpointResolver{Reader: kube, Namespace: namespace}).Resolve(t.Context(), member); err != nil {
				t.Fatal(err)
			}
			if _, err := podByUID(t.Context(), kube, "kube-system", string(pod.UID)); err == nil {
				t.Fatal("live lookup accepted a Pod outside the configured namespace")
			}
			t.Logf("Verified API Pod/PVC UID lookups and fence endpoint for %s/%s", namespace, pod.Name)
			return
		}
	}
	t.Fatal("member smoke test left no scheduled Pod with a PVC to verify")
}
