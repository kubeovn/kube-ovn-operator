package controller

import (
	"os"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLiveRecoveryJobAdmission(t *testing.T) {
	namespace := os.Getenv("RECOVERY_TEST_NAMESPACE")
	if namespace == "" {
		t.Skip("set RECOVERY_TEST_NAMESPACE for real API server admission")
	}
	_, _, spec := recoveryJobFixture()
	spec.Namespace = namespace
	desired, err := spec.Job(nil)
	if err != nil {
		t.Fatal(err)
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: recoveryJobTestScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	desired.Spec.Suspend = new(true)
	admitted := desired.DeepCopy()
	if err := kube.Create(t.Context(), admitted, client.DryRunAll); err != nil {
		t.Fatal(err)
	}
	if admitted.Spec.Selector == nil || !matchesRecoveryJobSpec(desired, admitted) {
		t.Fatalf("recovery Job comparison rejected the API server defaults; admitted spec: %+v", admitted.Spec)
	}
	admitted.Spec.Template.Spec.Containers[0].Args = []string{"inspect", "/wrong/database"}
	if matchesRecoveryJobSpec(desired, admitted) {
		t.Fatal("recovery Job comparison accepted altered native arguments")
	}
}
