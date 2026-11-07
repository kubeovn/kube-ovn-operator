package workload

import (
	"os"
	"strings"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/fenceagent"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestLiveFencedMemberAdmission validates the immutable workload comparison
// against API defaulting without creating a workload or running any Pods.
func TestLiveFencedMemberAdmission(t *testing.T) {
	namespace := os.Getenv("WORKLOAD_TEST_NAMESPACE")
	if namespace == "" {
		t.Skip("set WORKLOAD_TEST_NAMESPACE for real API server admission")
	}
	f := setup(t, nil)
	f.p.fenceAgent = &fenceagent.Config{Image: "registry.example/fence-agent@sha256:" + strings.Repeat("b", 64), ServerTLSSecret: "server-tls", ClientCASecret: "client-ca", Port: 8443}
	set := f.p.statefulSet(f.request, Reference{Name: "member-admission", UID: "record"}, &corev1.ConfigMap{Name: "launch"})
	set.Namespace = namespace
	if err := f.p.injectMemberFenceAgent(set); err != nil {
		t.Fatal(err)
	}
	expected := set.DeepCopy()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	kube, err := client.New(config, client.Options{Scheme: f.kube.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(t.Context(), set, client.DryRunAll); err != nil {
		t.Fatal(err)
	}
	if !matchesPod(expected.Spec.Template.Spec, set.Spec.Template.Spec) {
		t.Fatal("API server defaulting changed the protected fenced member contract")
	}
}
