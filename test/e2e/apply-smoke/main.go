package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
)

const workloadName = "kube-ovn-operator-workload-smoke"

func main() {
	var namespace string
	var installation string
	var installationUID string
	var name string
	var externalOwner bool
	flag.StringVar(&namespace, "namespace", "kube-ovn-operator-system", "Namespace for the smoke ConfigMap")
	flag.StringVar(&installation, "installation", "kube-ovn", "Installation name")
	flag.StringVar(&installationUID, "installation-uid", "", "Installation UID")
	flag.StringVar(&name, "name", workloadName, "ConfigMap name")
	flag.BoolVar(&externalOwner, "external-owner", false, "Verify that an externally owned ConfigMap is rejected")
	flag.Parse()
	if installationUID == "" {
		fatalf("-installation-uid is required")
	}

	config, err := kubeConfig()
	if err != nil {
		fatalf("load Kubernetes config: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		fatalf("create dynamic client: %v", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		fatalf("create discovery client: %v", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	adapter := &apply.Client{
		Dynamic: dynamicClient,
		Mapper:  mapper,
		Identity: apply.InstallationIdentity{
			Namespace: namespace,
			Name:      installation,
			UID:       types.UID(installationUID),
		},
	}
	resource := dynamicClient.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(namespace)
	if externalOwner {
		if err := ensureExternalObject(context.Background(), resource, namespace, name); err != nil {
			fatalf("create external owner object: %v", err)
		}
		if err := adapter.Apply(context.Background(), []*unstructured.Unstructured{workload(false, namespace, name)}); !errors.Is(err, apply.ErrOwnershipConflict) {
			fatalf("external owner was not rejected: %v", err)
		}
		fmt.Println("external owner rejected")
		return
	}
	if err := adapter.Apply(context.Background(), []*unstructured.Unstructured{workload(false, namespace, name)}); err != nil {
		fatalf("apply workload: %v", err)
	}
	if err := adapter.Apply(context.Background(), []*unstructured.Unstructured{workload(false, namespace, name)}); err != nil {
		fatalf("repeat workload apply: %v", err)
	}
	fmt.Println("server-side dry-run/apply succeeded")
}

func kubeConfig() (*rest.Config, error) {
	if kubeconfig := os.Getenv("KUBECONFIG"); kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return ctrl.GetConfig()
}

func workload(external bool, namespace, name string) *unstructured.Unstructured {
	labels := map[string]any{}
	if external {
		labels[apply.ManagedByLabel] = "helm"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
			"labels":    labels,
		},
		"data": map[string]any{"smoke": "true"},
	}}
}

func ensureExternalObject(ctx context.Context, resource dynamic.ResourceInterface, namespace, name string) error {
	_, err := resource.Create(ctx, workload(true, namespace, name), metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
