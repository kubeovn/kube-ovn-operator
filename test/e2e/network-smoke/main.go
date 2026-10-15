package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/probe"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/restmapper"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	config, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "probe-smoke", Namespace: "kube-ovn-operator-system", Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}, NetworkProbe: &operatorv1alpha1.NetworkProbeSpec{Image: "kube-ovn-operator-probe:e2e"}}}
	if err := kube.Create(ctx, installation); err != nil {
		return err
	}
	// The existing kind CNI supplies networking. This tests the probe mechanism,
	// not Kube-OVN installation or OVN runtime health.
	ovsTemplate := &appsv1.DaemonSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"}, Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "ovs"}}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux"}, Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}}}}}}
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ovsTemplate)
	if err != nil {
		return err
	}
	objects, err := chart.NetworkProbeObjects(installation, []*unstructured.Unstructured{{Object: data}})
	if err != nil {
		return err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return err
	}
	adapter := &apply.Client{Dynamic: dynamicClient, Mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient)), Identity: apply.InstallationIdentity{Namespace: installation.Namespace, Name: installation.Name, UID: installation.UID}}
	if err := adapter.Apply(ctx, objects); err != nil {
		return err
	}
	nodeList := new(corev1.NodeList)
	if err := kube.List(ctx, nodeList, client.MatchingLabels{"kubernetes.io/os": "linux"}); err != nil {
		return err
	}
	var nodes []operatorv1alpha1.DataPlaneNodeStatus
	for _, node := range nodeList.Items {
		nodes = append(nodes, operatorv1alpha1.DataPlaneNodeStatus{Node: node.Name, NodeUID: string(node.UID)})
	}
	runner, err := health.NewPodExecutor(config)
	if err != nil {
		return err
	}
	observer := health.NetworkObserver{Reader: kube, Runner: runner}
	if err := wait(ctx, func() error {
		report := observer.Observe(ctx, installation, nodes)
		if report.Condition.Status != metav1.ConditionTrue {
			return fmt.Errorf("network not ready: %s", report.Condition.Message)
		}
		fmt.Printf("network probes passed: %d nodes, %v, %d checks\n", report.Evidence.ObservedNodes, report.Evidence.AddressFamilies, report.Evidence.SuccessfulProbes)
		return nil
	}); err != nil {
		return err
	}
	if err := adapter.Apply(ctx, objects); err != nil {
		return fmt.Errorf("repeat apply: %w", err)
	}
	fmt.Println("repeated probe resource apply succeeded")
	// The fault injector owns its Update fields and must restore them itself;
	// non-forcing apply must not delete a separately owned literal value.
	serverKey := types.NamespacedName{Namespace: installation.Namespace, Name: probe.Name(string(installation.UID)) + "-server"}
	originalIdentity, err := replaceIdentity(ctx, kube, serverKey, corev1.EnvVar{Name: "POD_UID", Value: "foreign-backend"})
	if err != nil {
		return err
	}
	if err := wait(ctx, func() error {
		observed, err := adapter.Observe(ctx, objects)
		if err != nil {
			return err
		}
		if !observed.Ready {
			return fmt.Errorf("fault rollout incomplete")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := wait(ctx, func() error {
		report := observer.Observe(ctx, installation, nodes)
		identityFailure := strings.Contains(report.Condition.Message, "destination identity mismatch") || strings.Contains(report.Condition.Message, "service probe reached an unobserved backend")
		if report.Condition.Status != metav1.ConditionFalse || report.Evidence != nil || !identityFailure {
			return fmt.Errorf("backend identity rejection has not been observed: %s", report.Condition.Message)
		}
		return nil
	}); err != nil {
		return err
	}
	fmt.Println("healthy pods with incorrect backend identities were rejected")
	if _, err := replaceIdentity(ctx, kube, serverKey, originalIdentity); err != nil {
		return err
	}
	return wait(ctx, func() error {
		// Rollout status can change resourceVersion between the adapter's read
		// and dry-run. Retry the full batch, as the reconciler does.
		if err := adapter.Apply(ctx, objects); err != nil {
			return err
		}
		report := observer.Observe(ctx, installation, nodes)
		if report.Condition.Status != metav1.ConditionTrue {
			return fmt.Errorf("network has not recovered: %s", report.Condition.Message)
		}
		fmt.Println("network probe verification recovered after restoring pod identity")
		return nil
	})
}

func replaceIdentity(ctx context.Context, kube client.Client, key types.NamespacedName, identity corev1.EnvVar) (corev1.EnvVar, error) {
	server := new(appsv1.DaemonSet)
	if err := kube.Get(ctx, key, server); err != nil {
		return corev1.EnvVar{}, err
	}
	for i, env := range server.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "POD_UID" {
			server.Spec.Template.Spec.Containers[0].Env[i] = identity
			return env, kube.Update(ctx, server, client.FieldOwner("network-probe-smoke-fault"))
		}
	}
	return corev1.EnvVar{}, fmt.Errorf("server probe has no POD_UID environment variable")
}

func wait(ctx context.Context, check func() error) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		err := check()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("probe smoke timed out: %w", err)
		case <-ticker.C:
		}
	}
}
