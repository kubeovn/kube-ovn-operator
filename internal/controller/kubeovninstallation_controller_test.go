package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/chart"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileWritesPlanAndSafetyLeases(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{
		TypeMeta:   metav1.TypeMeta{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: types.UID("uid-demo"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Clock: func() time.Time { return time.Unix(10, 0) }}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "demo", Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	plan := new(corev1.ConfigMap)
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + "demo", Namespace: "default"}, plan); err != nil {
		t.Fatalf("plan ConfigMap was not written: %v", err)
	}
	if plan.Data["install-mode"] != "full" || plan.Data["central-replicas"] != "3" {
		t.Fatalf("unexpected plan data: %#v", plan.Data)
	}
	if !strings.Contains(plan.Data["values.yaml"], "installMode: full") {
		t.Fatalf("rendered chart values are missing from plan: %q", plan.Data["values.yaml"])
	}
	for _, name := range []string{installationLeasePrefix + "demo", membershipLeasePrefix + "demo"} {
		if err := client.Get(t.Context(), types.NamespacedName{Name: name, Namespace: "default"}, new(coordinationv1.Lease)); err != nil {
			t.Fatalf("lease %s was not written: %v", name, err)
		}
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: "demo", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseInstalling || got.Status.ObservedVersion != "v1.17.0" {
		t.Fatalf("unexpected status: %#v", got.Status)
	}
}

func TestReconcileBlocksInvalidSpecBeforeWritingChildren(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "invalid", Namespace: "default"}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "latest", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "invalid", Namespace: "default"}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + "invalid", Namespace: "default"}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("invalid spec unexpectedly wrote a plan")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: "invalid", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || len(got.Status.Conditions) != 2 {
		t.Fatalf("unexpected invalid status: %#v", got.Status)
	}
}

type failingRenderer struct{}

func (failingRenderer) Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return chart.Rendered{}, fmt.Errorf("chart dry-run rejected rendered objects")
}

func TestReconcileBlocksWhenRendererFails(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "render-failure", Namespace: "default", Generation: 1}, Spec: operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Renderer: failingRenderer{}}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Get(t.Context(), types.NamespacedName{Name: planConfigMapPrefix + installation.Name, Namespace: installation.Namespace}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("renderer failure unexpectedly wrote a plan")
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || got.Status.Conditions[0].Reason != "SafetyGateBlocked" {
		t.Fatalf("unexpected renderer failure status: %#v", got.Status)
	}
}

type objectRenderer struct{}

func (objectRenderer) Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (chart.Rendered, error) {
	return chart.Rendered{
		ValuesYAML: []byte("installMode: full\n"),
		Objects: []*unstructured.Unstructured{{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"name":      "managed-workload",
				"namespace": "default",
			},
		}}},
	}, nil
}

type recordingWorkload struct {
	dryRunCalls int
	applyCalls  int
	err         error
}

func (r *recordingWorkload) DryRun(context.Context, []*unstructured.Unstructured) error {
	r.dryRunCalls++
	return nil
}

func (r *recordingWorkload) Apply(context.Context, []*unstructured.Unstructured) error {
	r.applyCalls++
	return r.err
}

var _ apply.ApplyClient = (*recordingWorkload)(nil)

func TestReconcileUsesInjectedWorkloadAdapter(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "managed", Namespace: "default", UID: types.UID("uid-managed"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := new(recordingWorkload)
	var gotIdentity apply.InstallationIdentity
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Renderer: objectRenderer{}, WorkloadFactory: func(identity apply.InstallationIdentity) apply.ApplyClient {
		gotIdentity = identity
		return workload
	}}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if workload.applyCalls != 1 || workload.dryRunCalls != 0 {
		t.Fatalf("unexpected workload adapter calls: %#v", workload)
	}
	if gotIdentity.Namespace != installation.Namespace || gotIdentity.Name != installation.Name || gotIdentity.UID != installation.UID {
		t.Fatalf("unexpected workload identity: %#v", gotIdentity)
	}
}

func TestReconcileRecordsWorkloadApplyFailure(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "apply-failure", Namespace: "default", UID: types.UID("uid-apply-failure"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	workload := &recordingWorkload{err: fmt.Errorf("ownership conflict")}
	reconciler := &KubeOVNInstallationReconciler{Client: client, Scheme: scheme, Renderer: objectRenderer{}, Workload: workload}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := client.Get(t.Context(), types.NamespacedName{Name: installation.Name, Namespace: installation.Namespace}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != operatorv1alpha1.PhaseDegraded || got.Status.Upgrade.Step != "Apply" {
		t.Fatalf("unexpected apply failure status: %#v", got.Status)
	}
}

func controllerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
