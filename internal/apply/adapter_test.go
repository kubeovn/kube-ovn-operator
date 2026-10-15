package apply

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

var testIdentity = InstallationIdentity{Namespace: "default", Name: "demo", UID: types.UID("uid-demo")}

func TestApplyRunsDryRunBeforePersistingAndIsIdempotent(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), ownedConfigMap("workload", testIdentity))
	patches := 0
	installApplyReactor(client, &patches, nil)
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}

	object := configMap("workload", nil)
	if err := adapter.Apply(t.Context(), []*unstructured.Unstructured{object}); err != nil {
		t.Fatal(err)
	}
	if patches != 2 {
		t.Fatalf("expected one dry-run and one apply patch, got %d", patches)
	}
	if object.GetLabels() != nil || len(object.GetOwnerReferences()) != 0 {
		t.Fatalf("adapter mutated the rendered object: %#v", object.Object)
	}
	got, err := client.Resource(corev1.SchemeGroupVersion.WithResource("configmaps")).Namespace("default").Get(t.Context(), "workload", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertIdentityLabels(t, got, testIdentity)
	if err := adapter.Apply(t.Context(), []*unstructured.Unstructured{object}); err != nil {
		t.Fatal(err)
	}
	if patches != 4 {
		t.Fatalf("expected repeated apply to remain idempotent, got %d patches", patches)
	}
}

func TestApplyDryRunFailureDoesNotApply(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	patches := 0
	installApplyReactor(client, &patches, errors.New("dry-run rejected"))
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}
	if err := adapter.Apply(t.Context(), []*unstructured.Unstructured{configMap("workload", nil)}); err == nil {
		t.Fatal("dry-run failure unexpectedly succeeded")
	}
	if patches != 1 {
		t.Fatalf("dry-run failure performed %d patch calls, want 1", patches)
	}
	if _, err := client.Resource(corev1.SchemeGroupVersion.WithResource("configmaps")).Namespace("default").Get(t.Context(), "workload", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("dry-run failure persisted resource: %v", err)
	}
}

func TestApplyRejectsExternalOwner(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), configMap("workload", map[string]string{ManagedByLabel: "helm"}))
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}
	err := adapter.Apply(t.Context(), []*unstructured.Unstructured{configMap("workload", nil)})
	if !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("expected ownership conflict, got %v", err)
	}
}

func TestApplyRejectsMismatchedInstallationAndControllerOwner(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), configMap("workload", map[string]string{
		ManagedByLabel:             ManagedByValue,
		InstallationLabel:          "other",
		InstallationNamespaceLabel: "default",
		InstallationUIDLabel:       "uid-other",
	}))
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}
	if err := adapter.DryRun(t.Context(), []*unstructured.Unstructured{configMap("workload", nil)}); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("expected installation ownership conflict, got %v", err)
	}

	owned := ownedConfigMap("owned", testIdentity)
	controller := true
	owned.SetOwnerReferences([]metav1.OwnerReference{{Name: "other", UID: types.UID("uid-other"), Controller: &controller}})
	client = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), owned)
	adapter.Dynamic = client
	if err := adapter.DryRun(t.Context(), []*unstructured.Unstructured{configMap("owned", nil)}); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("expected controller owner conflict, got %v", err)
	}
}

func TestApplyRejectsInvalidDesiredIdentity(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}
	object := configMap("workload", map[string]string{InstallationLabel: "other"})
	if err := adapter.DryRun(t.Context(), []*unstructured.Unstructured{object}); !errors.Is(err, ErrOwnershipConflict) {
		t.Fatalf("expected desired ownership conflict, got %v", err)
	}
}

func TestApplyRejectsNamespaceAndUIDMismatch(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	adapter := &Client{Dynamic: client, Mapper: testMapper(), Identity: testIdentity}
	wrongNamespace := configMap("workload", nil)
	wrongNamespace.SetNamespace("other")
	if err := adapter.DryRun(t.Context(), []*unstructured.Unstructured{wrongNamespace}); err == nil {
		t.Fatal("namespace mismatch unexpectedly succeeded")
	}
	adapter.Identity.UID = ""
	if err := adapter.DryRun(t.Context(), []*unstructured.Unstructured{configMap("workload", nil)}); err == nil {
		t.Fatal("missing installation UID unexpectedly succeeded")
	}
}

func TestDryRunUsesServerSideApplyAndCreateOptions(t *testing.T) {
	resource := new(recordingResource)
	item := preparedObject{object: configMap("workload", nil), resource: resource}
	if err := dryRunAll(t.Context(), []preparedObject{item}); err != nil {
		t.Fatal(err)
	}
	if len(resource.patchOptions) != 1 || len(resource.patchOptions[0].DryRun) != 1 || resource.patchOptions[0].DryRun[0] != metav1.DryRunAll {
		t.Fatalf("unexpected patch dry-run options: %#v", resource.patchOptions)
	}
	if len(resource.createOptions) != 1 || len(resource.createOptions[0].DryRun) != 1 || resource.createOptions[0].DryRun[0] != metav1.DryRunAll {
		t.Fatalf("unexpected create dry-run options: %#v", resource.createOptions)
	}
	if _, err := patch(t.Context(), item, false); err != nil {
		t.Fatal(err)
	}
	if len(resource.patchOptions) != 2 || len(resource.patchOptions[1].DryRun) != 0 {
		t.Fatalf("unexpected apply patch options: %#v", resource.patchOptions)
	}
}

func testMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	return mapper
}

func configMap(name string, labels map[string]string) *unstructured.Unstructured {
	metadata := map[string]any{"name": name, "namespace": "default"}
	if labels != nil {
		labelValues := make(map[string]any, len(labels))
		for key, value := range labels {
			labelValues[key] = value
		}
		metadata["labels"] = labelValues
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   metadata,
		"data":       map[string]any{"managed": "true"},
	}}
}

func ownedConfigMap(name string, identity InstallationIdentity) *unstructured.Unstructured {
	object := configMap(name, identity.labels())
	object.SetUID(types.UID("resource-uid-" + name))
	object.SetResourceVersion("1")
	controller := true
	object.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "operator.kubeovn.io/v1alpha1", Kind: "KubeOVNInstallation",
		Name: identity.Name, UID: identity.UID, Controller: &controller,
	}})
	return object
}

func assertIdentityLabels(t *testing.T, object *unstructured.Unstructured, identity InstallationIdentity) {
	t.Helper()
	labels := object.GetLabels()
	for key, expected := range identity.labels() {
		if labels[key] != expected {
			t.Fatalf("label %q = %q, want %q", key, labels[key], expected)
		}
	}
}

func installApplyReactor(client *dynamicfake.FakeDynamicClient, patches *int, forcedError error) {
	client.PrependReactor("patch", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		(*patches)++
		if forcedError != nil {
			return true, nil, forcedError
		}
		patchAction := action.(k8stesting.PatchAction)
		object := &unstructured.Unstructured{Object: map[string]any{}}
		if err := json.Unmarshal(patchAction.GetPatch(), &object.Object); err != nil {
			return true, nil, err
		}
		object.SetName(patchAction.GetName())
		object.SetNamespace(patchAction.GetNamespace())
		return true, object, nil
	})
}

type recordingResource struct {
	dynamic.ResourceInterface
	patchOptions  []metav1.PatchOptions
	createOptions []metav1.CreateOptions
}

func (r *recordingResource) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, options metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.patchOptions = append(r.patchOptions, options)
	return new(unstructured.Unstructured), nil
}

func (r *recordingResource) Create(_ context.Context, _ *unstructured.Unstructured, options metav1.CreateOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.createOptions = append(r.createOptions, options)
	return new(unstructured.Unstructured), nil
}

func TestRetainedResourceHasNoGarbageCollectionOwner(t *testing.T) {
	identity := InstallationIdentity{Namespace: "default", Name: "installation", UID: "installation-uid"}
	for _, kind := range []string{"ConfigMap", "PersistentVolumeClaim"} {
		t.Run(kind, func(t *testing.T) {
			object := configMap("retained", nil)
			object.SetKind(kind)
			if kind == "ConfigMap" {
				object.SetAnnotations(map[string]string{"helm.sh/resource-policy": "keep"})
			}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
			mapper.Add(corev1.SchemeGroupVersion.WithKind(kind), meta.RESTScopeNamespace)
			dynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
			adapter := &Client{Dynamic: dynamic, Mapper: mapper, Identity: identity}
			prepared, err := adapter.prepare(t.Context(), identity, []*unstructured.Unstructured{object})
			if err != nil {
				t.Fatal(err)
			}
			if len(prepared[0].object.GetOwnerReferences()) != 0 {
				t.Fatal("retained resource has a garbage collection owner")
			}
			assertIdentityLabels(t, prepared[0].object, identity)
			if err := checkOwnership(prepared[0].object, identity, true, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplyStopsTheBatchWhenOperationGuardIsLost(t *testing.T) {
	kube := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), ownedConfigMap("first", testIdentity), ownedConfigMap("second", testIdentity))
	patches := 0
	installApplyReactor(kube, &patches, nil)
	stopped := errors.New("lease lost")
	writes := 0
	adapter := &Client{Dynamic: kube, Mapper: testMapper(), Identity: testIdentity, BeforeWrite: func(context.Context) error {
		writes++
		if writes == 2 {
			return stopped
		}
		return nil
	}}
	err := adapter.Apply(t.Context(), []*unstructured.Unstructured{configMap("first", nil), configMap("second", nil)})
	if !errors.Is(err, stopped) {
		t.Fatalf("guard loss not propagated: %v", err)
	}
	if patches != 3 {
		t.Fatalf("expected two dry-runs and one persisted patch, got %d", patches)
	}
}
