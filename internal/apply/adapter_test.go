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

func TestDryRunForcesOnlySelfManagedConflicts(t *testing.T) {
	live := ownedConfigMap("workload", testIdentity)
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{
		Manager:    DefaultFieldManager,
		Operation:  metav1.ManagedFieldsOperationUpdate,
		FieldsType: "FieldsV1",
		APIVersion: "v1",
	}})
	resource := &conflictResource{err: apierrors.NewApplyConflict([]metav1.StatusCause{{Type: metav1.CauseTypeFieldManagerConflict, Field: ".data.managed", Message: `conflict with "kube-ovn-operator" using v1`}}, "self-owned conflict")}
	objects := []preparedObject{{object: configMap("workload", nil), resource: resource, exists: true, live: live}}
	if err := dryRunAll(t.Context(), objects); err != nil {
		t.Fatal(err)
	}
	if len(resource.patchOptions) != 2 || resource.patchOptions[0].Force != nil || resource.patchOptions[1].Force == nil || !*resource.patchOptions[1].Force {
		t.Fatalf("unexpected force retry options: %#v", resource.patchOptions)
	}
	if !objects[0].force {
		t.Fatal("self-owned conflict did not persist force decision for the write")
	}
}

func TestDryRunRejectsExternalManagedConflicts(t *testing.T) {
	live := ownedConfigMap("workload", testIdentity)
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{
		Manager:    "helm",
		Operation:  metav1.ManagedFieldsOperationUpdate,
		FieldsType: "FieldsV1",
		APIVersion: "v1",
	}})
	resource := &conflictResource{err: apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "workload", errors.New(`conflicts with "helm" using apps/v1:`))}
	objects := []preparedObject{{object: configMap("workload", nil), resource: resource, exists: true, live: live}}
	if err := dryRunAll(t.Context(), objects); err == nil {
		t.Fatal("external managed conflict unexpectedly succeeded")
	}
	if len(resource.patchOptions) != 1 || resource.patchOptions[0].Force != nil {
		t.Fatalf("unexpected external conflict options: %#v", resource.patchOptions)
	}
}

func TestForceRejectsMixedOrUnstructuredConflicts(t *testing.T) {
	live := ownedConfigMap("workload", testIdentity)
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: DefaultFieldManager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1"}})
	self := metav1.StatusCause{Type: metav1.CauseTypeFieldManagerConflict, Field: ".data.managed", Message: `conflict with "kube-ovn-operator" using v1`}
	external := metav1.StatusCause{Type: metav1.CauseTypeFieldManagerConflict, Field: ".data.external", Message: `conflict with "helm"`}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"mixed managers", apierrors.NewApplyConflict([]metav1.StatusCause{self, external}, "conflicts with operator and helm")},
		{"unstructured", apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "workload", errors.New(`conflicts with "kube-ovn-operator" using v1`))},
		{"unknown cause", apierrors.NewApplyConflict([]metav1.StatusCause{self, {Type: metav1.CauseTypeUnexpectedServerResponse}}, "unknown conflict")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if canForceSelfOwnedConflict(tc.err, live) {
				t.Fatal("unsafe conflict authorized Force")
			}
		})
	}
}

func TestVersionRetryRejectsIdentityChanges(t *testing.T) {
	for _, change := range []string{"uid", "labels", "owner", "deletion"} {
		t.Run(change, func(t *testing.T) {
			live := ownedConfigMap("workload", testIdentity)
			switch change {
			case "uid":
				live.SetUID("replacement")
			case "labels":
				live.SetLabels(map[string]string{ManagedByLabel: "helm"})
			case "owner":
				live.SetOwnerReferences(nil)
			case "deletion":
				live.SetDeletionTimestamp(new(metav1.Now()))
			}
			resource := &versionConflictResource{live: live}
			item := &preparedObject{object: ownedConfigMap("workload", testIdentity), live: ownedConfigMap("workload", testIdentity), resource: resource, exists: true, expectedResourceVersion: "1"}
			if err := new(Client).persist(t.Context(), item); !errors.Is(err, ErrOwnershipConflict) {
				t.Fatalf("identity change was accepted: %v", err)
			}
			if resource.patchCalls != 1 {
				t.Fatalf("wrote after identity change: %d patches", resource.patchCalls)
			}
		})
	}
}

func TestPersistRefreshesResourceVersionConflict(t *testing.T) {
	resource := &versionConflictResource{}
	item := &preparedObject{
		object:                  ownedConfigMap("workload", testIdentity),
		live:                    ownedConfigMap("workload", testIdentity),
		resource:                resource,
		exists:                  true,
		expectedResourceVersion: "1",
	}
	guardCalls := 0
	client := &Client{BeforeWrite: func(context.Context) error {
		guardCalls++
		return nil
	}}
	if err := client.persist(t.Context(), item); err != nil {
		t.Fatal(err)
	}
	if resource.patchCalls != 3 || resource.getCalls != 1 {
		t.Fatalf("expected one refresh, one dry-run and two patch attempts, got %d gets and %d patches", resource.getCalls, resource.patchCalls)
	}
	if item.expectedResourceVersion != "2" {
		t.Fatalf("resourceVersion = %q, want refreshed version 2", item.expectedResourceVersion)
	}
	if guardCalls != 1 {
		t.Fatalf("retry guard calls = %d, want 1", guardCalls)
	}
}

func TestDryRunRefreshesResourceVersionConflict(t *testing.T) {
	resource := &versionConflictResource{}
	objects := []preparedObject{{
		object:                  ownedConfigMap("workload", testIdentity),
		live:                    ownedConfigMap("workload", testIdentity),
		resource:                resource,
		exists:                  true,
		expectedResourceVersion: "1",
	}}
	if err := dryRunAll(t.Context(), objects); err != nil {
		t.Fatal(err)
	}
	if resource.patchCalls != 2 || resource.getCalls != 1 {
		t.Fatalf("expected one refresh and two dry-run attempts, got %d gets and %d patches", resource.getCalls, resource.patchCalls)
	}
	if objects[0].expectedResourceVersion != "2" {
		t.Fatalf("resourceVersion = %q, want refreshed version 2", objects[0].expectedResourceVersion)
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

type conflictResource struct {
	dynamic.ResourceInterface
	patchOptions []metav1.PatchOptions
	err          error
}

type versionConflictResource struct {
	dynamic.ResourceInterface
	getCalls   int
	patchCalls int
	live       *unstructured.Unstructured
}

func (r *versionConflictResource) Get(_ context.Context, name string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.getCalls++
	if r.live != nil {
		return r.live.DeepCopy(), nil
	}
	live := ownedConfigMap(name, testIdentity)
	live.SetResourceVersion("2")
	return live, nil
}

func (r *versionConflictResource) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, _ metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.patchCalls++
	if r.patchCalls == 1 {
		return nil, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "workload", errors.New("object has been modified; please apply your changes to the latest version"))
	}
	return new(unstructured.Unstructured), nil
}

func (r *conflictResource) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, options metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.patchOptions = append(r.patchOptions, options)
	if len(r.patchOptions) == 1 {
		return nil, r.err
	}
	return new(unstructured.Unstructured), nil
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

func TestAdoptDryRunsTheCompleteExistingBatchBeforeClaimingOwnership(t *testing.T) {
	first := configMap("first", map[string]string{"app": "kube-ovn"})
	first.SetUID(types.UID("first-uid"))
	first.SetResourceVersion("1")
	second := configMap("second", map[string]string{"app": "kube-ovn"})
	second.SetUID(types.UID("second-uid"))
	second.SetResourceVersion("2")
	kube := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), first, second)
	patches := 0
	var payloads [][]byte
	kube.PrependReactor("patch", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		payloads = append(payloads, action.(k8stesting.PatchAction).GetPatch())
		return true, actionResult(action), nil
	})
	adapter := &Client{Dynamic: kube, Mapper: testMapper(), Identity: testIdentity}
	objects := []*unstructured.Unstructured{configMap("first", nil), configMap("second", nil)}
	if err := adapter.Adopt(t.Context(), objects, map[string]string{"app": "kube-ovn"}); err != nil {
		t.Fatal(err)
	}
	if patches != 4 {
		t.Fatalf("expected two dry-runs followed by two writes, got %d patch calls", patches)
	}
	for _, payload := range payloads {
		var object map[string]any
		if err := json.Unmarshal(payload, &object); err != nil {
			t.Fatal(err)
		}
		metadata := object["metadata"].(map[string]any)
		labels := metadata["labels"].(map[string]any)
		if labels[ManagedByLabel] != ManagedByValue || labels[InstallationLabel] != testIdentity.Name || labels[InstallationUIDLabel] != string(testIdentity.UID) {
			t.Fatalf("adoption patch did not claim installation labels: %#v", labels)
		}
		if _, ok := metadata["ownerReferences"]; !ok {
			t.Fatalf("adoption patch did not claim controller ownership: %#v", metadata)
		}
	}
	if objects[0].GetLabels() != nil || objects[0].GetOwnerReferences() != nil {
		t.Fatalf("adopter mutated rendered input: %#v", objects[0].Object)
	}
}

func TestAdoptRejectsMissingOrUnsafeResources(t *testing.T) {
	tests := []struct {
		name     string
		object   *unstructured.Unstructured
		selector map[string]string
		want     error
	}{
		{name: "missing", object: configMap("missing", nil), selector: map[string]string{"app": "kube-ovn"}},
		{name: "selector mismatch", object: adoptedConfigMap("mismatch", map[string]string{"app": "other"}), selector: map[string]string{"app": "kube-ovn"}},
		{name: "external owner", object: adoptedConfigMapWithOwner("owner", map[string]string{"app": "kube-ovn"}), selector: map[string]string{"app": "kube-ovn"}},
		{name: "other installation", object: adoptedConfigMap("other-installation", map[string]string{"app": "kube-ovn", InstallationLabel: "other"}), selector: map[string]string{"app": "kube-ovn"}},
		{name: "empty selector", object: adoptedConfigMap("empty", map[string]string{"app": "kube-ovn"}), selector: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []runtime.Object{}
			if tt.object.GetUID() != "" {
				objects = append(objects, tt.object)
			}
			kube := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
			adapter := &Client{Dynamic: kube, Mapper: testMapper(), Identity: testIdentity}
			if err := adapter.Adopt(t.Context(), []*unstructured.Unstructured{configMap(tt.object.GetName(), nil)}, tt.selector); err == nil {
				t.Fatal("unsafe adoption unexpectedly succeeded")
			}
		})
	}
}

func actionResult(action k8stesting.Action) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      action.(k8stesting.PatchAction).GetName(),
			"namespace": action.(k8stesting.PatchAction).GetNamespace(),
		},
	}}
}

func adoptedConfigMap(name string, labels map[string]string) *unstructured.Unstructured {
	object := configMap(name, labels)
	object.SetUID(types.UID("resource-uid-" + name))
	object.SetResourceVersion("1")
	return object
}

func adoptedConfigMapWithOwner(name string, labels map[string]string) *unstructured.Unstructured {
	object := adoptedConfigMap(name, labels)
	controller := true
	object.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "other", UID: "other", Controller: &controller}})
	return object
}

// Deployment status can change between the initial self-conflict and its
// forced retry. The refreshed version needs its own self-conflict retry.
func TestDryRunRetriesSelfConflictAfterVersionRefresh(t *testing.T) {
	live := ownedConfigMap("workload", testIdentity)
	live.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: DefaultFieldManager, Operation: metav1.ManagedFieldsOperationUpdate, APIVersion: "v1"}})
	resource := &interleavedConflictResource{live: live}
	objects := []preparedObject{{object: live.DeepCopy(), live: live, exists: true, resource: resource, expectedResourceVersion: "1"}}
	if err := dryRunAll(t.Context(), objects); err != nil {
		t.Fatal(err)
	}
	if resource.calls != 4 {
		t.Fatalf("expected two self-conflict attempts and one version refresh, got %d patches", resource.calls)
	}
}

type interleavedConflictResource struct {
	dynamic.ResourceInterface
	live  *unstructured.Unstructured
	calls int
}

func (r *interleavedConflictResource) Get(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error) {
	live := r.live.DeepCopy()
	live.SetResourceVersion("2")
	return live, nil
}

func (r *interleavedConflictResource) Patch(_ context.Context, _ string, _ types.PatchType, _ []byte, options metav1.PatchOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.calls++
	if r.calls == 2 {
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "workload", errors.New("object has been modified; please apply your changes to the latest version"))
	}
	if options.Force == nil || !*options.Force {
		return nil, apierrors.NewApplyConflict([]metav1.StatusCause{{Type: metav1.CauseTypeFieldManagerConflict, Field: ".data.managed", Message: `conflict with "kube-ovn-operator" using v1`}}, "self-owned conflict")
	}
	return r.live.DeepCopy(), nil
}
