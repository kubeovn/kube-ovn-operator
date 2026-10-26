package apply

import (
	stdjson "encoding/json/v2"
	"reflect"
	"testing"

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

func TestRollbackUnquiesceRestoresRemovedWorkloadsFromLiveState(t *testing.T) {
	deployment := rollbackWorkload("Deployment", "central", map[string]any{"paused": true})
	originalStrategy := map[string]any{"type": "RollingUpdate"}
	statefulSet := rollbackWorkload("StatefulSet", "member", map[string]any{"updateStrategy": originalStrategy})
	kube := rollbackDynamicClient(deployment, statefulSet)
	installRollbackPatchReactor(kube)
	adapter := &Client{Dynamic: kube, Mapper: rollbackMapper(), Identity: testIdentity}

	if err := adapter.Quiesce(t.Context(), "sha256:rollback"); err != nil {
		t.Fatal(err)
	}

	liveSet := rollbackResource(kube, "statefulsets")
	quiesced, err := liveSet.Get(t.Context(), statefulSet.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if strategy, _, _ := unstructured.NestedMap(quiesced.Object, "spec", "updateStrategy"); strategy["type"] != "OnDelete" {
		t.Fatalf("stateful set was not quiesced: %#v", strategy)
	}
	if quiesced.GetAnnotations()[rollbackStateAnnotation] == "" {
		t.Fatal("quiesce did not persist the original state")
	}

	// Pass an empty target Chart to model a rollback that removes the member
	// workload. The live marker must still drive restoration.
	if err := adapter.Unquiesce(t.Context(), "sha256:rollback", nil); err != nil {
		t.Fatal(err)
	}
	restored, err := liveSet.Get(t.Context(), statefulSet.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	strategy, found, err := unstructured.NestedMap(restored.Object, "spec", "updateStrategy")
	if err != nil || !found || !reflect.DeepEqual(strategy, originalStrategy) {
		t.Fatalf("stateful set strategy was not restored: found=%v strategy=%#v err=%v", found, strategy, err)
	}
	if _, present := restored.GetAnnotations()[rollbackQuiescedAnnotation]; present {
		t.Fatal("quiesce marker remains after restoration")
	}
	if _, present := restored.GetAnnotations()[rollbackStateAnnotation]; present {
		t.Fatal("rollback state marker remains after restoration")
	}

	deployments := rollbackResource(kube, "deployments")
	restoredDeployment, err := deployments.Get(t.Context(), deployment.GetName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	paused, found, err := unstructured.NestedBool(restoredDeployment.Object, "spec", "paused")
	if err != nil || !found || !paused {
		t.Fatalf("deployment paused state was not restored: found=%v paused=%v err=%v", found, paused, err)
	}
}

func TestRollbackUnquiesceRejectsAnotherOperationToken(t *testing.T) {
	deployment := rollbackWorkload("Deployment", "central", map[string]any{"paused": false})
	kube := rollbackDynamicClient(deployment)
	installRollbackPatchReactor(kube)
	adapter := &Client{Dynamic: kube, Mapper: rollbackMapper(), Identity: testIdentity}

	if err := adapter.Quiesce(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Unquiesce(t.Context(), "second", nil); err == nil {
		t.Fatal("unquiesce accepted another operation token")
	}
	live, err := rollbackResource(kube, "deployments").Get(t.Context(), "central", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if live.GetAnnotations()[rollbackQuiescedAnnotation] != "first" {
		t.Fatalf("foreign unquiesce changed the marker: %v", live.GetAnnotations())
	}
}

func TestRollbackQuiesceIsIdempotentAndRejectsAnotherToken(t *testing.T) {
	deployment := rollbackWorkload("Deployment", "central", map[string]any{"paused": false})
	kube := rollbackDynamicClient(deployment)
	installRollbackPatchReactor(kube)
	adapter := &Client{Dynamic: kube, Mapper: rollbackMapper(), Identity: testIdentity}

	if err := adapter.Quiesce(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Quiesce(t.Context(), "first"); err != nil {
		t.Fatalf("repeated quiesce was not idempotent: %v", err)
	}
	if err := adapter.Quiesce(t.Context(), "second"); err == nil {
		t.Fatal("quiesce accepted a different operation token")
	}
}

func rollbackMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: "apps", Version: "v1"}})
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		mapper.Add(schema.GroupVersion{Group: "apps", Version: "v1"}.WithKind(kind), meta.RESTScopeNamespace)
	}
	return mapper
}

func rollbackWorkload(kind, name string, spec map[string]any) *unstructured.Unstructured {
	controller := true
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       kind,
		"metadata": map[string]any{
			"name": name, "namespace": testIdentity.Namespace,
			"labels": map[string]any{
				ManagedByLabel: ManagedByValue, InstallationLabel: testIdentity.Name,
				InstallationNamespaceLabel: testIdentity.Namespace, InstallationUIDLabel: string(testIdentity.UID),
			},
			"ownerReferences": []any{map[string]any{
				"apiVersion": "operator.kubeovn.io/v1alpha1", "kind": "KubeOVNInstallation",
				"name": testIdentity.Name, "uid": string(testIdentity.UID), "controller": controller,
			}},
		},
		"spec": spec,
	}}
	object.SetUID(types.UID("uid-" + name))
	object.SetResourceVersion("1")
	return object
}

func rollbackResource(kube *dynamicfake.FakeDynamicClient, resource string) dynamic.ResourceInterface {
	return kube.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: resource}).Namespace(testIdentity.Namespace)
}

func rollbackDynamicClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		{Group: "apps", Version: "v1", Resource: "deployments"}:  "DeploymentList",
		{Group: "apps", Version: "v1", Resource: "statefulsets"}: "StatefulSetList",
		{Group: "apps", Version: "v1", Resource: "daemonsets"}:   "DaemonSetList",
	}, objects...)
}

func installRollbackPatchReactor(kube *dynamicfake.FakeDynamicClient) {
	kube.PrependReactor("patch", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patchAction := action.(k8stesting.PatchAction)
		resource := patchAction.GetResource()
		object, err := kube.Tracker().Get(resource, patchAction.GetNamespace(), patchAction.GetName())
		if err != nil {
			return true, nil, err
		}
		var patch map[string]any
		if err := stdjson.Unmarshal(patchAction.GetPatch(), &patch); err != nil {
			return true, nil, err
		}
		mergeRollbackPatch(object.(*unstructured.Unstructured).Object, patch)
		if err := kube.Tracker().Update(resource, object, patchAction.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, object, nil
	})
}

func mergeRollbackPatch(target, patch map[string]any) {
	for key, value := range patch {
		if value == nil {
			delete(target, key)
			continue
		}
		child, nested := value.(map[string]any)
		if !nested {
			target[key] = value
			continue
		}
		existing, _ := target[key].(map[string]any)
		if existing == nil {
			existing = make(map[string]any)
			target[key] = existing
		}
		mergeRollbackPatch(existing, child)
	}
}
