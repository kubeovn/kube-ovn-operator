package apply

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/fake"
)

func TestObserveRequiresCurrentNonemptyRollout(t *testing.T) {
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		t.Run(kind, func(t *testing.T) {
			identity := InstallationIdentity{Namespace: "network", Name: "test", UID: types.UID("installation")}
			object := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "apps/v1", "kind": kind, "metadata": map[string]any{"name": "ovn-central", "namespace": "network"},
				"spec": map[string]any{"replicas": int64(1)},
			}}
			object.SetLabels(identity.labels())
			object.SetGeneration(2)
			live := object.DeepCopy()
			// Retained objects avoid GC owners; normal workloads need the installation owner.
			live.SetOwnerReferences(ownedConfigMap("owner", identity).GetOwnerReferences())
			gvk := schema.GroupVersion{Group: "apps", Version: "v1"}.WithKind(kind)
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{gvk.GroupVersion()})
			mapper.Add(gvk, meta.RESTScopeNamespace)
			dynamic := fake.NewSimpleDynamicClient(runtime.NewScheme(), live)
			adapter := &Client{Dynamic: dynamic, Mapper: mapper, Identity: identity}
			observed, err := adapter.Observe(t.Context(), []*unstructured.Unstructured{object})
			if err != nil || observed.Ready {
				t.Fatalf("empty rollout marked ready: %+v %v", observed, err)
			}
			live.Object["status"] = map[string]any{
				"observedGeneration": int64(1), "replicas": int64(1), "readyReplicas": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(1),
				"desiredNumberScheduled": int64(1), "updatedNumberScheduled": int64(1), "numberReady": int64(1), "numberAvailable": int64(1),
				"currentRevision": "revision-a", "updateRevision": "revision-a",
			}
			mapping, _ := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
			if err := dynamic.Tracker().Update(mapping.Resource, live, identity.Namespace); err != nil {
				t.Fatal(err)
			}
			observed, err = adapter.Observe(t.Context(), []*unstructured.Unstructured{object})
			if err != nil || observed.Ready {
				t.Fatalf("stale rollout marked ready: %+v %v", observed, err)
			}
			live.Object["status"].(map[string]any)["observedGeneration"] = int64(2)
			if err := dynamic.Tracker().Update(mapping.Resource, live, identity.Namespace); err != nil {
				t.Fatal(err)
			}
			observed, err = adapter.Observe(t.Context(), []*unstructured.Unstructured{object})
			if err != nil || !observed.Ready {
				t.Fatalf("complete rollout not ready: %+v %v", observed, err)
			}
		})
	}
}
