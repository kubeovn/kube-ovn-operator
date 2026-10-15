package apply

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// Observation reports workload rollout, not OVN quorum or network health.
type Observation struct {
	Ready                bool
	Message              string
	ReadyCentralReplicas int32
}

type Observer interface {
	Observe(context.Context, []*unstructured.Unstructured) (Observation, error)
}

func (c *Client) Observe(ctx context.Context, objects []*unstructured.Unstructured) (Observation, error) {
	result := Observation{Ready: true}
	workloads := 0
	for _, desired := range objects {
		gvk := desired.GroupVersionKind()
		mapping, err := c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return Observation{}, err
		}
		namespaced := mapping.Scope.Name() == meta.RESTScopeNameNamespace
		var resource dynamic.ResourceInterface = c.Dynamic.Resource(mapping.Resource)
		if namespaced {
			resource = c.Dynamic.Resource(mapping.Resource).Namespace(desired.GetNamespace())
		}
		live, err := resource.Get(ctx, desired.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			result.Ready = false
			result.Message = fmt.Sprintf("Waiting for %s/%s to exist", desired.GetKind(), desired.GetName())
			continue
		}
		if err != nil {
			return Observation{}, fmt.Errorf("observe %s: %w", objectKey(desired), err)
		}
		if err := checkOwnership(live, c.Identity, true, namespaced && !retainResource(desired)); err != nil {
			return Observation{}, err
		}
		ready := live.GetDeletionTimestamp() == nil
		switch gvk.GroupKind().String() {
		case "Deployment.apps", "StatefulSet.apps":
			workloads++
			replicas, found, _ := unstructured.NestedInt64(live.Object, "spec", "replicas")
			if !found {
				replicas = 1
			}
			wanted, found, _ := unstructured.NestedInt64(desired.Object, "spec", "replicas")
			if !found {
				wanted = 1
			}
			ready = ready && replicas == wanted && replicas > 0 && count(live, "observedGeneration") >= live.GetGeneration() &&
				count(live, "replicas") == replicas && count(live, "updatedReplicas") == replicas && count(live, "readyReplicas") == replicas
			if desired.GetKind() == "Deployment" {
				ready = ready && count(live, "availableReplicas") == replicas
			}
			if desired.GetKind() == "StatefulSet" {
				current, _, _ := unstructured.NestedString(live.Object, "status", "currentRevision")
				update, _, _ := unstructured.NestedString(live.Object, "status", "updateRevision")
				ready = ready && current != "" && current == update
			}
			if desired.GetName() == "ovn-central" {
				result.ReadyCentralReplicas = int32(count(live, "readyReplicas"))
			}
		case "DaemonSet.apps":
			workloads++
			desiredCount := count(live, "desiredNumberScheduled")
			ready = ready && desiredCount > 0 && count(live, "observedGeneration") >= live.GetGeneration() &&
				count(live, "updatedNumberScheduled") == desiredCount && count(live, "numberReady") == desiredCount &&
				count(live, "numberAvailable") == desiredCount && count(live, "numberMisscheduled") == 0
		case "Job.batch":
			ready = ready && conditionTrue(live, "Complete") && !conditionTrue(live, "Failed")
		case "PersistentVolumeClaim":
			phase, _, _ := unstructured.NestedString(live.Object, "status", "phase")
			ready = ready && phase == "Bound"
		case "CustomResourceDefinition.apiextensions.k8s.io":
			ready = ready && conditionTrue(live, "Established") && conditionTrue(live, "NamesAccepted")
		}
		if !ready {
			result.Ready = false
			result.Message = fmt.Sprintf("Waiting for %s/%s rollout", desired.GetKind(), desired.GetName())
		}
	}
	if workloads == 0 && result.Ready {
		result.Ready = false
		result.Message = "No workload controllers were observed"
	}
	if result.Ready {
		result.Message = "All rendered workloads have completed rollout; OVN and network verification is still required"
	}
	return result, nil
}

func count(object *unstructured.Unstructured, field string) int64 {
	value, _, _ := unstructured.NestedInt64(object.Object, "status", field)
	return value
}

func conditionTrue(object *unstructured.Unstructured, kind string) bool {
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if ok && condition["type"] == kind && condition["status"] == "True" {
			return true
		}
	}
	return false
}

// Persistent data must survive deletion of the namespaced installation CR.
func retainResource(object *unstructured.Unstructured) bool {
	return object.GroupVersionKind().GroupKind().String() == "PersistentVolumeClaim" || object.GetAnnotations()["helm.sh/resource-policy"] == "keep"
}
