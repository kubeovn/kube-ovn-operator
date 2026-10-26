package apply

import (
	"context"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	rollbackQuiescedAnnotation = "operator.kubeovn.io/rollback-quiesced"
	// rollbackStateAnnotation records the exact controller state that Quiesce
	// changed. It is kept on the live object rather than inferred from the
	// target Chart because a rollback may remove a workload from that Chart.
	rollbackStateAnnotation = "operator.kubeovn.io/rollback-state"
)

// RollbackClient is the workload capability required by a destructive
// database restore. Quiesce freezes rollout controllers before the databases
// are changed; Unquiesce is called only after the target Chart has been
// applied successfully.
type RollbackClient interface {
	ApplyClient
	Observer
	Quiesce(context.Context, string) error
	Unquiesce(context.Context, string, []*unstructured.Unstructured) error
}

// Quiesce pauses owned application rollouts while preserving running central
// Pods so the native OVSDB restore endpoint remains reachable. The target
// Chart apply later restores the original update strategy and clears the
// marker through Unquiesce.
func (c *Client) Quiesce(ctx context.Context, token string) error {
	if c == nil || c.Dynamic == nil || c.Mapper == nil || token == "" {
		return errors.New("rollback quiesce requires a client and fence token")
	}
	seen := 0
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		resource, namespaced, err := c.appsResource(kind)
		if err != nil {
			return err
		}
		list, err := resource.List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", InstallationUIDLabel, c.Identity.UID)})
		if err != nil {
			return fmt.Errorf("list owned %s for rollback quiesce: %w", kind, err)
		}
		for index := range list.Items {
			object := &list.Items[index]
			if namespaced {
				if err := checkOwnership(object, c.Identity, true, true); err != nil {
					return fmt.Errorf("validate rollback workload %s/%s: %w", kind, object.GetName(), err)
				}
			}
			seen++
			annotations := object.GetAnnotations()
			if current := annotations[rollbackQuiescedAnnotation]; current != "" {
				if current != token {
					return fmt.Errorf("rollback workload %s/%s is already quiesced by another operation", kind, object.GetName())
				}
				if annotations[rollbackStateAnnotation] == "" {
					return fmt.Errorf("rollback workload %s/%s has no original controller state", kind, object.GetName())
				}
				continue
			}
			state := map[string]any{}
			switch kind {
			case "Deployment":
				paused, found, err := unstructured.NestedBool(object.Object, "spec", "paused")
				if err != nil {
					return fmt.Errorf("read rollback state for %s/%s: %w", kind, object.GetName(), err)
				}
				if found {
					state["paused"] = paused
				}
			default:
				strategy, found, err := unstructured.NestedMap(object.Object, "spec", "updateStrategy")
				if err != nil {
					return fmt.Errorf("read rollback state for %s/%s: %w", kind, object.GetName(), err)
				}
				if found {
					state["updateStrategy"] = strategy
				}
			}
			stateJSON, err := stdjson.Marshal(state, stdjson.Deterministic(true))
			if err != nil {
				return fmt.Errorf("encode rollback state for %s/%s: %w", kind, object.GetName(), err)
			}
			patch := map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{
					rollbackQuiescedAnnotation: token,
					rollbackStateAnnotation:    string(stateJSON),
				}},
			}
			spec := map[string]any{}
			if kind == "Deployment" {
				spec["paused"] = true
			} else {
				spec["updateStrategy"] = map[string]any{"type": "OnDelete"}
			}
			patch["spec"] = spec
			payload, err := stdjson.Marshal(patch)
			if err != nil {
				return err
			}
			if c.BeforeWrite != nil {
				if err := c.BeforeWrite(ctx); err != nil {
					return fmt.Errorf("guard rollback quiesce: %w", err)
				}
			}
			if _, err := resource.Patch(ctx, object.GetName(), types.MergePatchType, payload, metav1.PatchOptions{FieldValidation: metav1.FieldValidationStrict}); err != nil {
				return fmt.Errorf("quiesce %s/%s: %w", kind, object.GetName(), err)
			}
		}
	}
	if seen == 0 {
		return errors.New("rollback quiesce found no owned workload controllers")
	}
	return nil
}

// Unquiesce scans all owned controllers carrying this operation's durable
// marker, restores the exact pre-quiesce state, then removes the marker. It
// deliberately does not depend on the target Chart: a rollback may remove a
// workload that still needs to be resumed. The token binding prevents one
// operation from restoring another operation's paused workloads and makes a
// retry after a lost response idempotent.
func (c *Client) Unquiesce(ctx context.Context, token string, _ []*unstructured.Unstructured) error {
	if c == nil || c.Dynamic == nil || c.Mapper == nil || token == "" {
		return errors.New("rollback unquiesce requires a client and operation token")
	}
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		resource, namespaced, err := c.appsResource(kind)
		if err != nil {
			return err
		}
		if !namespaced {
			continue
		}
		list, err := resource.List(ctx, metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", InstallationUIDLabel, c.Identity.UID)})
		if err != nil {
			return fmt.Errorf("list owned %s for rollback unquiesce: %w", kind, err)
		}
		for index := range list.Items {
			live := &list.Items[index]
			annotations := live.GetAnnotations()
			if annotations[rollbackQuiescedAnnotation] == "" {
				continue
			}
			if annotations[rollbackQuiescedAnnotation] != token {
				return fmt.Errorf("rollback workload %s/%s is quiesced by another operation", kind, live.GetName())
			}
			if err := checkOwnership(live, c.Identity, true, true); err != nil {
				return fmt.Errorf("validate rollback workload %s/%s: %w", kind, live.GetName(), err)
			}
			stateJSON := annotations[rollbackStateAnnotation]
			if stateJSON == "" {
				return fmt.Errorf("rollback workload %s/%s has no original controller state", kind, live.GetName())
			}
			state := make(map[string]any)
			if err := stdjson.Unmarshal([]byte(stateJSON), &state, stdjson.RejectUnknownMembers(true)); err != nil {
				return fmt.Errorf("decode rollback state for %s/%s: %w", kind, live.GetName(), err)
			}
			patch := map[string]any{"metadata": map[string]any{"annotations": map[string]any{
				rollbackQuiescedAnnotation: nil,
				rollbackStateAnnotation:    nil,
			}}}
			spec := map[string]any{}
			if kind == "Deployment" {
				if paused, found := state["paused"]; found {
					spec["paused"] = paused
				} else {
					spec["paused"] = nil
				}
			} else if strategy, found := state["updateStrategy"]; found {
				spec["updateStrategy"] = strategy
			} else {
				spec["updateStrategy"] = nil
			}
			patch["spec"] = spec
			payload, err := stdjson.Marshal(patch, stdjson.Deterministic(true))
			if err != nil {
				return err
			}
			if c.BeforeWrite != nil {
				if err := c.BeforeWrite(ctx); err != nil {
					return fmt.Errorf("guard rollback unquiesce: %w", err)
				}
			}
			if _, err := resource.Patch(ctx, live.GetName(), types.MergePatchType, payload, metav1.PatchOptions{FieldValidation: metav1.FieldValidationStrict}); err != nil {
				return fmt.Errorf("unquiesce %s/%s: %w", kind, live.GetName(), err)
			}
		}
	}
	return nil
}

func (c *Client) appsResource(kind string) (dynamicResource, bool, error) {
	resource := resourceName(kind)
	if resource == "" {
		return nil, false, fmt.Errorf("unsupported rollback workload kind %q", kind)
	}
	mapping, err := c.Mapper.RESTMapping(schema.GroupKind{Group: "apps", Kind: kind}, "v1")
	if err != nil {
		return nil, false, fmt.Errorf("map rollback workload %s: %w", kind, err)
	}
	namespaced := mapping.Scope.Name() == "namespace"
	namespaceable := c.Dynamic.Resource(mapping.Resource)
	var result dynamic.ResourceInterface = namespaceable
	if namespaced {
		result = namespaceable.Namespace(c.Identity.Namespace)
	}
	return result, namespaced, nil
}

type dynamicResource interface {
	List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Get(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error)
	Patch(context.Context, string, types.PatchType, []byte, metav1.PatchOptions, ...string) (*unstructured.Unstructured, error)
}

func resourceName(kind string) string {
	switch kind {
	case "Deployment":
		return "deployments"
	case "StatefulSet":
		return "statefulsets"
	case "DaemonSet":
		return "daemonsets"
	default:
		return ""
	}
}
