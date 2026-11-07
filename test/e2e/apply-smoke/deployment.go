package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"reflect"

	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// verifyDeploymentApply exercises defaulted atomic fieldRefs and normalized CPU
// quantities against the real API server, then proves external ownership blocks
// the entire batch before any other object is written. No Pods are scheduled.
func verifyDeploymentApply(ctx context.Context, adapter *apply.Client) error {
	const name = "kube-ovn-operator-apply-deployment"
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": name, "namespace": adapter.Identity.Namespace},
		"spec": map[string]any{
			"replicas": int64(0),
			"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": name}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name": "central", "image": "unused:test",
					"env":       []any{map[string]any{"name": "POD_IP", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "status.podIP"}}}},
					"resources": map[string]any{"limits": map[string]any{"cpu": "2000m"}},
				}}},
			},
		},
	}}
	// Create, reconcile unchanged fields, and change a previously owned field.
	for range 3 {
		if err := adapter.Apply(ctx, []*unstructured.Unstructured{object}); err != nil {
			return fmt.Errorf("create/reapply deployment: %w", err)
		}
	}
	resource := adapter.Dynamic.Resource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).Namespace(adapter.Identity.Namespace)
	if err := unstructured.SetNestedField(object.Object, int64(10), "spec", "minReadySeconds"); err != nil {
		return err
	}
	if err := adapter.Apply(ctx, []*unstructured.Unstructured{object}); err != nil {
		return fmt.Errorf("change operator-created field: %w", err)
	}
	updated, err := resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if value, _, _ := unstructured.NestedInt64(updated.Object, "spec", "minReadySeconds"); value != 10 {
		return fmt.Errorf("deployment change was not persisted: %d", value)
	}
	// Another manager takes a single workload field while installation ownership
	// stays intact. The operator must not force that field back.
	external := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": name, "namespace": adapter.Identity.Namespace},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "central", "resources": map[string]any{"limits": map[string]any{"cpu": "2500m"}},
		}}}}},
	}}
	payload, err := json.Marshal(external.Object)
	if err != nil {
		return err
	}
	before, err := resource.Patch(ctx, name, types.ApplyPatchType, payload, metav1.PatchOptions{FieldManager: "apply-smoke-external", Force: new(true)})
	if err != nil {
		return fmt.Errorf("set external field owner: %w", err)
	}
	marker := workload(false, adapter.Identity.Namespace, "apply-blocked-batch")
	if err := adapter.Apply(ctx, []*unstructured.Unstructured{marker, object}); !apierrors.IsConflict(err) {
		return fmt.Errorf("expected external field conflict, got %v", err)
	}
	if _, err := adapter.Dynamic.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace(adapter.Identity.Namespace).Get(ctx, marker.GetName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return fmt.Errorf("conflicting batch persisted its marker: %v", err)
	}
	after, err := resource.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before.Object["spec"], after.Object["spec"]) {
		return fmt.Errorf("conflicting apply changed externally owned workload fields")
	}
	fmt.Println("Deployment Create/SSA, self-conflict migration, external field rejection and batch preflight verified")
	return nil
}
