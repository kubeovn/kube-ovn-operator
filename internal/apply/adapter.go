// Package apply provides an ownership-checked Kubernetes workload boundary.
package apply

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
)

const (
	ManagedByLabel             = "app.kubernetes.io/managed-by"
	ManagedByValue             = "kube-ovn-operator"
	InstallationLabel          = "operator.kubeovn.io/installation"
	InstallationNamespaceLabel = "operator.kubeovn.io/installation-namespace"
	InstallationUIDLabel       = "operator.kubeovn.io/installation-uid"
	DefaultFieldManager        = "kube-ovn-operator"
	AdoptionFieldManager       = "kube-ovn-operator-adoption"
)

// ErrOwnershipConflict means a resource cannot be adopted by this adapter.
var ErrOwnershipConflict = errors.New("kubernetes resource ownership conflict")

type InstallationIdentity struct {
	Namespace string
	Name      string
	UID       types.UID
}

// ApplyClient validates and applies one rendered workload batch. Apply must
// dry-run the entire batch before making any persistent change.
type ApplyClient interface {
	DryRun(context.Context, []*unstructured.Unstructured) error
	Apply(context.Context, []*unstructured.Unstructured) error
}

// Adopter claims ownership metadata on an existing, explicitly selected
// resource set. It never creates or changes workload fields.
type Adopter interface {
	Adopt(context.Context, []*unstructured.Unstructured, map[string]string) error
}

// Client uses uncached reads and server-side apply without force ownership.
// New objects use create-only semantics after dry-run to prevent an SSA upsert
// from overwriting a resource created concurrently by another actor.
type Client struct {
	Dynamic  dynamic.Interface
	Mapper   meta.RESTMapper
	Identity InstallationIdentity
	// BeforeWrite, when configured, checks the operation lease before each persistent request.
	BeforeWrite func(context.Context) error
}

var _ ApplyClient = (*Client)(nil)
var _ Adopter = (*Client)(nil)

type preparedObject struct {
	object                  *unstructured.Unstructured
	resource                dynamic.ResourceInterface
	exists                  bool
	expectedResourceVersion string
}

type preparedAdoption struct {
	object   *unstructured.Unstructured
	resource dynamic.ResourceInterface
}

func (c *Client) DryRun(ctx context.Context, objects []*unstructured.Unstructured) error {
	if len(objects) == 0 {
		return nil
	}
	prepared, err := c.prepare(ctx, c.Identity, objects)
	if err != nil {
		return err
	}
	return dryRunAll(ctx, prepared)
}

// Apply uses the same immutable snapshots for preflight and persistence.
// Kubernetes does not provide multi-object transactions: a later write failure may leave earlier
// objects applied; the caller must retry the whole batch after observing state.
func (c *Client) Apply(ctx context.Context, objects []*unstructured.Unstructured) error {
	if len(objects) == 0 {
		return nil
	}
	prepared, err := c.prepare(ctx, c.Identity, objects)
	if err != nil {
		return err
	}
	if err := dryRunAll(ctx, prepared); err != nil {
		return err
	}
	for _, item := range prepared {
		if c.BeforeWrite != nil {
			if err := c.BeforeWrite(ctx); err != nil {
				return fmt.Errorf("guard workload write: %w", err)
			}
		}
		if item.exists {
			_, err = patch(ctx, item, false)
		} else {
			_, err = item.resource.Create(ctx, item.object, metav1.CreateOptions{
				FieldManager: DefaultFieldManager, FieldValidation: metav1.FieldValidationStrict,
			})
		}
		if err != nil {
			return fmt.Errorf("apply %s: %w", objectKey(item.object), err)
		}
	}
	return nil
}

// Adopt validates the complete target set before writing any ownership
// metadata. Every object must already exist and match selector. Existing
// workload fields are preserved; server-side apply only claims labels and the
// installation controller owner reference.
func (c *Client) Adopt(ctx context.Context, objects []*unstructured.Unstructured, selector map[string]string) error {
	prepared, err := c.prepareAdoption(ctx, c.Identity, objects, selector)
	if err != nil {
		return err
	}
	for _, item := range prepared {
		if _, err := adoptionPatch(ctx, item, true); err != nil {
			return fmt.Errorf("server-side adoption dry-run %s: %w", objectKey(item.object), err)
		}
	}
	for _, item := range prepared {
		if c.BeforeWrite != nil {
			if err := c.BeforeWrite(ctx); err != nil {
				return fmt.Errorf("guard adoption write: %w", err)
			}
		}
		if _, err := adoptionPatch(ctx, item, false); err != nil {
			return fmt.Errorf("adopt %s: %w", objectKey(item.object), err)
		}
	}
	return nil
}

func dryRunAll(ctx context.Context, objects []preparedObject) error {
	for _, item := range objects {
		if _, err := patch(ctx, item, true); err != nil {
			return fmt.Errorf("server-side dry-run %s: %w", objectKey(item.object), err)
		}
		// Admission can distinguish CREATE from PATCH. Validate the exact
		// persistence operation as well when the object does not exist yet.
		if !item.exists {
			if _, err := item.resource.Create(ctx, item.object, metav1.CreateOptions{
				FieldManager: DefaultFieldManager, FieldValidation: metav1.FieldValidationStrict,
				DryRun: []string{metav1.DryRunAll},
			}); err != nil {
				return fmt.Errorf("create dry-run %s: %w", objectKey(item.object), err)
			}
		}
	}
	return nil
}

func patch(ctx context.Context, item preparedObject, dryRun bool) (*unstructured.Unstructured, error) {
	applyObject := item.object.DeepCopy()
	applyObject.SetUID("")
	applyObject.SetResourceVersion(item.expectedResourceVersion)
	applyObject.SetManagedFields(nil)
	payload, err := json.Marshal(applyObject.Object)
	if err != nil {
		return nil, err
	}
	options := metav1.PatchOptions{FieldManager: DefaultFieldManager, FieldValidation: metav1.FieldValidationStrict}
	if dryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	return item.resource.Patch(ctx, item.object.GetName(), types.ApplyPatchType, payload, options)
}

func (c *Client) prepare(ctx context.Context, identity InstallationIdentity, objects []*unstructured.Unstructured) ([]preparedObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Dynamic == nil || c.Mapper == nil {
		return nil, errors.New("dynamic Kubernetes client and REST mapper are required")
	}
	if identity.Namespace == "" || identity.Name == "" || identity.UID == "" {
		return nil, errors.New("installation namespace, name and UID are required")
	}
	prepared := make([]preparedObject, 0, len(objects))
	seen := make(map[string]bool, len(objects))
	for _, object := range objects {
		if object == nil {
			return nil, errors.New("rendered workload contains a nil object")
		}
		gvk := object.GroupVersionKind()
		if gvk.Version == "" || gvk.Kind == "" || object.GetName() == "" || object.GetGenerateName() != "" {
			return nil, errors.New("rendered objects require apiVersion, kind and a fixed name")
		}
		if object.GetUID() != "" || object.GetResourceVersion() != "" || len(object.GetManagedFields()) != 0 || object.GetDeletionTimestamp() != nil {
			return nil, fmt.Errorf("%s contains server-owned metadata", objectKey(object))
		}
		if _, hasStatus := object.Object["status"]; hasStatus {
			return nil, fmt.Errorf("%s contains status", objectKey(object))
		}
		mapping, err := c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("map %s: %w", gvk, err)
		}
		var resource dynamic.ResourceInterface
		namespaced := mapping.Scope.Name() == meta.RESTScopeNameNamespace
		if namespaced {
			if object.GetNamespace() != identity.Namespace {
				return nil, fmt.Errorf("%s must be in installation namespace %q", objectKey(object), identity.Namespace)
			}
			resource = c.Dynamic.Resource(mapping.Resource).Namespace(object.GetNamespace())
		} else {
			if object.GetNamespace() != "" {
				return nil, fmt.Errorf("cluster-scoped %s cannot have a namespace", objectKey(object))
			}
			resource = c.Dynamic.Resource(mapping.Resource)
		}
		key := mapping.Resource.GroupResource().String() + "/" + object.GetNamespace() + "/" + object.GetName()
		if seen[key] {
			return nil, fmt.Errorf("duplicate rendered object %s", key)
		}
		seen[key] = true
		copy := object.DeepCopy()
		if err := checkOwnership(copy, identity, false, namespaced && !retainResource(copy)); err != nil {
			return nil, err
		}
		labels := copy.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		for label, value := range identity.labels() {
			labels[label] = value
		}
		copy.SetLabels(labels)
		if namespaced && !retainResource(copy) {
			copy.SetOwnerReferences([]metav1.OwnerReference{{
				APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation",
				Name: identity.Name, UID: identity.UID, Controller: new(true),
			}})
		}
		item := preparedObject{object: copy, resource: resource}
		existing, err := resource.Get(ctx, copy.GetName(), metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			return nil, fmt.Errorf("read %s: %w", objectKey(copy), err)
		default:
			if existing.GetName() != copy.GetName() || existing.GetNamespace() != copy.GetNamespace() {
				return nil, ownershipConflict(copy, "live resource identity differs from requested identity")
			}
			if err := checkOwnership(existing, identity, true, namespaced && !retainResource(copy)); err != nil {
				return nil, err
			}
			if existing.GetDeletionTimestamp() != nil || existing.GetUID() == "" || existing.GetResourceVersion() == "" {
				return nil, fmt.Errorf("%s is deleting or has no UID/resourceVersion", objectKey(copy))
			}
			item.exists = true
			item.expectedResourceVersion = existing.GetResourceVersion()
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func (c *Client) prepareAdoption(ctx context.Context, identity InstallationIdentity, objects []*unstructured.Unstructured, selector map[string]string) ([]preparedAdoption, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.Dynamic == nil || c.Mapper == nil {
		return nil, errors.New("dynamic Kubernetes client and REST mapper are required")
	}
	if identity.Namespace == "" || identity.Name == "" || identity.UID == "" {
		return nil, errors.New("installation namespace, name and UID are required")
	}
	if len(selector) == 0 {
		return nil, errors.New("adoption selector must contain at least one label")
	}
	for key, value := range selector {
		if errs := kvalidation.IsQualifiedName(key); len(errs) != 0 {
			return nil, fmt.Errorf("adoption selector key %q is invalid: %s", key, errs[0])
		}
		if errs := kvalidation.IsValidLabelValue(value); len(errs) != 0 {
			return nil, fmt.Errorf("adoption selector value for %q is invalid: %s", key, errs[0])
		}
	}
	prepared := make([]preparedAdoption, 0, len(objects))
	seen := make(map[string]bool, len(objects))
	for _, object := range objects {
		if err := validateRenderedObject(object); err != nil {
			return nil, err
		}
		gvk := object.GroupVersionKind()
		mapping, err := c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("map %s: %w", gvk, err)
		}
		namespaced := mapping.Scope.Name() == meta.RESTScopeNameNamespace
		var resource dynamic.ResourceInterface
		if namespaced {
			if object.GetNamespace() != identity.Namespace {
				return nil, fmt.Errorf("%s must be in installation namespace %q", objectKey(object), identity.Namespace)
			}
			resource = c.Dynamic.Resource(mapping.Resource).Namespace(object.GetNamespace())
		} else {
			if object.GetNamespace() != "" {
				return nil, fmt.Errorf("cluster-scoped %s cannot have a namespace", objectKey(object))
			}
			resource = c.Dynamic.Resource(mapping.Resource)
		}
		key := mapping.Resource.GroupResource().String() + "/" + object.GetNamespace() + "/" + object.GetName()
		if seen[key] {
			return nil, fmt.Errorf("duplicate adoption object %s", key)
		}
		seen[key] = true
		existing, err := resource.Get(ctx, object.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("adoption requires existing resource %s", objectKey(object))
		}
		if err != nil {
			return nil, fmt.Errorf("read %s for adoption: %w", objectKey(object), err)
		}
		if existing.GetDeletionTimestamp() != nil || existing.GetUID() == "" || existing.GetResourceVersion() == "" {
			return nil, fmt.Errorf("%s is deleting or has no UID/resourceVersion", objectKey(object))
		}
		if !labelsMatch(existing.GetLabels(), selector) {
			return nil, ownershipConflict(object, "existing resource does not match the adoption selector")
		}
		if err := checkAdoptionLabels(existing, identity); err != nil {
			return nil, err
		}
		ownerReferences, err := adoptionOwners(existing, identity, namespaced && !retainResource(object))
		if err != nil {
			return nil, err
		}
		labels := identity.labels()
		patchObject := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]any{
				"name":            existing.GetName(),
				"resourceVersion": existing.GetResourceVersion(),
			},
		}}
		metadata := patchObject.Object["metadata"].(map[string]any)
		if namespaced {
			metadata["namespace"] = existing.GetNamespace()
		}
		if err := unstructured.SetNestedStringMap(patchObject.Object, labels, "metadata", "labels"); err != nil {
			return nil, fmt.Errorf("prepare adoption labels for %s: %w", objectKey(object), err)
		}
		if len(ownerReferences) != 0 {
			owners, err := ownerReferencesValue(ownerReferences)
			if err != nil {
				return nil, fmt.Errorf("prepare adoption owners for %s: %w", objectKey(object), err)
			}
			if err := unstructured.SetNestedSlice(patchObject.Object, owners, "metadata", "ownerReferences"); err != nil {
				return nil, fmt.Errorf("prepare adoption owners for %s: %w", objectKey(object), err)
			}
		}
		prepared = append(prepared, preparedAdoption{object: patchObject, resource: resource})
	}
	return prepared, nil
}

func validateRenderedObject(object *unstructured.Unstructured) error {
	if object == nil {
		return errors.New("rendered workload contains a nil object")
	}
	gvk := object.GroupVersionKind()
	if gvk.Version == "" || gvk.Kind == "" || object.GetName() == "" || object.GetGenerateName() != "" {
		return errors.New("rendered objects require apiVersion, kind and a fixed name")
	}
	if object.GetUID() != "" || object.GetResourceVersion() != "" || len(object.GetManagedFields()) != 0 || object.GetDeletionTimestamp() != nil {
		return fmt.Errorf("%s contains server-owned metadata", objectKey(object))
	}
	if _, hasStatus := object.Object["status"]; hasStatus {
		return fmt.Errorf("%s contains status", objectKey(object))
	}
	return nil
}

func labelsMatch(labels, selector map[string]string) bool {
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func checkAdoptionLabels(object *unstructured.Unstructured, identity InstallationIdentity) error {
	labels := object.GetLabels()
	for _, label := range []string{InstallationLabel, InstallationNamespaceLabel, InstallationUIDLabel} {
		if actual, present := labels[label]; present && actual != identity.labels()[label] {
			return ownershipConflict(object, fmt.Sprintf("label %q belongs to another installation", label))
		}
	}
	return nil
}

func adoptionOwners(object *unstructured.Unstructured, identity InstallationIdentity, requireOwner bool) ([]metav1.OwnerReference, error) {
	owners := object.GetOwnerReferences()
	if len(owners) > 1 {
		return nil, ownershipConflict(object, "multiple owner references are not supported")
	}
	if len(owners) == 1 {
		owner := owners[0]
		if owner.APIVersion != operatorv1alpha1.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" || owner.Name != identity.Name || owner.UID != identity.UID || owner.Controller == nil || !*owner.Controller {
			return nil, ownershipConflict(object, "owner reference belongs to another controller")
		}
		return owners, nil
	}
	if !requireOwner {
		return nil, nil
	}
	return []metav1.OwnerReference{{
		APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation",
		Name: identity.Name, UID: identity.UID, Controller: new(true),
	}}, nil
}

func ownerReferencesValue(owners []metav1.OwnerReference) ([]any, error) {
	data, err := json.Marshal(owners)
	if err != nil {
		return nil, err
	}
	var values []any
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func adoptionPatch(ctx context.Context, item preparedAdoption, dryRun bool) (*unstructured.Unstructured, error) {
	payload, err := json.Marshal(item.object.Object)
	if err != nil {
		return nil, err
	}
	options := metav1.PatchOptions{FieldManager: AdoptionFieldManager, FieldValidation: metav1.FieldValidationStrict}
	if dryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	return item.resource.Patch(ctx, item.object.GetName(), types.ApplyPatchType, payload, options)
}

func (i InstallationIdentity) labels() map[string]string {
	return map[string]string{
		ManagedByLabel: ManagedByValue, InstallationLabel: i.Name,
		InstallationNamespaceLabel: i.Namespace, InstallationUIDLabel: string(i.UID),
	}
}

func checkOwnership(object *unstructured.Unstructured, identity InstallationIdentity, existing, namespaced bool) error {
	labels := object.GetLabels()
	for label, expected := range identity.labels() {
		actual, present := labels[label]
		if (existing || present) && actual != expected {
			return ownershipConflict(object, fmt.Sprintf("label %q is %q, expected %q", label, actual, expected))
		}
	}
	owners := object.GetOwnerReferences()
	if existing && namespaced && len(owners) != 1 {
		return ownershipConflict(object, "exactly one installation controller owner is required")
	}
	for _, owner := range owners {
		if !namespaced || owner.APIVersion != operatorv1alpha1.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" ||
			owner.Name != identity.Name || owner.UID != identity.UID || owner.Controller == nil || !*owner.Controller {
			return ownershipConflict(object, "owner reference does not match the installation")
		}
	}
	if len(owners) > 1 {
		return ownershipConflict(object, "multiple owner references are not supported")
	}
	return nil
}

func objectKey(object *unstructured.Unstructured) string {
	return object.GroupVersionKind().String() + " " + object.GetNamespace() + "/" + object.GetName()
}

func ownershipConflict(object *unstructured.Unstructured, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrOwnershipConflict, objectKey(object), reason)
}
