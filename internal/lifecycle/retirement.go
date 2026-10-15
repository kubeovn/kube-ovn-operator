package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Retirement binds one independently controlled member workload. Deployments
// cannot identify which replica will be removed and are deliberately ineligible.
// Storage and the zero-replica StatefulSet are retained after retirement.
type Retirement struct {
	Workload   workload.Reference `json:"workload"`
	SpecDigest string             `json:"specDigest"`
}

// PinRetirement reads a one-member StatefulSet without modifying it. Its
// identity and complete spec become immutable operation input.
func PinRetirement(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, ref workload.Reference) (Retirement, error) {
	if kube == nil || installation == nil || installation.UID == "" || ref.Name == "" || ref.UID == "" {
		return Retirement{}, errors.New("retirement requires installation and workload identities")
	}
	set := new(appsv1.StatefulSet)
	if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: ref.Name}, set); err != nil {
		return Retirement{}, err
	}
	if err := checkRetirement(set, installation, ref); err != nil {
		return Retirement{}, err
	}
	if set.Spec.Replicas == nil || *set.Spec.Replicas != 1 {
		return Retirement{}, errors.New("new retirement target must have exactly one replica")
	}
	digest, err := retirementDigest(set)
	return Retirement{Workload: ref, SpecDigest: digest}, err
}

func checkRetirement(set *appsv1.StatefulSet, installation *api.KubeOVNInstallation, ref workload.Reference) error {
	owner := metav1.GetControllerOf(set)
	if set.Name != ref.Name || set.UID != ref.UID || set.DeletionTimestamp != nil || owner == nil || owner.UID != installation.UID || owner.Name != installation.Name || owner.APIVersion != api.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" ||
		set.Labels[apply.ManagedByLabel] != apply.ManagedByValue || set.Labels[apply.InstallationUIDLabel] != string(installation.UID) || set.Labels[apply.InstallationLabel] != installation.Name || set.Labels[apply.InstallationNamespaceLabel] != installation.Namespace {
		return errors.New("retirement workload identity or installation ownership changed")
	}
	if set.Spec.Replicas == nil || (*set.Spec.Replicas != 0 && *set.Spec.Replicas != 1) || len(set.Spec.VolumeClaimTemplates) != 0 || set.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType || len(set.Spec.Template.Spec.Containers) != 1 || set.Spec.Template.Spec.Containers[0].Name != "ovn-central" || set.Spec.Ordinals != nil && set.Spec.Ordinals.Start != 0 {
		return errors.New("retirement requires an independent single-member StatefulSet with retained external storage")
	}
	return nil
}

func retirementDigest(set *appsv1.StatefulSet) (string, error) {
	spec := set.Spec.DeepCopy()
	spec.Replicas = new(int32(1))
	data, err := json.Marshal(spec, json.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), err
}

func (e *Executor) retirementTarget(ctx context.Context, target Retirement) (*appsv1.StatefulSet, error) {
	set := new(appsv1.StatefulSet)
	if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: target.Workload.Name}, set); err != nil {
		return nil, err
	}
	if err := checkRetirement(set, e.installation, target.Workload); err != nil {
		return nil, err
	}
	digest, err := retirementDigest(set)
	if err != nil || digest != target.SpecDigest {
		return nil, errors.New("retirement workload spec changed")
	}
	pod := new(corev1.Pod)
	if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: target.Workload.Name + "-0"}, pod); err == nil {
		owner := metav1.GetControllerOf(pod)
		if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "StatefulSet" || owner.Name != set.Name || owner.UID != set.UID {
			return nil, errors.New("retirement endpoint belongs to a different workload")
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}
	return set, nil
}

func (e *Executor) retire(ctx context.Context, target Retirement) (bool, error) {
	set, err := e.retirementTarget(ctx, target)
	if err != nil {
		return false, err
	}
	if *set.Spec.Replicas != 0 {
		if err := e.guard(ctx); err != nil {
			return false, err
		}
		set.Spec.Replicas = new(int32(0))
		if err := e.kube.Update(ctx, set); err != nil {
			return false, err
		}
		return false, nil
	}
	pods := new(corev1.PodList)
	if err := e.kube.List(ctx, pods, client.InNamespace(e.installation.Namespace)); err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner != nil && owner.Kind == "StatefulSet" && owner.APIVersion == "apps/v1" && owner.UID == set.UID {
			return false, nil
		}
	}
	current, err := e.retirementTarget(ctx, target)
	return err == nil && *current.Spec.Replicas == 0, err
}
