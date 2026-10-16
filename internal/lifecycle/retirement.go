package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strconv"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	statefulSetRetirementKind = "StatefulSet"
	deploymentRetirementKind  = "Deployment"
	podDeletionCostAnnotation = "controller.kubernetes.io/pod-deletion-cost"
	minimumPodDeletionCost    = -2147483648
)

// Retirement binds one independently controlled member workload. A StatefulSet
// retirement scales an exact one-member workload to zero. A Deployment
// retirement pins one Pod and its ReplicaSet, then scales the Deployment down
// by one only after the joint database removal has completed.
type Retirement struct {
	Kind       string               `json:"kind,omitempty"`
	Workload   workload.Reference   `json:"workload"`
	Endpoint   workload.Reference   `json:"endpoint,omitempty"`
	ReplicaSet workload.Reference   `json:"replicaSet,omitempty"`
	Survivors  []workload.Reference `json:"survivors,omitempty"`
	Replicas   int32                `json:"replicas,omitzero"`
	SpecDigest string               `json:"specDigest"`
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
	return Retirement{Kind: statefulSetRetirementKind, Workload: ref, Endpoint: workload.Reference{Name: ref.Name + "-0"}, SpecDigest: digest}, err
}

// PinDeploymentRetirement binds one exact healthy Pod from an ordinary Chart
// Deployment. The target's ReplicaSet and all surviving Pod UIDs are retained
// so a later scale-down can prove that Kubernetes removed the requested member.
func PinDeploymentRetirement(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, ref, endpoint workload.Reference) (Retirement, error) {
	if kube == nil || installation == nil || installation.UID == "" || ref.Name == "" || ref.UID == "" || endpoint.Name == "" || endpoint.UID == "" {
		return Retirement{}, errors.New("deployment retirement requires installation, workload and Pod identities")
	}
	deployment := new(appsv1.Deployment)
	if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: ref.Name}, deployment); err != nil {
		return Retirement{}, err
	}
	if err := checkDeploymentRetirement(deployment, installation, ref); err != nil {
		return Retirement{}, err
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 2 {
		return Retirement{}, errors.New("deployment retirement requires at least two replicas")
	}
	pods, replicaSets, err := deploymentPods(ctx, kube, deployment)
	if err != nil {
		return Retirement{}, err
	}
	var target *corev1.Pod
	var targetRS *appsv1.ReplicaSet
	survivors := make([]workload.Reference, 0, len(pods)-1)
	for index := range pods {
		pod := &pods[index]
		if pod.Name == endpoint.Name {
			if pod.UID != endpoint.UID {
				return Retirement{}, errors.New("retirement endpoint UID changed")
			}
			target = pod
			owner := metav1.GetControllerOf(pod)
			if owner == nil {
				return Retirement{}, errors.New("retirement endpoint has no ReplicaSet controller")
			}
			targetRS = replicaSets[owner.UID]
			continue
		}
		if pod.UID == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
			return Retirement{}, errors.New("deployment retirement requires every surviving Pod to be healthy")
		}
		survivors = append(survivors, workload.Reference{Name: pod.Name, UID: pod.UID})
	}
	if target == nil || targetRS == nil || target.DeletionTimestamp != nil || target.Status.Phase != corev1.PodRunning || !podReady(target) {
		return Retirement{}, errors.New("deployment retirement endpoint is not a healthy current Pod")
	}
	for _, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.UID != targetRS.UID {
			return Retirement{}, errors.New("deployment retirement requires one stable ReplicaSet")
		}
	}
	if len(pods) != int(*deployment.Spec.Replicas) || len(survivors) != int(*deployment.Spec.Replicas)-1 {
		return Retirement{}, errors.New("deployment retirement requires one stable ReplicaSet with the expected Pod count")
	}
	slices.SortFunc(survivors, func(a, b workload.Reference) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	digest, err := deploymentRetirementDigest(deployment)
	return Retirement{Kind: deploymentRetirementKind, Workload: ref, Endpoint: endpoint,
		ReplicaSet: workload.Reference{Name: targetRS.Name, UID: targetRS.UID}, Survivors: survivors,
		Replicas: *deployment.Spec.Replicas, SpecDigest: digest}, err
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

func checkDeploymentRetirement(deployment *appsv1.Deployment, installation *api.KubeOVNInstallation, ref workload.Reference) error {
	owner := metav1.GetControllerOf(deployment)
	if deployment.Name != ref.Name || deployment.UID != ref.UID || deployment.DeletionTimestamp != nil || owner == nil || owner.UID != installation.UID || owner.Name != installation.Name || owner.APIVersion != api.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" ||
		deployment.Labels[apply.ManagedByLabel] != apply.ManagedByValue || deployment.Labels[apply.InstallationUIDLabel] != string(installation.UID) || deployment.Labels[apply.InstallationLabel] != installation.Name || deployment.Labels[apply.InstallationNamespaceLabel] != installation.Namespace {
		return errors.New("deployment retirement workload identity or installation ownership changed")
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 || deployment.Spec.Selector == nil || len(deployment.Spec.Template.Spec.Containers) != 1 || deployment.Spec.Template.Spec.Containers[0].Name != "ovn-central" || !deployment.Spec.Template.Spec.HostNetwork {
		return errors.New("deployment retirement requires a multi-replica host-network ovn-central workload")
	}
	return nil
}

func retirementDigest(set *appsv1.StatefulSet) (string, error) {
	spec := set.Spec.DeepCopy()
	spec.Replicas = new(int32(1))
	data, err := json.Marshal(spec, json.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), err
}

func deploymentRetirementDigest(deployment *appsv1.Deployment) (string, error) {
	spec := deployment.Spec.DeepCopy()
	// The operation is allowed to change only the replica count by one.
	// Normalizing it makes the same immutable template digest valid before and
	// after the authorized scale-down.
	spec.Replicas = new(int32(0))
	data, err := json.Marshal(spec, json.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), err
}

func deploymentPods(ctx context.Context, reader client.Reader, deployment *appsv1.Deployment) ([]corev1.Pod, map[types.UID]*appsv1.ReplicaSet, error) {
	sets := new(appsv1.ReplicaSetList)
	if err := reader.List(ctx, sets, client.InNamespace(deployment.Namespace)); err != nil {
		return nil, nil, err
	}
	replicaSets := make(map[types.UID]*appsv1.ReplicaSet)
	for index := range sets.Items {
		set := &sets.Items[index]
		owner := metav1.GetControllerOf(set)
		if owner != nil && owner.APIVersion == "apps/v1" && owner.Kind == "Deployment" && owner.Name == deployment.Name && owner.UID == deployment.UID {
			replicaSets[set.UID] = set
		}
	}
	pods := new(corev1.PodList)
	if err := reader.List(ctx, pods, client.InNamespace(deployment.Namespace)); err != nil {
		return nil, nil, err
	}
	result := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || replicaSets[owner.UID] == nil {
			continue
		}
		result = append(result, pod)
	}
	return result, replicaSets, nil
}

type retirementState struct {
	kind             string
	statefulSet      *appsv1.StatefulSet
	deployment       *appsv1.Deployment
	endpointPresent  bool
	survivorsPresent int
}

func (e *Executor) retirementTarget(ctx context.Context, target Retirement) (*retirementState, error) {
	if target.Kind == "" {
		target.Kind = statefulSetRetirementKind
	}
	switch target.Kind {
	case statefulSetRetirementKind:
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
		endpoint := target.Endpoint.Name
		if endpoint == "" {
			endpoint = target.Workload.Name + "-0"
		}
		pod := new(corev1.Pod)
		endpointPresent := false
		if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: endpoint}, pod); err == nil {
			owner := metav1.GetControllerOf(pod)
			if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "StatefulSet" || owner.Name != set.Name || owner.UID != set.UID {
				return nil, errors.New("retirement endpoint belongs to a different workload")
			}
			endpointPresent = true
		} else if !apierrors.IsNotFound(err) {
			return nil, err
		}
		return &retirementState{kind: target.Kind, statefulSet: set, endpointPresent: endpointPresent}, nil
	case deploymentRetirementKind:
		deployment := new(appsv1.Deployment)
		if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: target.Workload.Name}, deployment); err != nil {
			return nil, err
		}
		if err := checkDeploymentRetirement(deployment, e.installation, target.Workload); err != nil {
			return nil, err
		}
		digest, err := deploymentRetirementDigest(deployment)
		if err != nil || digest != target.SpecDigest {
			return nil, errors.New("retirement Deployment spec changed")
		}
		if target.Replicas < 2 || deployment.Spec.Replicas == nil || (*deployment.Spec.Replicas != target.Replicas && *deployment.Spec.Replicas != target.Replicas-1) {
			return nil, errors.New("retirement Deployment replica count changed")
		}
		pods, replicaSets, err := deploymentPods(ctx, e.kube, deployment)
		if err != nil {
			return nil, err
		}
		state := &retirementState{kind: target.Kind, deployment: deployment}
		for _, pod := range pods {
			owner := metav1.GetControllerOf(&pod)
			if owner == nil || owner.UID != target.ReplicaSet.UID || replicaSets[owner.UID] == nil {
				return nil, errors.New("retirement Deployment ReplicaSet changed")
			}
			if pod.Name == target.Endpoint.Name {
				if pod.UID != target.Endpoint.UID {
					return nil, errors.New("retirement endpoint was replaced")
				}
				state.endpointPresent = true
				continue
			}
			if pod.DeletionTimestamp != nil {
				return nil, errors.New("retirement Deployment survivor Pod is already terminating")
			}
			if slices.ContainsFunc(target.Survivors, func(ref workload.Reference) bool { return ref.Name == pod.Name && ref.UID == pod.UID }) {
				state.survivorsPresent++
				continue
			}
			return nil, errors.New("retirement Deployment contains an unexpected Pod")
		}
		if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == target.Replicas && len(pods) != int(target.Replicas) {
			return nil, errors.New("retirement Deployment has an unexpected Pod count")
		}
		if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == target.Replicas-1 {
			if len(pods) < int(target.Replicas-1) || len(pods) > int(target.Replicas) {
				return nil, errors.New("retirement Deployment has an unexpected Pod count during scale-down")
			}
			if !state.endpointPresent && len(pods) != int(target.Replicas-1) {
				return nil, errors.New("retirement Deployment has a replacement Pod after target removal")
			}
		}
		return state, nil
	default:
		return nil, errors.New("unsupported retirement workload kind")
	}
}

func (e *Executor) retirementReadyForConvergence(ctx context.Context, target Retirement) error {
	state, err := e.retirementTarget(ctx, target)
	if err != nil {
		return err
	}
	if state.kind == deploymentRetirementKind {
		if state.deployment.Spec.Replicas == nil || *state.deployment.Spec.Replicas != target.Replicas {
			return errors.New("retirement Deployment was scaled before joint membership convergence")
		}
		return nil
	}
	if state.statefulSet.Spec.Replicas == nil || *state.statefulSet.Spec.Replicas != 1 {
		return errors.New("retirement workload was scaled before joint membership convergence")
	}
	return nil
}

func (e *Executor) retire(ctx context.Context, target Retirement) (bool, error) {
	state, err := e.retirementTarget(ctx, target)
	if err != nil {
		return false, err
	}
	if state.kind == deploymentRetirementKind {
		if state.deployment.Spec.Replicas == nil {
			return false, errors.New("retirement Deployment has no replica count")
		}
		if *state.deployment.Spec.Replicas == target.Replicas {
			if state.survivorsPresent != len(target.Survivors) || !state.endpointPresent {
				return false, errors.New("retirement Deployment changed before scale-down")
			}
			if err := e.prioritizeDeploymentTarget(ctx, target); err != nil {
				return false, err
			}
			if err := e.guard(ctx); err != nil {
				return false, err
			}
			state.deployment.Spec.Replicas = new(target.Replicas - 1)
			if err := e.kube.Update(ctx, state.deployment); err != nil {
				return false, err
			}
			return false, nil
		}
		if *state.deployment.Spec.Replicas != target.Replicas-1 {
			return false, errors.New("retirement Deployment replica count changed during scale-down")
		}
		if state.endpointPresent {
			if state.survivorsPresent != len(target.Survivors) {
				return false, errors.New("retirement Deployment lost a non-target Pod during scale-down")
			}
			return false, nil
		}
		if state.survivorsPresent != len(target.Survivors) {
			return false, errors.New("retirement Deployment lost a non-target Pod during scale-down")
		}
		return true, nil
	}
	if *state.statefulSet.Spec.Replicas != 0 {
		if err := e.guard(ctx); err != nil {
			return false, err
		}
		state.statefulSet.Spec.Replicas = new(int32(0))
		if err := e.kube.Update(ctx, state.statefulSet); err != nil {
			return false, err
		}
		return false, nil
	}
	return !state.endpointPresent, nil
}

func (e *Executor) prioritizeDeploymentTarget(ctx context.Context, target Retirement) error {
	pod := new(corev1.Pod)
	if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: target.Endpoint.Name}, pod); err != nil {
		return err
	}
	if pod.UID != target.Endpoint.UID {
		return errors.New("retirement endpoint changed before Deployment scale-down")
	}
	if err := e.guard(ctx); err != nil {
		return err
	}
	for _, survivor := range target.Survivors {
		other := new(corev1.Pod)
		if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: survivor.Name}, other); err != nil {
			return err
		}
		if other.UID != survivor.UID {
			return errors.New("retirement survivor Pod changed before Deployment scale-down")
		}
		cost, err := podDeletionCost(other)
		if err != nil {
			return err
		}
		if cost <= minimumPodDeletionCost {
			return errors.New("another Deployment Pod already has an equal or lower deletion cost")
		}
	}
	if pod.DeletionTimestamp != nil {
		return errors.New("retirement endpoint is already terminating")
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[podDeletionCostAnnotation] = strconv.Itoa(minimumPodDeletionCost)
	if err := e.guard(ctx); err != nil {
		return err
	}
	return e.kube.Update(ctx, pod)
}

func podDeletionCost(pod *corev1.Pod) (int, error) {
	value := pod.Annotations[podDeletionCostAnnotation]
	if value == "" {
		return 0, nil
	}
	cost, err := strconv.Atoi(value)
	if err != nil || cost < minimumPodDeletionCost || cost > 2147483647 {
		return 0, errors.New("invalid Deployment Pod deletion cost")
	}
	return cost, nil
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(condition corev1.PodCondition) bool {
		return condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue
	})
}
