package health

import (
	"context"
	"fmt"
	"slices"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Pod labels alone are not ownership evidence. Follow controller UID links to
// the exact Deployment, StatefulSet or DaemonSet managed by this installation.
func ownedPods(ctx context.Context, reader client.Reader, installation *operatorv1alpha1.KubeOVNInstallation, component string) ([]corev1.Pod, error) {
	list := new(corev1.PodList)
	if err := reader.List(ctx, list, client.InNamespace(installation.Namespace), client.MatchingLabels{"app": component}); err != nil {
		return nil, fmt.Errorf("list component pods: %w", err)
	}
	var pods []corev1.Pod
	for _, pod := range list.Items {
		workload, err := ownedPodWorkload(ctx, reader, installation, pod)
		if err != nil {
			return nil, err
		}
		if workload != nil {
			pods = append(pods, pod)
		}
	}
	slices.SortFunc(pods, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return pods, nil
}

func ownedPodWorkload(ctx context.Context, reader client.Reader, installation *operatorv1alpha1.KubeOVNInstallation, pod corev1.Pod) (client.Object, error) {
	owner := metav1.GetControllerOf(&pod)
	if owner == nil || owner.APIVersion != "apps/v1" {
		return nil, nil
	}
	if owner.Kind == "ReplicaSet" {
		rs := new(appsv1.ReplicaSet)
		if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}, rs); err != nil {
			return nil, client.IgnoreNotFound(err)
		}
		if rs.UID != owner.UID || rs.DeletionTimestamp != nil {
			return nil, nil
		}
		owner = metav1.GetControllerOf(rs)
	}
	if owner == nil || owner.APIVersion != "apps/v1" {
		return nil, nil
	}
	var workload client.Object
	switch owner.Kind {
	case "Deployment":
		workload = new(appsv1.Deployment)
	case "StatefulSet":
		workload = new(appsv1.StatefulSet)
	case "DaemonSet":
		workload = new(appsv1.DaemonSet)
	default:
		return nil, nil
	}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}, workload); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if workload.GetUID() != owner.UID || workload.GetDeletionTimestamp() != nil || !ownedWorkload(workload, installation) {
		return nil, nil
	}
	return workload, nil
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func ownedWorkload(workload client.Object, installation *operatorv1alpha1.KubeOVNInstallation) bool {
	labels := workload.GetLabels()
	owner := metav1.GetControllerOf(workload)
	return owner != nil && owner.UID == installation.UID &&
		owner.APIVersion == operatorv1alpha1.GroupVersion.String() && owner.Kind == "KubeOVNInstallation" &&
		owner.Name == installation.Name && labels[apply.ManagedByLabel] == apply.ManagedByValue &&
		labels[apply.InstallationUIDLabel] == string(installation.UID) && labels[apply.InstallationLabel] == installation.Name &&
		labels[apply.InstallationNamespaceLabel] == installation.Namespace
}

func samePods(before, after []corev1.Pod) bool {
	return slices.EqualFunc(before, after, func(a, b corev1.Pod) bool {
		if a.UID != b.UID || a.Spec.NodeName != b.Spec.NodeName || b.DeletionTimestamp != nil || b.Status.Phase != corev1.PodRunning || !podReady(b) {
			return false
		}
		if len(a.Status.ContainerStatuses) != len(b.Status.ContainerStatuses) {
			return false
		}
		for _, previous := range a.Status.ContainerStatuses {
			index := slices.IndexFunc(b.Status.ContainerStatuses, func(current corev1.ContainerStatus) bool { return current.Name == previous.Name })
			if index < 0 {
				return false
			}
			current := b.Status.ContainerStatuses[index]
			if current.ContainerID != previous.ContainerID || current.RestartCount != previous.RestartCount || !current.Ready {
				return false
			}
		}
		return true
	})
}
