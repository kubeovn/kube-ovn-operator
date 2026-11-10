package recovery

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesStorageOwnershipVerifier validates the live Kubernetes identities
// behind a target rebuild root. The Job that invokes the verifier still owns
// the mount; this adapter binds the mount contract to the current Pod, node,
// controller workload and PVC objects before native files are written.
//
// The verifier deliberately uses UID lookups instead of names from the plan.
// Names can be reused after deletion, while the plan binds the identities that
// were admitted for this recovery operation.
type KubernetesStorageOwnershipVerifier struct {
	Reader    client.Reader
	Namespace string
}

func (v *KubernetesStorageOwnershipVerifier) Verify(ctx context.Context, member Member, root string) error {
	if v == nil || v.Reader == nil {
		return errors.New("kubernetes storage verifier requires a live client")
	}
	if err := validateStorage(member.Storage); err != nil {
		return fmt.Errorf("validate target storage identity: %w", err)
	}
	if !filepath.IsAbs(root) {
		return errors.New("target storage root must be absolute")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("stat target storage root: %w", err)
	}
	if !info.IsDir() {
		return errors.New("target storage root is not a directory")
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("target storage root must not be a symlink")
	}

	pod, err := podByUID(ctx, v.Reader, v.Namespace, member.Storage.PodUID)
	if err != nil {
		return fmt.Errorf("read target Pod ownership: %w", err)
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != member.NodeName {
		return errors.New("target Pod is deleted or scheduled on a different node")
	}
	if err := verifyControllerOwner(pod, member.Storage.WorkloadUID); err != nil {
		return err
	}
	containerID, ok := containerIDFor(pod, "ovn-central")
	if !ok || containerID != member.Storage.ContainerID {
		return errors.New("target Pod container identity does not match the recovery plan")
	}

	node := new(corev1.Node)
	if err := v.Reader.Get(ctx, client.ObjectKey{Name: member.NodeName}, node); err != nil {
		return fmt.Errorf("read target node: %w", err)
	}
	if string(node.UID) != member.Storage.NodeUID || node.DeletionTimestamp != nil {
		return errors.New("target node identity does not match the recovery plan")
	}

	pvc, err := v.targetPVC(ctx, pod, member.Storage.VolumeUID)
	if err != nil {
		return err
	}
	if pvc.Status.Phase != corev1.ClaimBound || strings.TrimSpace(pvc.Spec.VolumeName) == "" {
		return errors.New("target PVC is not currently bound")
	}
	volume := new(corev1.PersistentVolume)
	if err := v.Reader.Get(ctx, client.ObjectKey{Name: pvc.Spec.VolumeName}, volume); err != nil {
		return fmt.Errorf("read target PV ownership: %w", err)
	}
	if volume.Status.Phase != corev1.VolumeBound || volume.Spec.ClaimRef == nil || volume.Spec.ClaimRef.Name != pvc.Name || volume.Spec.ClaimRef.Namespace != pvc.Namespace || volume.Spec.ClaimRef.UID != pvc.UID {
		return errors.New("target PV claim binding does not match the recovery PVC")
	}
	return nil
}

func (v *KubernetesStorageOwnershipVerifier) targetPVC(ctx context.Context, pod *corev1.Pod, volumeUID string) (*corev1.PersistentVolumeClaim, error) {
	if strings.TrimSpace(volumeUID) == "" {
		return nil, errors.New("target PVC UID is required")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvc := new(corev1.PersistentVolumeClaim)
		key := client.ObjectKey{Namespace: pod.Namespace, Name: volume.PersistentVolumeClaim.ClaimName}
		if err := v.Reader.Get(ctx, key, pvc); err != nil {
			return nil, fmt.Errorf("read target Pod PVC %s: %w", key, err)
		}
		if string(pvc.UID) == volumeUID {
			if pvc.DeletionTimestamp != nil {
				return nil, errors.New("target PVC is being deleted")
			}
			return pvc, nil
		}
	}
	return nil, errors.New("target Pod has no PVC matching the recovery plan UID")
}

func podByUID(ctx context.Context, reader client.Reader, namespace, uid string) (*corev1.Pod, error) {
	if strings.TrimSpace(namespace) == "" || strings.TrimSpace(uid) == "" {
		return nil, errors.New("pod ownership lookup requires namespace and UID")
	}
	// Kubernetes does not support metadata.uid as a field selector. Restrict
	// the list to the installation namespace, then compare immutable UIDs.
	list := new(corev1.PodList)
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var match *corev1.Pod
	for index := range list.Items {
		pod := &list.Items[index]
		if string(pod.UID) != uid {
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("multiple Pods with UID %q", uid)
		}
		match = pod
	}
	if match == nil {
		return nil, fmt.Errorf("no Pod with UID %q in namespace %q", uid, namespace)
	}
	return match, nil
}

func verifyControllerOwner(pod *corev1.Pod, workloadUID string) error {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && string(owner.UID) == workloadUID {
			return nil
		}
	}
	return errors.New("target Pod controller workload identity does not match the recovery plan")
}

func containerIDFor(pod *corev1.Pod, name string) (string, bool) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == name && status.State.Running != nil && strings.TrimSpace(status.ContainerID) != "" {
			return status.ContainerID, true
		}
	}
	return "", false
}

var _ StorageOwnershipVerifier = (*KubernetesStorageOwnershipVerifier)(nil)
