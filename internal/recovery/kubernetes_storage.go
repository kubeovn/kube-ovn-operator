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
	Reader client.Reader
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

	pod, err := podByUID(ctx, v.Reader, member.Storage.PodUID)
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
	return nil
}

func (v *KubernetesStorageOwnershipVerifier) targetPVC(ctx context.Context, pod *corev1.Pod, volumeUID string) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := pvcByUID(ctx, v.Reader, volumeUID)
	if err != nil {
		return nil, fmt.Errorf("read target PVC ownership: %w", err)
	}
	if pvc.Namespace != pod.Namespace {
		return nil, errors.New("target PVC is in a different namespace from the target Pod")
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
			return pvc, nil
		}
	}
	return nil, errors.New("target PVC is not mounted by the target Pod")
}

func podByUID(ctx context.Context, reader client.Reader, uid string) (*corev1.Pod, error) {
	list := new(corev1.PodList)
	object, err := objectByUID(ctx, reader, uid, list)
	if err != nil {
		return nil, err
	}
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("ownership lookup returned %T instead of Pod", object)
	}
	return pod, nil
}

func pvcByUID(ctx context.Context, reader client.Reader, uid string) (*corev1.PersistentVolumeClaim, error) {
	list := new(corev1.PersistentVolumeClaimList)
	object, err := objectByUID(ctx, reader, uid, list)
	if err != nil {
		return nil, err
	}
	pvc, ok := object.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil, fmt.Errorf("ownership lookup returned %T instead of PersistentVolumeClaim", object)
	}
	return pvc, nil
}

func objectByUID(ctx context.Context, reader client.Reader, uid string, list client.ObjectList) (client.Object, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New("object UID is required")
	}
	if err := reader.List(ctx, list, client.MatchingFields{"metadata.uid": uid}); err != nil {
		return nil, err
	}
	objects, err := listItems(list)
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, fmt.Errorf("expected one object with UID %q, found %d", uid, len(objects))
	}
	return objects[0], nil
}

func listItems(list client.ObjectList) ([]client.Object, error) {
	switch list := list.(type) {
	case *corev1.PodList:
		items := make([]client.Object, len(list.Items))
		for index := range list.Items {
			items[index] = &list.Items[index]
		}
		return items, nil
	case *corev1.PersistentVolumeClaimList:
		items := make([]client.Object, len(list.Items))
		for index := range list.Items {
			items[index] = &list.Items[index]
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported ownership list type %T", list)
	}
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
		if status.Name == name && strings.TrimSpace(status.ContainerID) != "" {
			return status.ContainerID, true
		}
	}
	return "", false
}

var _ StorageOwnershipVerifier = (*KubernetesStorageOwnershipVerifier)(nil)
