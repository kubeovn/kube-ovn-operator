package workload

import (
	"github.com/kubeovn/kube-ovn-operator/internal/fenceagent"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func (p *Provisioner) injectMemberFenceAgent(set *appsv1.StatefulSet) error {
	if p.fenceAgent == nil {
		return nil
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(set)
	if err != nil {
		return err
	}
	rendered := &unstructured.Unstructured{Object: object}
	if err := fenceagent.Inject(rendered, *p.fenceAgent, p.installation.Name, p.installation.Namespace); err != nil {
		return err
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rendered.Object, set); err != nil {
		return err
	}
	spec := &set.Spec.Template.Spec
	spec.ServiceAccountName = fenceagent.ResourceName(p.installation.Name)
	// Prepare only the ephemeral socket directory before using it as a subPath.
	// A read-only /var/run mount would hide the mountpoints needed for TLS and
	// the projected API credentials, preventing the sidecar from starting.
	member := spec.Containers[0]
	spec.InitContainers = []corev1.Container{{
		Name: "prepare-fence-socket", Image: member.Image, ImagePullPolicy: member.ImagePullPolicy,
		Command:         []string{"/bin/sh", "-ec", "umask 077; mkdir -p /var/run/ovn"},
		VolumeMounts:    []corev1.VolumeMount{{Name: "runtime", MountPath: "/var/run"}},
		SecurityContext: member.SecurityContext.DeepCopy(), Resources: *member.Resources.DeepCopy(),
	}}
	agent := &spec.Containers[len(spec.Containers)-1]
	agent.VolumeMounts[0] = corev1.VolumeMount{Name: "runtime", MountPath: "/var/run/ovn", SubPath: "ovn", ReadOnly: true}
	// Keep automatic mounting disabled: only the sidecar can read this token.
	const access = "fence-api-access"
	spec.Volumes = append(spec.Volumes, corev1.Volume{Name: access, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
		DefaultMode: new(int32(0444)),
		Sources: []corev1.VolumeProjection{
			{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: new(int64(3600))}},
			{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
			{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
		},
	}}})
	agent.VolumeMounts = append(agent.VolumeMounts, corev1.VolumeMount{Name: access, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
	return nil
}
