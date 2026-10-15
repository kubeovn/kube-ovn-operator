package chart

import (
	"fmt"
	"maps"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/probe"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// NetworkProbeObjects shares OVS scheduling constraints but never its host
// network, privileges, volumes or credentials. Both roles use ordinary pods,
// so even a one-node installation checks distinct pod network namespaces.
func NetworkProbeObjects(installation *operatorv1alpha1.KubeOVNInstallation, objects []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	if installation.Spec.NetworkProbe == nil {
		return nil, nil
	}
	if installation.UID == "" {
		return nil, fmt.Errorf("network probes require an installation UID")
	}
	var ovs *appsv1.DaemonSet
	for _, object := range objects {
		if object.GetAPIVersion() != "apps/v1" || object.GetKind() != "DaemonSet" {
			continue
		}
		var ds appsv1.DaemonSet
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &ds); err != nil {
			return nil, err
		}
		if ds.Spec.Selector == nil || ds.Spec.Selector.MatchLabels["app"] != "ovs" {
			continue
		}
		if ovs != nil {
			return nil, fmt.Errorf("network probes require one OVS DaemonSet")
		}
		ovs = &ds
	}
	if ovs == nil {
		return nil, fmt.Errorf("network probes require an OVS DaemonSet in the rendered chart")
	}
	name := probe.Name(string(installation.UID))
	labels := func(app string) map[string]string {
		return map[string]string{"app": app, "operator.kubeovn.io/probe-installation": string(installation.UID)}
	}
	var resources []runtime.Object
	for _, role := range []struct{ name, app string }{{"client", probe.ClientApp}, {"server", probe.ServerApp}} {
		pod := corev1.PodSpec{
			AutomountServiceAccountToken: new(false),
			NodeSelector:                 maps.Clone(ovs.Spec.Template.Spec.NodeSelector),
			Affinity:                     ovs.Spec.Template.Spec.Affinity.DeepCopy(),
			Tolerations:                  append([]corev1.Toleration(nil), ovs.Spec.Template.Spec.Tolerations...),
			DNSPolicy:                    corev1.DNSClusterFirst,
			SecurityContext:              &corev1.PodSecurityContext{RunAsNonRoot: new(true), RunAsUser: new(int64(65532)), RunAsGroup: new(int64(65532)), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{Name: probe.Container, Image: installation.Spec.NetworkProbe.Image, ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"/network-probe", "serve"},
				Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: probe.Port}},
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}},
				ReadinessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(probe.Port)}}, PeriodSeconds: 2},
			}},
		}
		for _, field := range []struct{ name, path string }{{"POD_NAME", "metadata.name"}, {"POD_UID", "metadata.uid"}, {"NODE_NAME", "spec.nodeName"}} {
			// fieldRef is atomic for SSA. Match the server's default so a
			// repeated apply agrees with the value recorded during create.
			pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: field.name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: field.path}}})
		}
		resources = append(resources, &appsv1.DaemonSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"}, Name: name + "-" + role.name, Namespace: installation.Namespace,
			Spec: appsv1.DaemonSetSpec{Selector: &metav1.LabelSelector{MatchLabels: labels(role.app)}, Template: corev1.PodTemplateSpec{Labels: labels(role.app), Spec: pod}},
		})
	}
	resources = append(resources, &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, Name: name, Namespace: installation.Namespace,
		Spec: corev1.ServiceSpec{Selector: labels(probe.ServerApp), Type: corev1.ServiceTypeClusterIP, IPFamilyPolicy: new(corev1.IPFamilyPolicyPreferDualStack), Ports: []corev1.ServicePort{{Name: "http", Port: probe.Port, TargetPort: intstr.FromInt(probe.Port)}}},
	})
	result := make([]*unstructured.Unstructured, 0, len(resources))
	for _, object := range resources {
		data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			return nil, err
		}
		// Typed Kubernetes objects serialize an empty status. Desired manifests
		// must omit it: the apply boundary rejects caller-supplied status.
		delete(data, "status")
		result = append(result, &unstructured.Unstructured{Object: data})
	}
	return result, nil
}
