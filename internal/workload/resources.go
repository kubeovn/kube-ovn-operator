package workload

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (p *Provisioner) owner() []metav1.OwnerReference {
	return []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: p.installation.Name, UID: p.installation.UID, Controller: new(true)}}
}

func (p *Provisioner) pod(request Request, ref Reference, configName string, running bool) corev1.PodTemplateSpec {
	labels := p.labels(&ref)
	name := "prepare"
	command := "exec /member-prepare --root=" + dataPath + " --request=/member-request/request.json > /dev/termination-log"
	restart := corev1.RestartPolicyOnFailure
	volumes := []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: request.Claim.Name}}},
		{Name: "request", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: configName}, DefaultMode: new(int32(0444)), Items: []corev1.KeyToPath{{Key: "request.json", Path: "request.json"}}}}},
	}
	mounts := []corev1.VolumeMount{{Name: "data", MountPath: dataPath}, {Name: "request", MountPath: "/member-request", ReadOnly: true}}
	if running {
		name, restart = "ovn-central", corev1.RestartPolicyAlways
		labels["app"] = "ovn-central"
		// emptyDir is writable; create only our private runtime subdirectory.
		// Existing database directories are never created or permission-repaired.
		command = "umask 077; mkdir -p /var/run/ovn; exec /member-run --root=" + dataPath + " --run-dir=/var/run/ovn --request=/member-request/request.json"
		volumes = append(volumes, corev1.Volume{Name: "runtime", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "runtime", MountPath: "/var/run"})
	}
	return corev1.PodTemplateSpec{
		Labels: labels,
		Spec: corev1.PodSpec{
			NodeName: request.Node.Name, HostNetwork: true, DNSPolicy: corev1.DNSClusterFirstWithHostNet,
			AutomountServiceAccountToken: new(false), EnableServiceLinks: new(false), RestartPolicy: restart,
			TerminationGracePeriodSeconds: new(int64(40)), Tolerations: request.Tolerations,
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: new(int64(65532)), RunAsGroup: new(int64(65532)), RunAsNonRoot: new(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{Name: name, Image: request.Image, ImagePullPolicy: corev1.PullIfNotPresent,
				Command: []string{"/bin/sh", "-ec", command}, VolumeMounts: mounts, Resources: request.Resources,
				TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			}}, Volumes: volumes,
		},
	}
}

func (p *Provisioner) job(request Request, ref Reference) *batchv1.Job {
	return &batchv1.Job{Name: ref.Name + "-prepare", Namespace: p.installation.Namespace, Labels: p.labels(&ref), OwnerReferences: p.owner(),
		Spec: batchv1.JobSpec{Completions: new(int32(1)), Parallelism: new(int32(1)), BackoffLimit: new(int32(6)),
			Template: p.pod(request, ref, ref.Name+"-plan", false)}}
}

func (p *Provisioner) service(ref Reference) *corev1.Service {
	return &corev1.Service{Name: ref.Name, Namespace: p.installation.Namespace, Labels: p.labels(&ref), OwnerReferences: p.owner(),
		Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: map[string]string{memberLabel: string(ref.UID), "app": "ovn-central"}}}
}

func (p *Provisioner) statefulSet(request Request, ref Reference, config *corev1.ConfigMap) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{Name: ref.Name, Namespace: p.installation.Namespace, Labels: p.labels(&ref), OwnerReferences: p.owner(),
		Spec: appsv1.StatefulSetSpec{Replicas: new(int32(1)), ServiceName: ref.Name,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{memberLabel: string(ref.UID), "app": "ovn-central"}},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template:       p.pod(request, ref, config.Name, true)}}
}

// ensure pins the UID and admitted spec of a create-only object. An uncertain
// create response can be recovered by exact ownership and request comparison;
// once a binding exists, an absent or replaced object is never recreated.
func (p *Provisioner) ensure(ctx context.Context, request Request, ref Reference, object client.Object, name string) error {
	var saved binding
	err := p.read(ctx, name, "binding.json", &saved, &ref)
	if err == nil {
		return p.existing(ctx, object, name, ref)
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	expected := object.DeepCopyObject().(client.Object)
	err = p.kube.Get(ctx, client.ObjectKeyFromObject(object), object)
	if apierrors.IsNotFound(err) {
		// Admission failures are reported before persisting the workload. The
		// immutable reservation remains to prevent a conflicting retry.
		dry := expected.DeepCopyObject().(client.Object)
		if err := p.kube.Create(ctx, dry, client.DryRunAll); err != nil {
			return err
		}
		if !p.matches(expected, dry, ref) {
			return errors.New("admission changed the protected member workload contract")
		}
		if err := p.beforeCreate(ctx, request, ref); err != nil {
			return err
		}
		if err := p.kube.Create(ctx, object); err != nil {
			return err
		}
		uid := object.GetUID()
		if err := p.kube.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			return err
		}
		if uid != object.GetUID() {
			return errors.New("member workload replaced during creation")
		}
	} else if err != nil {
		return err
	}
	if object.GetUID() == "" || !p.matches(expected, object, ref) {
		return errors.New("existing workload conflicts with the reserved member")
	}
	digest, err := specDigest(object)
	if err != nil {
		return err
	}
	if err := p.beforeCreate(ctx, request, ref); err != nil {
		return err
	}
	if _, err := p.record(ctx, name, "binding.json", binding{Object: Reference{Name: object.GetName(), UID: object.GetUID()}, Digest: digest}, &ref); err != nil {
		return err
	}
	return p.existing(ctx, object, name, ref)
}

func (p *Provisioner) beforeCreate(ctx context.Context, request Request, ref Reference) error {
	current, err := p.load(ctx, ref)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, request) {
		return errors.New("member reservation changed before workload write")
	}
	if err := p.guard(ctx, request, string(ref.UID)); err != nil {
		return err
	}
	return p.available(ctx, request, ref)
}

func (p *Provisioner) existing(ctx context.Context, object client.Object, name string, ref Reference) error {
	var saved binding
	if err := p.read(ctx, name, "binding.json", &saved, &ref); err != nil {
		return err
	}
	if saved.Object.Name != object.GetName() || saved.Object.UID == "" {
		return errors.New("invalid member workload binding")
	}
	if err := p.kube.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		return err
	}
	digest, err := specDigest(object)
	if err != nil {
		return err
	}
	if object.GetUID() != saved.Object.UID || object.GetDeletionTimestamp() != nil || !p.owned(object, &ref) ||
		!reflect.DeepEqual(object.GetOwnerReferences(), p.owner()) || digest != saved.Digest {
		return errors.New("member workload identity or spec changed")
	}
	return nil
}

func specDigest(object client.Object) (string, error) {
	var spec any
	switch object := object.(type) {
	case *batchv1.Job:
		spec = object.Spec
	case *appsv1.StatefulSet:
		spec = object.Spec
	case *corev1.Service:
		spec = object.Spec
	default:
		return "", errors.New("unsupported member workload type")
	}
	data, err := json.Marshal(spec, json.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data)), err
}

func (p *Provisioner) matches(expected, actual client.Object, ref Reference) bool {
	if actual.GetDeletionTimestamp() != nil || !p.owned(actual, &ref) || !reflect.DeepEqual(actual.GetOwnerReferences(), p.owner()) {
		return false
	}
	switch expected := expected.(type) {
	case *batchv1.Job:
		got := actual.(*batchv1.Job)
		return apiequality.Semantic.DeepDerivative(expected.Spec, got.Spec) && matchesPod(expected.Spec.Template.Spec, got.Spec.Template.Spec) &&
			(got.Spec.Suspend == nil || !*got.Spec.Suspend) && got.Spec.TTLSecondsAfterFinished == nil && got.Spec.ActiveDeadlineSeconds == nil
	case *appsv1.StatefulSet:
		got := actual.(*appsv1.StatefulSet)
		return apiequality.Semantic.DeepDerivative(expected.Spec, got.Spec) && matchesPod(expected.Spec.Template.Spec, got.Spec.Template.Spec) && len(got.Spec.VolumeClaimTemplates) == 0
	case *corev1.Service:
		got := actual.(*corev1.Service)
		return apiequality.Semantic.DeepDerivative(expected.Spec, got.Spec) && len(got.Spec.Ports) == 0 && len(got.Spec.ExternalIPs) == 0 && (got.Spec.Type == "" || got.Spec.Type == corev1.ServiceTypeClusterIP)
	}
	return false
}

// DeepDerivative tolerates API defaults, but is not enough for zero-valued
// security fields or absent sidecars. Check those explicitly before accepting
// a template or the completed native result producer.
func matchesPod(expected, actual corev1.PodSpec) bool {
	if !apiequality.Semantic.DeepDerivative(expected, actual) || actual.HostPID || actual.HostIPC ||
		len(actual.Containers) != 1 || len(actual.InitContainers) != 0 || len(actual.EphemeralContainers) != 0 || len(actual.Volumes) != len(expected.Volumes) ||
		actual.ShareProcessNamespace != nil && *actual.ShareProcessNamespace {
		return false
	}
	a, e := actual.Containers[0], expected.Containers[0]
	return len(a.Env) == 0 && len(a.EnvFrom) == 0 && len(a.VolumeDevices) == 0 && len(a.VolumeMounts) == len(e.VolumeMounts) &&
		reflect.DeepEqual(a.Command, e.Command) && len(a.Args) == 0 && a.WorkingDir == "" && a.Lifecycle == nil &&
		a.StartupProbe == nil && a.LivenessProbe == nil && a.ReadinessProbe == nil && a.SecurityContext != nil &&
		(a.SecurityContext.Privileged == nil || !*a.SecurityContext.Privileged) &&
		(a.SecurityContext.RunAsUser == nil || *a.SecurityContext.RunAsUser == 65532) &&
		(a.SecurityContext.RunAsGroup == nil || *a.SecurityContext.RunAsGroup == 65532) &&
		(a.SecurityContext.RunAsNonRoot == nil || *a.SecurityContext.RunAsNonRoot) &&
		a.SecurityContext.SeccompProfile == nil && a.SecurityContext.ProcMount == nil &&
		len(a.SecurityContext.Capabilities.Add) == 0 && actual.SecurityContext != nil && actual.SecurityContext.FSGroup == nil &&
		len(actual.SecurityContext.SupplementalGroups) == 0 && len(actual.SecurityContext.Sysctls) == 0
}
