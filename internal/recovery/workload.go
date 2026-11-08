package recovery

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	RebuildWorkloadLabel          = "operator.kubeovn.io/recovery-workload"
	RebuildPlanDigestAnnotation   = "operator.kubeovn.io/recovery-plan-digest"
	RebuildOperationAnnotation    = "operator.kubeovn.io/recovery-operation-id"
	RebuildInstallationAnnotation = "operator.kubeovn.io/recovery-installation-uid"
	rebuildTargetRoot             = "/var/lib/ovn-rebuild-target"
	rebuildRetainedRoot           = "/var/lib/ovn-rebuild-retained"
	rebuildBackupRoot             = "/var/lib/ovn-backups"
	rebuildPlanRoot               = "/var/lib/ovn-rebuild-plan"
	rebuildProofRoot              = "/var/lib/ovn-rebuild-proof"
	rebuildProcRoot               = "/host/proc"
)

// WorkloadVolume binds a named PVC to the rebuild Job. Retained volumes are
// mounted read-only; the target volume is the only volume the native helper may
// modify.
type WorkloadVolume struct {
	Name     string
	Claim    string
	ReadOnly bool
}

// WorkloadSpec is the Kubernetes execution contract for one offline rebuild.
// It deliberately contains references and digests only. Backup bytes,
// credentials and approval material never enter the Job manifest.
type WorkloadSpec struct {
	Namespace               string
	Name                    string
	InstallationUID         string
	OperationID             string
	PlanDigest              string
	MemberName              string
	NodeName                string
	Image                   string
	ServiceAccountName      string
	PlanConfigMapName       string
	IsolationProofConfigMap string
	BackupClaim             string
	Target                  WorkloadVolume
	Retained                []WorkloadVolume
	Helper                  string
}

// Validate checks the privileged workload boundary before a Job is created.
// In particular, it rejects a writable retained volume, mutable image, or
// ambiguous PVC mapping.
func (s WorkloadSpec) Validate() error {
	for field, value := range map[string]string{
		"namespace": s.Namespace, "name": s.Name, "installation UID": s.InstallationUID,
		"operation ID": s.OperationID, "member name": s.MemberName, "node name": s.NodeName,
		"image": s.Image, "service account": s.ServiceAccountName, "plan ConfigMap": s.PlanConfigMapName,
		"isolation proof ConfigMap": s.IsolationProofConfigMap, "backup PVC": s.BackupClaim,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("offline rebuild workload requires %s", field)
		}
	}
	for field, value := range map[string]string{
		"namespace": s.Namespace, "name": s.Name, "service account": s.ServiceAccountName,
		"plan ConfigMap": s.PlanConfigMapName, "isolation proof ConfigMap": s.IsolationProofConfigMap,
		"backup PVC": s.BackupClaim, "target PVC": s.Target.Claim,
	} {
		if len(validation.IsDNS1123Subdomain(value)) != 0 {
			return fmt.Errorf("offline rebuild workload %s is not a DNS name", field)
		}
	}
	if !validSHA256(s.PlanDigest) {
		return errors.New("offline rebuild workload plan digest must be a SHA-256 value")
	}
	if !membership.ValidID(s.OperationID) {
		return errors.New("offline rebuild workload operation ID must be a UUID")
	}
	if len(s.Name) > 63 || len(validation.IsDNS1123Label(s.MemberName)) != 0 || len(validation.IsDNS1123Subdomain(s.NodeName)) != 0 {
		return errors.New("offline rebuild workload name, member or node is invalid")
	}
	if !strings.Contains(s.Image, "@sha256:") || !validSHA256(strings.TrimPrefix(s.Image[strings.LastIndex(s.Image, "@"):], "@")) {
		return errors.New("offline rebuild workload image must be digest pinned")
	}
	if s.Target.Name == "" || s.Target.Name != "target" || s.Target.ReadOnly {
		return errors.New("offline rebuild target volume must be named target and writable")
	}
	if s.BackupClaim == s.Target.Claim {
		return errors.New("offline rebuild backup and target PVCs must be distinct")
	}
	if s.PlanConfigMapName == s.IsolationProofConfigMap {
		return errors.New("offline rebuild plan and isolation proof ConfigMaps must be distinct")
	}
	if len(s.Retained) == 0 {
		return errors.New("offline rebuild workload requires retained storage volumes")
	}
	if len(validation.IsDNS1123Label(s.Target.Name)) != 0 {
		return errors.New("offline rebuild target volume name is invalid")
	}
	seen := map[string]struct{}{s.BackupClaim: {}, s.Target.Claim: {}}
	for _, volume := range s.Retained {
		if volume.Name == "" || volume.Claim == "" || volume.Name == "target" || !volume.ReadOnly {
			return errors.New("offline rebuild retained volumes must be named, read-only and distinct from target")
		}
		if len(validation.IsDNS1123Label(volume.Name)) != 0 || len(validation.IsDNS1123Subdomain(volume.Claim)) != 0 {
			return fmt.Errorf("offline rebuild retained volume %q is invalid", volume.Name)
		}
		if _, exists := seen[volume.Claim]; exists {
			return fmt.Errorf("offline rebuild PVC %q is mounted more than once", volume.Claim)
		}
		seen[volume.Claim] = struct{}{}
	}
	if s.Helper == "" {
		s.Helper = "/recovery-native"
	}
	if !filepath.IsAbs(s.Helper) || filepath.Base(s.Helper) != "recovery-native" {
		return errors.New("offline rebuild helper must be the versioned recovery-native executable")
	}
	return nil
}

// Job renders the dedicated privileged workload. The returned Job has no
// owner reference unless one is supplied by the caller; deletion remains an
// explicit lifecycle action and is never inferred from a recovery result.
func (s WorkloadSpec) Job(owner *metav1.OwnerReference) (*batchv1.Job, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if owner != nil {
		owner = owner.DeepCopy()
	}
	if s.Helper == "" {
		s.Helper = "/recovery-native"
	}
	labels := map[string]string{RebuildWorkloadLabel: "true", "operator.kubeovn.io/recovery-member": s.MemberName}
	annotations := map[string]string{RebuildPlanDigestAnnotation: s.PlanDigest, RebuildOperationAnnotation: s.OperationID, RebuildInstallationAnnotation: s.InstallationUID}
	procType := corev1.HostPathDirectory
	volumes := []corev1.Volume{
		{Name: "target", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: s.Target.Claim}}},
		{Name: "backups", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: s.BackupClaim, ReadOnly: true}}},
		{Name: "plan", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: s.PlanConfigMapName}, Items: []corev1.KeyToPath{{Key: "plan.json", Path: "plan.json"}}, DefaultMode: new(int32(0444))}}},
		{Name: "proof", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: s.IsolationProofConfigMap}, Items: []corev1.KeyToPath{{Key: "proof.json", Path: "proof.json"}}, DefaultMode: new(int32(0444))}}},
		{Name: "proc", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/proc", Type: &procType}}},
		{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	mounts := []corev1.VolumeMount{
		{Name: "target", MountPath: rebuildTargetRoot},
		{Name: "backups", MountPath: rebuildBackupRoot, ReadOnly: true},
		{Name: "plan", MountPath: rebuildPlanRoot, ReadOnly: true},
		{Name: "proof", MountPath: rebuildProofRoot, ReadOnly: true},
		{Name: "proc", MountPath: rebuildProcRoot, ReadOnly: true},
		{Name: "tmp", MountPath: "/tmp"},
	}
	for _, retained := range s.Retained {
		volumes = append(volumes, corev1.Volume{Name: retained.Name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: retained.Claim, ReadOnly: true}}})
		mounts = append(mounts, corev1.VolumeMount{Name: retained.Name, MountPath: rebuildRetainedRoot + "/" + retained.Name, ReadOnly: true})
	}
	command := []string{s.Helper}
	args := []string{
		"--workload-plan=" + rebuildPlanRoot + "/plan.json",
		"--isolation-proof=" + rebuildProofRoot + "/proof.json",
		"--target-root=" + rebuildTargetRoot,
		"--proc-root=" + rebuildProcRoot,
		"--backup-dir=" + rebuildBackupRoot,
		"--member=" + s.MemberName,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: labels, Annotations: annotations}, Spec: batchv1.JobSpec{
		Completions: new(int32(1)), Parallelism: new(int32(1)), BackoffLimit: new(int32(0)), ActiveDeadlineSeconds: new(int64(30 * 60)),
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations}, Spec: corev1.PodSpec{
			NodeName: s.NodeName, HostNetwork: true, HostPID: true, DNSPolicy: corev1.DNSClusterFirstWithHostNet,
			ServiceAccountName: s.ServiceAccountName, AutomountServiceAccountToken: new(false), EnableServiceLinks: new(false), RestartPolicy: corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: new(int64(30)), ReadinessGates: nil, Volumes: volumes,
			SecurityContext: &corev1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0)), RunAsNonRoot: new(false), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{Name: "rebuild", Image: s.Image, ImagePullPolicy: corev1.PullIfNotPresent, Command: command, Args: args, WorkingDir: "/", VolumeMounts: mounts,
				TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				SecurityContext: &corev1.SecurityContext{Privileged: new(true), AllowPrivilegeEscalation: new(true), ReadOnlyRootFilesystem: new(true)},
			}},
		}},
	}}
	if owner != nil {
		job.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	return job, nil
}
