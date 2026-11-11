package recovery

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func rebuildWorkloadFixture() WorkloadSpec {
	return WorkloadSpec{
		Namespace: "ovn-system", Name: DeterministicRebuildJobName("sha256:" + strings.Repeat("a", 64)), InstallationUID: "installation-uid",
		OperationID: "11111111-1111-1111-1111-111111111111", PlanDigest: "sha256:" + strings.Repeat("a", 64), MemberName: "member-new", NodeName: "node-new",
		Image: "ghcr.io/kubeovn/kube-ovn-operator@sha256:" + strings.Repeat("b", 64), ServiceAccountName: "rebuild-agent",
		PlanConfigMapName: "rebuild-plan", PlanConfigMapUID: "plan-uid", PlanConfigMapDataDigest: ConfigMapDataDigest("{}"), IsolationProofConfigMap: "rebuild-proof", IsolationProofConfigMapUID: "proof-uid", IsolationProofDataDigest: ConfigMapDataDigest("{}"), BackupClaim: "ovn-backups", BackupClaimUID: "backup-uid", BackupPVUID: "backup-pv-uid",
		Target: WorkloadVolume{Name: "target", Claim: "ovn-target", ClaimUID: "target-uid", PVUID: "target-pv-uid"}, Retained: []WorkloadVolume{{Name: "member-old", Claim: "ovn-old", ClaimUID: "retained-uid", PVUID: "retained-pv-uid", ReadOnly: true}},
	}
}

func TestWorkloadRendersDedicatedPrivilegedJob(t *testing.T) {
	spec := rebuildWorkloadFixture()
	job, err := spec.Job(&metav1.OwnerReference{APIVersion: "operator.kubeovn.io/v1alpha1", Kind: "KubeOVNRecoveryRequest", Name: "recover", UID: "request-uid"})
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Template.Spec.NodeName != spec.NodeName || !job.Spec.Template.Spec.HostPID || !job.Spec.Template.Spec.HostNetwork {
		t.Fatalf("job is not pinned to the isolated host boundary: %+v", job.Spec.Template.Spec)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.Privileged == nil || !*container.SecurityContext.Privileged {
		t.Fatal("rebuild container is not privileged")
	}
	if container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem || len(container.VolumeMounts) != 7 {
		t.Fatalf("rebuild container does not use the expected read-only boundary: %+v", container)
	}
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == rebuildRetainedRoot+"/member-old" && !mount.ReadOnly {
			t.Fatal("retained storage was mounted writable")
		}
		if mount.MountPath == rebuildTargetRoot && mount.ReadOnly {
			t.Fatal("target storage was mounted read-only")
		}
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.ActiveDeadlineSeconds == nil {
		t.Fatalf("rebuild job is retryable or unbounded: %+v", job.Spec)
	}
	if job.Annotations[RebuildPlanDigestAnnotation] != spec.PlanDigest || len(job.OwnerReferences) != 1 {
		t.Fatalf("job lost immutable identity: %+v", job.ObjectMeta)
	}
}

func TestWorkloadRejectsUnsafeStorageAndImage(t *testing.T) {
	for name, mutate := range map[string]func(*WorkloadSpec){
		"mutable image":            func(spec *WorkloadSpec) { spec.Image = "ghcr.io/kubeovn/operator:latest" },
		"nondeterministic name":    func(spec *WorkloadSpec) { spec.Name = "rebuild-random" },
		"writable retained volume": func(spec *WorkloadSpec) { spec.Retained[0].ReadOnly = false },
		"shared target and backup": func(spec *WorkloadSpec) { spec.BackupClaim = spec.Target.Claim },
		"invalid helper":           func(spec *WorkloadSpec) { spec.Helper = "/bin/sh" },
	} {
		t.Run(name, func(t *testing.T) {
			spec := rebuildWorkloadFixture()
			mutate(&spec)
			if _, err := spec.Job(nil); err == nil {
				t.Fatal("unsafe rebuild workload was accepted")
			}
		})
	}
}

func TestIsolationProofBindsCanonicalPlan(t *testing.T) {
	plan := validPlan()
	canonical, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	proof := IsolationProof{PlanDigest: digest, OperationID: canonical.OperationID, RetainedMembers: []string{canonical.RetainedMembers[0].Name}, VerifiedAt: time.Now().UTC()}
	if err := proof.Validate(canonical); err != nil {
		t.Fatal(err)
	}
	proof.PlanDigest = "sha256:" + strings.Repeat("f", 64)
	if err := proof.Validate(canonical); err == nil {
		t.Fatal("proof with a different plan digest was accepted")
	}
}

func TestIsolationProofRejectsOldVerification(t *testing.T) {
	plan := validPlan()
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	proof := IsolationProof{PlanDigest: digest, OperationID: plan.OperationID, RetainedMembers: []string{"old"}, VerifiedAt: time.Now().Add(-time.Hour)}
	if err := proof.Validate(plan); err == nil {
		t.Fatal("expired isolation metadata was accepted")
	}
}

func TestIsolationProofAcceptsUnsortedPlan(t *testing.T) {
	plan := validPlan()
	plan.RetainedMembers = append(plan.RetainedMembers, member("earlier", "2", "b", "c", "d", "e"))
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	proof := IsolationProof{PlanDigest: digest, OperationID: plan.OperationID, RetainedMembers: []string{"old", "earlier"}, VerifiedAt: time.Now()}
	if err := proof.Validate(plan); err != nil {
		t.Fatalf("canonical-equivalent plan rejected: %v", err)
	}
}

func TestIsolationProofTimestampWindow(t *testing.T) {
	plan := validPlan()
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.November, 9, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		timestamp time.Time
		valid     bool
	}{
		{"missing", time.Time{}, false},
		{"old boundary", now.Add(-5 * time.Minute), true},
		{"expired", now.Add(-5*time.Minute - time.Nanosecond), false},
		{"future boundary", now.Add(time.Minute), true},
		{"future", now.Add(time.Minute + time.Nanosecond), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proof := IsolationProof{PlanDigest: digest, OperationID: plan.OperationID, RetainedMembers: []string{"old"}, VerifiedAt: tc.timestamp}
			if err := proof.validateAt(plan, now); (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
}
