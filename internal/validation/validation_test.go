package validation

import (
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestValidateAdoptionRequiresValidSelector(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central:  operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		Adoption: operatorv1alpha1.AdoptionSpec{Enabled: true},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "selector must contain") {
		t.Fatalf("missing adoption selector was accepted: %v", errors)
	}
	spec.Adoption.Selector = map[string]string{"bad key": "value"}
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "selector key") {
		t.Fatalf("invalid adoption selector key was accepted: %v", errors)
	}
	spec.Adoption.Selector = map[string]string{"app": "kube-ovn"}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid adoption selector was rejected: %v", errors)
	}
}

func TestValidateBackupScheduleRequiresPolicyAndDestination(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		Upgrade: operatorv1alpha1.UpgradeSpec{Backup: operatorv1alpha1.BackupSpec{Schedule: "6h"}},
	}
	joined := JoinErrors(ValidateInstallationSpec(spec))
	if !strings.Contains(joined, "required must be true") || !strings.Contains(joined, "destination is required") {
		t.Fatalf("missing schedule prerequisites were not rejected: %s", joined)
	}
	spec.Upgrade.Backup.Required = true
	spec.Upgrade.Backup.Destination = "s3://example/kube-ovn"
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid backup schedule was rejected: %v", errors)
	}
}

func TestValidateBackupScheduleRejectsInvalidDuration(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		Upgrade: operatorv1alpha1.UpgradeSpec{Backup: operatorv1alpha1.BackupSpec{Required: true, Destination: "s3://example/kube-ovn", Schedule: "15s"}},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "at least") {
		t.Fatalf("invalid backup schedule was accepted: %v", errors)
	}
}
