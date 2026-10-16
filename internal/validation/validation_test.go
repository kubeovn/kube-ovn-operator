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
