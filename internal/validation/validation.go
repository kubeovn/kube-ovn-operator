package validation

import (
	"fmt"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// ValidateInstallationSpec enforces invariants that must hold before a plan
// can write any child resource or change OVNDB membership.
func ValidateInstallationSpec(spec operatorv1alpha1.KubeOVNInstallationSpec) []error {
	errs := make([]error, 0, 4)
	if strings.TrimSpace(spec.Version) == "" || strings.EqualFold(spec.Version, "latest") {
		errs = append(errs, fmt.Errorf("spec.version must be a pinned version and cannot be latest"))
	}
	switch spec.Mode {
	case operatorv1alpha1.InstallationModeFull, operatorv1alpha1.InstallationModeControlPlaneOnly, operatorv1alpha1.InstallationModeDataPlaneOnly:
	default:
		errs = append(errs, fmt.Errorf("spec.mode must be full, controlPlaneOnly, or dataPlaneOnly"))
	}
	switch spec.Central.Mode {
	case operatorv1alpha1.CentralModeRaft:
		if spec.Central.Replicas != 3 && spec.Central.Replicas != 5 {
			errs = append(errs, fmt.Errorf("spec.central.replicas must be 3 or 5 for raft mode"))
		}
	case operatorv1alpha1.CentralModeSingle:
		if spec.Central.Replicas != 1 {
			errs = append(errs, fmt.Errorf("spec.central.replicas must be 1 for single mode"))
		}
		if strings.TrimSpace(spec.Central.Storage.Size) == "" {
			errs = append(errs, fmt.Errorf("spec.central.storage.size is required for single mode"))
		}
	default:
		errs = append(errs, fmt.Errorf("spec.central.mode must be raft or single"))
	}
	if spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly {
		if strings.TrimSpace(spec.DataPlane.ExternalOVNCentral.NBEndpoint) == "" || strings.TrimSpace(spec.DataPlane.ExternalOVNCentral.SBEndpoint) == "" {
			errs = append(errs, fmt.Errorf("dataPlaneOnly requires externalOvnCentral.nbEndpoint and sbEndpoint"))
		}
	}
	if spec.Upgrade.Backup.Required && strings.TrimSpace(spec.Upgrade.Backup.Destination) == "" {
		errs = append(errs, fmt.Errorf("spec.upgrade.backup.destination is required when backup.required is true"))
	}
	if spec.Adoption.Enabled {
		if len(spec.Adoption.Selector) == 0 {
			errs = append(errs, fmt.Errorf("spec.adoption.selector must contain at least one label when adoption is enabled"))
		}
		for key, value := range spec.Adoption.Selector {
			if messages := kvalidation.IsQualifiedName(key); len(messages) != 0 {
				errs = append(errs, fmt.Errorf("spec.adoption.selector key %q is invalid: %s", key, messages[0]))
			}
			if messages := kvalidation.IsValidLabelValue(value); len(messages) != 0 {
				errs = append(errs, fmt.Errorf("spec.adoption.selector value for %q is invalid: %s", key, messages[0]))
			}
		}
	}
	if spec.NetworkProbe != nil {
		image := spec.NetworkProbe.Image
		lastPart := image[strings.LastIndex(image, "/")+1:]
		_, tag, pinned := strings.Cut(lastPart, ":")
		if len(image) > 512 || strings.ContainsAny(image, " \t\n\r") || !pinned || tag == "" || tag == "latest" {
			errs = append(errs, fmt.Errorf("spec.networkProbe.image requires an explicit non-latest tag or digest"))
		}
		if spec.Mode == operatorv1alpha1.InstallationModeControlPlaneOnly {
			errs = append(errs, fmt.Errorf("controlPlaneOnly cannot schedule data-plane network probes"))
		}
	}
	if spec.HCP.Enabled {
		if spec.HCP.Role != operatorv1alpha1.HCPRoleManagement && spec.HCP.Role != operatorv1alpha1.HCPRoleDataPlane {
			errs = append(errs, fmt.Errorf("spec.hcp.role must be management or dataPlane when HCP is enabled"))
		}
		if strings.TrimSpace(spec.HCP.PairRef) == "" {
			errs = append(errs, fmt.Errorf("spec.hcp.pairRef is required when HCP is enabled"))
		}
		if strings.TrimSpace(spec.HCP.NBEndpoint) == "" || strings.TrimSpace(spec.HCP.SBEndpoint) == "" {
			errs = append(errs, fmt.Errorf("spec.hcp.nbEndpoint and sbEndpoint are required when HCP is enabled"))
		}
	}
	if spec.Central.Mode == operatorv1alpha1.CentralModeRaft && spec.Central.DynamicMembership && spec.Upgrade.Backup.Required && spec.Upgrade.Backup.Destination == "" {
		errs = append(errs, fmt.Errorf("dynamic membership cannot be enabled without a backup destination"))
	}
	return errs
}

func JoinErrors(errs []error) string {
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "; ")
}
