package planner

import (
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"github.com/kubeovn/kube-ovn-operator/internal/validation"
)

type Plan struct {
	Phase    operatorv1alpha1.Phase
	Step     string
	Actions  []string
	Values   map[string]string
	Blocking string
}

// Build creates an idempotent plan. Membership actions are descriptions for
// the central admin adapter; they are intentionally not shell commands.
func Build(spec operatorv1alpha1.KubeOVNInstallationSpec, observedVersion string) Plan {
	if errs := validation.ValidateInstallationSpec(spec); len(errs) > 0 {
		return Plan{Phase: operatorv1alpha1.PhaseDegraded, Step: "Validate", Blocking: validation.JoinErrors(errs)}
	}
	actions := []string{"ValidateVersionContract", "AcquireInstallationLease", "RenderChartValues"}
	step := "Render"
	phase := operatorv1alpha1.PhaseInstalling
	if spec.Adoption.Enabled {
		phase = operatorv1alpha1.PhaseAdopting
		actions = append(actions, "InspectExistingOwnership", "ValidateClusterAndServerIDs")
	}
	if observedVersion != "" && observedVersion != spec.Version {
		phase = operatorv1alpha1.PhaseUpgrading
		step = "Validate"
		actions = append(actions, "PreflightUpgrade", "CreateBackupCheckpoint", "MigrateCRDs", "WaitForCRDsReady", "RollCentralBeforeDataPlane", "VerifyCentralReadiness", "RollDataPlane", "VerifyNetworkProbes")
		if spec.Upgrade.AllowRollback {
			actions = append(actions, "RecordRollbackCheckpoint")
		}
	}
	if spec.Central.Mode == operatorv1alpha1.CentralModeRaft {
		actions = append(actions, "AcquireCentralMembershipLease", "ValidateQuorum", "JoinBeforeReplicaIncrease", "LeaveBeforeReplicaDecrease")
	}
	if spec.HCP.Enabled {
		actions = append(actions, "CoordinateHCPPeer", "ResolveDualStackEndpoints", "VerifyPeerVersionContract")
	}
	actions = append(actions, "PublishObservedStatus")
	values := map[string]string{
		"installMode":     string(spec.Mode),
		"centralMode":     string(spec.Central.Mode),
		"centralReplicas": fmt.Sprintf("%d", spec.Central.Replicas),
		"version":         spec.Version,
	}
	external := spec.DataPlane.ExternalOVNCentral
	if spec.HCP.Enabled && spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly {
		if northbound, err := hcp.ParseEndpoint(spec.HCP.NBEndpoint); err == nil {
			if southbound, err := hcp.ParseEndpoint(spec.HCP.SBEndpoint); err == nil {
				external.NBEndpoint, external.NBPort = northbound.Host, northbound.Port
				external.SBEndpoint, external.SBPort = southbound.Host, southbound.Port
			}
		}
	}
	if external.NBEndpoint != "" {
		values["externalOvnCentral.nbEndpoint"] = external.NBEndpoint
		values["externalOvnCentral.sbEndpoint"] = external.SBEndpoint
		if external.NBPort != 0 {
			values["externalOvnCentral.nbPort"] = fmt.Sprintf("%d", external.NBPort)
		}
		if external.SBPort != 0 {
			values["externalOvnCentral.sbPort"] = fmt.Sprintf("%d", external.SBPort)
		}
	}
	return Plan{Phase: phase, Step: step, Actions: actions, Values: values}
}
