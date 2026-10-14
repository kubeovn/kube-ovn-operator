package planner

import (
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestBuildRejectsUnsafeDataPlaneOnlySpec(t *testing.T) {
	plan := Build(operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0",
		Mode:    operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
	}, "")
	if plan.Phase != operatorv1alpha1.PhaseDegraded || plan.Blocking == "" {
		t.Fatalf("expected a degraded plan, got %#v", plan)
	}
}

func TestBuildOrdersMembershipAndHCPGates(t *testing.T) {
	plan := Build(operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0",
		Mode:    operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		HCP:     operatorv1alpha1.HCPConfig{Enabled: true, Role: operatorv1alpha1.HCPRoleManagement, PairRef: "tenant-a", NBEndpoint: "nb.example", SBEndpoint: "sb.example"},
	}, "")
	for _, action := range []string{"AcquireCentralMembershipLease", "ValidateQuorum", "JoinBeforeReplicaIncrease", "LeaveBeforeReplicaDecrease", "CoordinateHCPPeer"} {
		found := false
		for _, planned := range plan.Actions {
			if planned == action {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing action %q in %#v", action, plan.Actions)
		}
	}
}
