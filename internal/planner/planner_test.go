package planner

import (
	"slices"
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
		Mode:    operatorv1alpha1.InstallationModeControlPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleManagement, PairRef: "tenant-a", CentralNamespace: "hcp",
			NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "ssl:sb.example:6642",
			TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
		},
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
	for index, action := range []string{"ValidateVersionContract", "AcquireInstallationLease", "RenderChartValues", "AcquireCentralMembershipLease", "ValidateQuorum", "JoinBeforeReplicaIncrease", "LeaveBeforeReplicaDecrease", "CoordinateHCPPeer", "ResolveDualStackEndpoints", "VerifyPeerVersionContract", "PublishObservedStatus"} {
		if plan.Actions[index] != action {
			t.Fatalf("action %d should be %q, got %q in %#v", index, action, plan.Actions[index], plan.Actions)
		}
	}
}

func TestBuildListsDurableUpgradeRolloutBoundaries(t *testing.T) {
	plan := Build(operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.18.0",
		Mode:    operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
	}, "v1.17.0")
	want := []string{"MigrateCRDs", "WaitForCRDsReady", "RollCentralBeforeDataPlane", "VerifyCentralReadiness", "RollDataPlane", "VerifyNetworkProbes"}
	for _, action := range want {
		found := false
		for _, planned := range plan.Actions {
			if planned == action {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("upgrade plan is missing %q: %#v", action, plan.Actions)
		}
	}
	for index := 1; index < len(want); index++ {
		previous, current := slices.Index(plan.Actions, want[index-1]), slices.Index(plan.Actions, want[index])
		if previous >= current {
			t.Fatalf("upgrade actions are out of order: %#v", plan.Actions)
		}
	}
}

func TestBuildMapsHCPDataPlaneEndpointsToChartValues(t *testing.T) {
	plan := Build(operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "tcp:nb.example:30641", SBEndpoint: "tcp:sb.example:30642",
			TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
		},
	}, "")
	if plan.Blocking != "" || plan.Values["externalOvnCentral.nbEndpoint"] != "nb.example" || plan.Values["externalOvnCentral.sbPort"] != "30642" {
		t.Fatalf("HCP endpoints were not normalized in plan values: %#v", plan)
	}
}
