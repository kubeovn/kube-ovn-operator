package hcp

import (
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestValidatePairRequiresComplementaryRolesAndMatchingVersion(t *testing.T) {
	local := operatorv1alpha1.HCPConfig{Enabled: true, Role: operatorv1alpha1.HCPRoleManagement, PairRef: "data", NBEndpoint: "nb.local:6641", SBEndpoint: "sb.local:6642"}
	peer := Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "nb.remote:6641", SBEndpoint: "sb.remote:6642"}
	if err := ValidatePair(local, "v1.17.0", peer); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePair(local, "v1.16.0", peer); err == nil {
		t.Fatal("expected version mismatch")
	}
}

func TestValidatePairRejectsSameRoleAndWrongPairRef(t *testing.T) {
	local := operatorv1alpha1.HCPConfig{Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "other", NBEndpoint: "nb.local:6641", SBEndpoint: "sb.local:6642"}
	peer := Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "nb.remote:6641", SBEndpoint: "sb.remote:6642"}
	if err := ValidatePair(local, "v1.17.0", peer); err == nil {
		t.Fatal("expected invalid pair")
	}
}
