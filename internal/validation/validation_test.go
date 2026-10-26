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

func TestValidateDataPlaneNodeSelector(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		DataPlane: operatorv1alpha1.DataPlaneSpec{NodeSelector: map[string]string{"network.example/zone": "edge-a"}, ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example", SBEndpoint: "sb.example"}},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid data-plane node selector was rejected: %v", errors)
	}
	spec.DataPlane.NodeSelector = map[string]string{"bad key": "edge-a"}
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "nodeSelector key") {
		t.Fatalf("invalid data-plane selector key was accepted: %v", errors)
	}
	spec.DataPlane.NodeSelector = map[string]string{"network.example/zone": "bad value!"}
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "nodeSelector value") {
		t.Fatalf("invalid data-plane selector value was accepted: %v", errors)
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

func TestValidateHCPConfigRequiresBoundedTransportInputs(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		DataPlane: operatorv1alpha1.DataPlaneSpec{ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example", SBEndpoint: "sb.example"}},
		HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management",
			CentralNamespace: "hcp", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
			NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "tcp:sb.example:6642",
		},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid HCP config was rejected: %v", errors)
	}
	spec.DataPlane.ExternalOVNCentral = operatorv1alpha1.EndpointSpec{}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("HCP endpoints should satisfy data-plane central configuration: %v", errors)
	}
	spec.HCP.TLSSecretRef = nil
	joined := JoinErrors(ValidateInstallationSpec(spec))
	if !strings.Contains(joined, "tlsSecretRef") {
		t.Fatalf("missing HCP TLS Secret was not rejected: %s", joined)
	}
	spec.HCP.TLSSecretRef = &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"}
	spec.HCP.NBEndpoint = "nb.example:6641"
	joined = JoinErrors(ValidateInstallationSpec(spec))
	if !strings.Contains(joined, "tcp or ssl") {
		t.Fatalf("invalid HCP endpoint was not rejected: %s", joined)
	}
	spec.HCP.NBEndpoint = "tcp:nb.example:6641"
	spec.Mode = operatorv1alpha1.InstallationModeControlPlaneOnly
	joined = JoinErrors(ValidateInstallationSpec(spec))
	if !strings.Contains(joined, "role=dataPlane requires spec.mode=dataPlaneOnly") {
		t.Fatalf("HCP role/mode mismatch was not rejected: %s", joined)
	}
}

func TestValidateMemberRequestsRequireDynamicMembershipAndPinnedImage(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeFull,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 5, MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "member-new", NodeName: "node", ClaimName: "claim", VolumeName: "volume", Image: "example/central:dev", NorthboundAddress: "tcp:192.0.2.10:6643", SouthboundAddress: "tcp:192.0.2.10:6644", NorthboundClientAddress: "tcp:192.0.2.10:6641", SouthboundClientAddress: "tcp:192.0.2.10:6642"}}},
	}
	joined := JoinErrors(ValidateInstallationSpec(spec))
	if !strings.Contains(joined, "dynamicMembership") || !strings.Contains(joined, "sha256") {
		t.Fatalf("invalid member request was not fenced: %s", joined)
	}
	spec.Central.DynamicMembership = true
	spec.Central.MemberRequests[0].Image = "example/central@sha256:" + strings.Repeat("a", 64)
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid member request was rejected: %v", errors)
	}
}

func TestValidateMemberRequestReplacementEndpoint(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{{
			Name: "member-replacement", ReplaceEndpoint: "member-0-0", NodeName: "node", ClaimName: "claim", VolumeName: "volume",
			Image: "example/central@sha256:" + strings.Repeat("a", 64), NorthboundAddress: "tcp:192.0.2.10:6643", SouthboundAddress: "tcp:192.0.2.10:6644", NorthboundClientAddress: "tcp:192.0.2.10:6641", SouthboundClientAddress: "tcp:192.0.2.10:6642",
		}}},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid replacement endpoint was rejected: %v", errors)
	}
	spec.Central.MemberRequests[0].RemovalApprovalRef = &operatorv1alpha1.MemberRemovalApprovalReference{Name: "lost-approval", UID: "approval-uid"}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid permanent-loss approval reference was rejected: %v", errors)
	}
	spec.Central.MemberRequests[0].ReplaceEndpoint = ""
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid automatic replacement approval reference was rejected: %v", errors)
	}
	spec.Central.MemberRequests[0].RemovalApprovalRef.UID = ""
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "removalApprovalRef") {
		t.Fatalf("incomplete permanent-loss approval reference was accepted: %v", errors)
	}
	spec.Central.MemberRequests[0].ReplaceEndpoint = "bad endpoint"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "replaceEndpoint") {
		t.Fatalf("invalid replacement endpoint was accepted: %v", errors)
	}
}

func TestValidateMemberRequestTLSProtocolBinding(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{validMemberRequest("tls")}},
	}
	member := &spec.Central.MemberRequests[0]
	member.NorthboundAddress = "ssl:192.0.2.10:6643"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "one protocol") {
		t.Fatalf("mixed member protocols were accepted: %v", errors)
	}
	member.SouthboundAddress = "ssl:192.0.2.10:6644"
	member.NorthboundClientAddress = "ssl:192.0.2.10:6641"
	member.SouthboundClientAddress = "ssl:192.0.2.10:6642"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "tlsSecretRef") {
		t.Fatalf("SSL member without a Secret was accepted: %v", errors)
	}
	member.TLSSecretRef = &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid SSL member request was rejected: %v", errors)
	}
	member.TLSSecretRef.Name = "bad name"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "tlsSecretRef.name") {
		t.Fatalf("invalid member TLS Secret name was accepted: %v", errors)
	}
}

func TestValidateMemberRequestRetirementEndpoint(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{{Name: "retire", RetireEndpoint: "ovn-central-abc-0"}}},
	}
	if errors := ValidateInstallationSpec(spec); len(errors) != 0 {
		t.Fatalf("valid retirement-only request was rejected: %v", errors)
	}
	spec.Central.MemberRequests[0].ReplaceEndpoint = "ovn-central-abc-0"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "both replaceEndpoint and retireEndpoint") {
		t.Fatalf("conflicting retirement and replacement endpoints were accepted: %v", errors)
	}
}

func TestValidateMemberRequestsAllowOnlyOneReplacementTarget(t *testing.T) {
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeControlPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3, DynamicMembership: true, MemberRequests: []operatorv1alpha1.CentralMemberSpec{
			validMemberRequest("first"), validMemberRequest("second"),
		}},
	}
	spec.Central.MemberRequests[0].ReplaceEndpoint = "central-0"
	spec.Central.MemberRequests[1].ReplaceEndpoint = "central-1"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "only one replaceEndpoint") {
		t.Fatalf("expected multiple replacement targets to be rejected: %v", errors)
	}

	spec.Central.MemberRequests[1].ReplaceEndpoint = "central-0"
	if errors := ValidateInstallationSpec(spec); len(errors) == 0 || !strings.Contains(JoinErrors(errors), "duplicates target") {
		t.Fatalf("expected duplicate replacement target to be rejected: %v", errors)
	}
}

func validMemberRequest(name string) operatorv1alpha1.CentralMemberSpec {
	return operatorv1alpha1.CentralMemberSpec{
		Name: name, NodeName: "node-" + name, ClaimName: "claim-" + name, VolumeName: "volume-" + name,
		Image:             "example/central@sha256:" + strings.Repeat("a", 64),
		NorthboundAddress: "tcp:192.0.2.10:6643", SouthboundAddress: "tcp:192.0.2.10:6644",
		NorthboundClientAddress: "tcp:192.0.2.10:6641", SouthboundClientAddress: "tcp:192.0.2.10:6642",
	}
}
