package hcp

import (
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestValidatePairRequiresComplementaryRolesAndMatchingVersion(t *testing.T) {
	local := validConfig()
	peer := Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "ssl:sb.remote:6642"}
	if err := ValidatePair(local, "v1.17.0", peer); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePair(local, "v1.16.0", peer); err == nil {
		t.Fatal("expected version mismatch")
	}
}

func TestValidatePairRejectsSameRoleAndWrongPairRef(t *testing.T) {
	local := validConfig()
	local.Role = operatorv1alpha1.HCPRoleDataPlane
	local.PairRef = "other"
	peer := Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "tcp:sb.remote:6642"}
	if err := ValidatePair(local, "v1.17.0", peer); err == nil {
		t.Fatal("expected invalid pair")
	}
}

func TestValidateConfigRequiresNamespaceSecretAndStrictEndpoints(t *testing.T) {
	config := validConfig()
	if err := ValidateConfig(config); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*operatorv1alpha1.HCPConfig)
		want   string
	}{
		{"namespace", func(config *operatorv1alpha1.HCPConfig) { config.CentralNamespace = "bad namespace" }, "centralNamespace"},
		{"secret", func(config *operatorv1alpha1.HCPConfig) { config.TLSSecretRef = nil }, "tlsSecretRef"},
		{"protocol", func(config *operatorv1alpha1.HCPConfig) { config.NBEndpoint = "http:nb.local:6641" }, "tcp or ssl"},
		{"host", func(config *operatorv1alpha1.HCPConfig) { config.NBEndpoint = "tcp::6641" }, "host"},
		{"port", func(config *operatorv1alpha1.HCPConfig) { config.NBEndpoint = "tcp:nb.local:0" }, "port"},
		{"missing port", func(config *operatorv1alpha1.HCPConfig) { config.NBEndpoint = "tcp:nb.local" }, "missing port"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			candidate := config
			testCase.mutate(&candidate)
			if err := ValidateConfig(candidate); err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("ValidateConfig() error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestValidateConfigAcceptsIPv6AndDisabledOnlyFields(t *testing.T) {
	config := validConfig()
	config.NBEndpoint = "ssl:[2001:db8::1]:6641"
	config.SBEndpoint = "tcp:central.example:6642"
	if err := ValidateConfig(config); err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEndpoint(config.NBEndpoint)
	if err != nil || parsed.Protocol != "ssl" || parsed.Host != "2001:db8::1" || parsed.Port != 6641 {
		t.Fatalf("unexpected parsed IPv6 endpoint: %+v %v", parsed, err)
	}
	if err := ValidateConfig(operatorv1alpha1.HCPConfig{Enabled: false, NBEndpoint: "not-an-endpoint"}); err != nil {
		t.Fatalf("disabled HCP config should not require connection fields: %v", err)
	}
}

func validConfig() operatorv1alpha1.HCPConfig {
	return operatorv1alpha1.HCPConfig{
		Enabled: true, Role: operatorv1alpha1.HCPRoleManagement, PairRef: "data",
		CentralNamespace: "hcp", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
		NBEndpoint: "tcp:nb.local:6641", SBEndpoint: "ssl:sb.local:6642",
	}
}
