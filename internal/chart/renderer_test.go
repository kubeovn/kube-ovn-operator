package chart

import (
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestNetworkStackPreservesHCPTransportValues(t *testing.T) {
	for _, stack := range []string{"", "ipv4", "ipv6", "dual_stack"} {
		t.Run(stack, func(t *testing.T) {
			spec := operatorv1alpha1.KubeOVNInstallationSpec{
				Version: "v1.16.13", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly, NetworkStack: stack,
				Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
				HCP:     operatorv1alpha1.HCPConfig{Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp", NBEndpoint: "ssl:nb.example:6641", SBEndpoint: "ssl:sb.example:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"}},
			}
			values, err := installationValues(spec)
			if err != nil {
				t.Fatal(err)
			}
			networking := values["networking"].(map[string]any)
			if networking["ENABLE_SSL"] != true {
				t.Fatal("network stack selection lost HCP TLS")
			}
			if got, _ := networking["NET_STACK"].(string); got != stack {
				t.Fatalf("network stack = %q, want %q", got, stack)
			}
			if _, found := networking["NET_STACK"]; stack == "" && found {
				t.Fatal("omitted stack changed upstream defaults")
			}
		})
	}
}

func TestValuesRendererIncludesModeCentralAndHCPWithoutCredentials(t *testing.T) {
	rendered, err := (ValuesRenderer{}).Render(t.Context(), operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		DataPlane: operatorv1alpha1.DataPlaneSpec{ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example", SBEndpoint: "sb.example"}},
		HCP:       operatorv1alpha1.HCPConfig{Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "tenant-a", CentralNamespace: "hcp", NBEndpoint: "tcp:hcp-nb:6641", SBEndpoint: "tcp:hcp-sb:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(rendered.ValuesYAML)
	for _, want := range []string{"installMode: dataPlaneOnly", "OVN_CENTRAL_MODE: cluster", "tag: v1.17.0", "nbEndpoint: hcp-nb", "nbPort: 6641"} {
		if !strings.Contains(text, want) {
			t.Fatalf("rendered values missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "private") || strings.Contains(text, "certificate") {
		t.Fatalf("rendered values contain credential-like data:\n%s", text)
	}
}
