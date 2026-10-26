package hcp

import (
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestValidateReportAllowsPendingDiscoveryWithoutPeerData(t *testing.T) {
	request := PeerRequest{Installation: types.NamespacedName{Namespace: "network", Name: "tenant"}, Version: "v1.17.0", Config: validConfig()}
	if err := ValidateReport(request, PeerReport{Reason: "PeerPending"}); err != nil {
		t.Fatalf("pending peer discovery was rejected: %v", err)
	}
}

func TestValidateReportRequiresACompletePairedContract(t *testing.T) {
	request := PeerRequest{Version: "v1.17.0", Config: validConfig(), TLSSecret: testSecret()}
	report := PeerReport{Paired: true, EndpointReady: true, Ready: true, TLSSecret: testSecret(), Peer: Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "ssl:sb.remote:6642"}}
	if err := ValidateReport(request, report); err != nil {
		t.Fatalf("valid paired report was rejected: %v", err)
	}
	report.Ready = false
	report.Peer.Version = "v1.16.0"
	if err := ValidateReport(request, report); err == nil || !strings.Contains(err.Error(), "versions must match") {
		t.Fatalf("version drift was not rejected: %v", err)
	}
}

func TestValidateReportRejectsReadyWithoutEndpointEvidence(t *testing.T) {
	request := PeerRequest{Version: "v1.17.0", Config: validConfig(), TLSSecret: testSecret()}
	report := PeerReport{Paired: true, Ready: true, TLSSecret: testSecret(), Peer: Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "ssl:sb.remote:6642"}}
	if err := ValidateReport(request, report); err == nil || !strings.Contains(err.Error(), "without paired endpoints") {
		t.Fatalf("ready report without endpoint evidence was accepted: %v", err)
	}
}

func TestValidateReportRejectsStaleTLSSecretGeneration(t *testing.T) {
	request := PeerRequest{Version: "v1.17.0", Config: validConfig(), TLSSecret: testSecret()}
	report := PeerReport{
		Paired: true, EndpointReady: true, Ready: true,
		TLSSecret: SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: types.UID("replaced-secret"), ResourceVersion: "8"},
		Peer:      Peer{Name: "data", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleDataPlane, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "ssl:sb.remote:6642"},
	}
	if err := ValidateReport(request, report); err == nil || !strings.Contains(err.Error(), "different TLS Secret generation") {
		t.Fatalf("stale TLS Secret generation was accepted: %v", err)
	}
}

func testSecret() SecretIdentity {
	return SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: types.UID("secret-uid"), ResourceVersion: "7"}
}
