package hcp

import (
	"context"
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// PeerRequest is the complete input to a hosted-control-plane provider. It
// deliberately carries a Secret reference through HCPConfig, never Secret
// data, certificates or a Kubernetes object that could contain credentials.
type PeerRequest struct {
	Installation types.NamespacedName
	Version      string
	Config       operatorv1alpha1.HCPConfig
	// TLSSecret identifies the exact Secret generation that the provider must
	// use. It deliberately contains metadata only; Secret data stays inside
	// the provider's client and is never copied into this request.
	TLSSecret SecretIdentity
}

// SecretIdentity is the non-sensitive identity of the TLS Secret used by an
// HCP peer. A UID change means the Secret was replaced and requires a fresh
// provider observation before readiness can be claimed.
type SecretIdentity struct {
	Namespace       string
	Name            string
	UID             types.UID
	ResourceVersion string
}

// PeerReport contains only non-sensitive peer identity and health evidence.
// A provider must set Ready only after Paired and EndpointReady are both true
// and the peer satisfies ValidatePair for the request's version and role.
type PeerReport struct {
	Peer          Peer
	TLSSecret     SecretIdentity
	Paired        bool
	EndpointReady bool
	Ready         bool
	Reason        string
	Message       string
}

// PeerProvider observes and coordinates the remote side of an HCP pair. The
// transport and Secret lifecycle remain behind this interface so the
// controller cannot accidentally copy private key material into status,
// ConfigMaps or rendered chart values.
type PeerProvider interface {
	Observe(context.Context, PeerRequest) (PeerReport, error)
}

// ValidateReport checks the provider's externally visible contract before the
// controller uses it as a readiness proof. Pending reports may omit peer
// identity while the remote side is still being discovered; once a provider
// claims pairing or readiness, the full complementary contract is required.
func ValidateReport(request PeerRequest, report PeerReport) error {
	if !request.Config.Enabled || (!report.Paired && !report.Ready) {
		return nil
	}
	if err := ValidatePair(request.Config, request.Version, report.Peer); err != nil {
		return err
	}
	if report.TLSSecret.Namespace != request.TLSSecret.Namespace || report.TLSSecret.Name != request.TLSSecret.Name || report.TLSSecret.UID != request.TLSSecret.UID || report.TLSSecret.ResourceVersion != request.TLSSecret.ResourceVersion {
		return fmt.Errorf("HCP provider observed a different TLS Secret generation")
	}
	if report.Ready && (!report.Paired || !report.EndpointReady) {
		return fmt.Errorf("HCP provider marked peer ready without paired endpoints")
	}
	return nil
}
