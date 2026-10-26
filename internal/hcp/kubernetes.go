package hcp

import (
	"context"
	"crypto/tls"
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesPeerProvider observes a peer KubeOVNInstallation in the same
// Kubernetes API and probes the configured OVN endpoints with the local
// installation's generation-pinned TLS Secret. Cross-cluster deployments can
// provide a different PeerProvider; this implementation deliberately never
// copies Secret data into the peer report or Kubernetes status.
type KubernetesPeerProvider struct {
	Reader  client.Reader
	LoadTLS func(context.Context, client.Reader, PeerRequest) (*tls.Config, error)
	Probe   func(context.Context, operatorv1alpha1.HCPConfig, *tls.Config) error
}

var _ PeerProvider = (*KubernetesPeerProvider)(nil)

func (p *KubernetesPeerProvider) Observe(ctx context.Context, request PeerRequest) (PeerReport, error) {
	report := PeerReport{TLSSecret: request.TLSSecret}
	if !request.Config.Enabled {
		return report, nil
	}
	if p == nil || p.Reader == nil {
		return report, fmt.Errorf("HCP Kubernetes peer provider requires a Kubernetes reader")
	}

	peer := new(operatorv1alpha1.KubeOVNInstallation)
	key := client.ObjectKey{Namespace: request.Config.CentralNamespace, Name: request.Config.PairRef}
	if err := p.Reader.Get(ctx, key, peer); err != nil {
		if apierrors.IsNotFound(err) {
			report.Reason = "PeerNotFound"
			report.Message = fmt.Sprintf("HCP peer %s/%s was not found", key.Namespace, key.Name)
			return report, nil
		}
		return report, fmt.Errorf("read HCP peer %s/%s: %w", key.Namespace, key.Name, err)
	}

	peerConfig := peer.Spec.HCP
	report.Peer = Peer{
		Name:       peer.Name,
		Version:    peer.Spec.Version,
		Role:       peerConfig.Role,
		NBEndpoint: peerConfig.NBEndpoint,
		SBEndpoint: peerConfig.SBEndpoint,
	}
	if !peerConfig.Enabled {
		return pendingReport(&report, "PeerHCPDisabled", "the referenced HCP peer is not enabled")
	}
	if peerConfig.PairRef != request.Installation.Name || peerConfig.CentralNamespace != request.Installation.Namespace {
		return pendingReport(&report, "PeerPairRefMismatch", "the referenced HCP peer does not point back to this installation")
	}
	if err := ValidateConfig(peerConfig); err != nil {
		return pendingReport(&report, "PeerContractInvalid", err.Error())
	}
	if err := ValidatePair(request.Config, request.Version, report.Peer); err != nil {
		return pendingReport(&report, "PairContractInvalid", err.Error())
	}
	report.Paired = true
	if peer.Spec.Version == "" || peer.Status.ObservedVersion != peer.Spec.Version || !conditionTrue(peer.Status.Conditions, "WorkloadsReady") {
		return pendingReport(&report, "PeerNotReady", "the referenced HCP peer has not reported its requested workload version ready")
	}

	loadTLS := p.LoadTLS
	if loadTLS == nil {
		loadTLS = LoadTLSConfig
	}
	tlsConfig, err := loadTLS(ctx, p.Reader, request)
	if err != nil {
		return pendingReport(&report, "TLSSecretInvalid", err.Error())
	}
	probe := p.Probe
	if probe == nil {
		probe = ProbeEndpoints
	}
	if err := probe(ctx, request.Config, tlsConfig); err != nil {
		return pendingReport(&report, "EndpointUnavailable", err.Error())
	}
	report.EndpointReady = true
	report.Ready = true
	report.Reason = "PeerReady"
	report.Message = "the reciprocal HCP peer is ready and both configured endpoints are reachable"
	return report, nil
}

func pendingReport(report *PeerReport, reason, message string) (PeerReport, error) {
	report.Reason = reason
	report.Message = message
	return *report, nil
}

func conditionTrue(conditions []metav1.Condition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}
