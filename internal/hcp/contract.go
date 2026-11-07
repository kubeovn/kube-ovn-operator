// Package hcp validates the two-sided contract for hosted control-plane
// installations. It never transports or copies Secret data.
package hcp

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
)

type Peer struct {
	Name       string
	Version    string
	Role       operatorv1alpha1.HCPRole
	NBEndpoint string
	SBEndpoint string
}

// Endpoint is the parsed form of an OVN client endpoint. Host never includes
// IPv6 brackets; callers that serialize it must add them when required by the
// target protocol.
type Endpoint struct {
	Protocol string
	Host     string
	Port     int32
}

// ValidateConfig checks the local, non-secret HCP contract before any chart or
// child resource is written. Endpoint values use OVN's address form:
// tcp:<host>:<port> or ssl:<host>:<port>. TLS material is referenced by name
// only; the operator never copies Secret data into a CR or status field.
func ValidateConfig(config operatorv1alpha1.HCPConfig) error {
	if !config.Enabled {
		return nil
	}
	if config.Role != operatorv1alpha1.HCPRoleManagement && config.Role != operatorv1alpha1.HCPRoleDataPlane {
		return fmt.Errorf("HCP role must be management or dataPlane")
	}
	if strings.TrimSpace(config.PairRef) == "" {
		return fmt.Errorf("HCP pairRef is required")
	}
	if config.CentralNamespace == "" {
		return fmt.Errorf("HCP centralNamespace is required")
	} else if messages := kvalidation.IsDNS1123Label(config.CentralNamespace); len(messages) != 0 {
		return fmt.Errorf("HCP centralNamespace %q is invalid: %s", config.CentralNamespace, messages[0])
	}
	if config.TLSSecretRef == nil || strings.TrimSpace(config.TLSSecretRef.Name) == "" {
		return fmt.Errorf("HCP tlsSecretRef.name is required")
	}
	if messages := kvalidation.IsDNS1123Subdomain(config.TLSSecretRef.Name); len(messages) != 0 {
		return fmt.Errorf("HCP tlsSecretRef.name %q is invalid: %s", config.TLSSecretRef.Name, messages[0])
	}
	for _, endpoint := range []struct {
		name  string
		value string
	}{
		{name: "nbEndpoint", value: config.NBEndpoint},
		{name: "sbEndpoint", value: config.SBEndpoint},
	} {
		if err := validateEndpoint(endpoint.name, endpoint.value); err != nil {
			return err
		}
	}
	return nil
}

func ValidatePair(local operatorv1alpha1.HCPConfig, localVersion string, peer Peer) error {
	if !local.Enabled {
		return nil
	}
	if err := ValidateConfig(local); err != nil {
		return err
	}
	if local.Role == operatorv1alpha1.HCPRoleNone || peer.Role == operatorv1alpha1.HCPRoleNone {
		return fmt.Errorf("HCP pair roles must be management or dataPlane")
	}
	if local.Role == peer.Role {
		return fmt.Errorf("HCP pair roles must be complementary, got %q and %q", local.Role, peer.Role)
	}
	if strings.TrimSpace(local.PairRef) == "" || local.PairRef != peer.Name {
		return fmt.Errorf("HCP pairRef %q does not identify peer %q", local.PairRef, peer.Name)
	}
	if localVersion == "" || peer.Version == "" || localVersion != peer.Version {
		return fmt.Errorf("HCP versions must match: local %q, peer %q", localVersion, peer.Version)
	}
	for _, endpoint := range []struct {
		name  string
		value string
	}{
		{name: "peer NB endpoint", value: peer.NBEndpoint},
		{name: "peer SB endpoint", value: peer.SBEndpoint},
	} {
		if err := validateEndpoint(endpoint.name, endpoint.value); err != nil {
			return err
		}
	}
	return nil
}

func validateEndpoint(name, endpoint string) error {
	if _, err := ParseEndpoint(endpoint); err != nil {
		return fmt.Errorf("HCP %s %w", name, err)
	}
	return nil
}

// ParseEndpoint validates and splits an OVN endpoint without resolving DNS or
// opening a connection. It is shared by chart mapping and peer providers so
// the external address cannot be rendered with a duplicated protocol/port.
func ParseEndpoint(endpoint string) (Endpoint, error) {
	if endpoint != strings.TrimSpace(endpoint) || endpoint == "" {
		return Endpoint{}, fmt.Errorf("must be a non-empty OVN endpoint")
	}
	protocol, address, ok := strings.Cut(endpoint, ":")
	if !ok || (protocol != "tcp" && protocol != "ssl") {
		return Endpoint{}, fmt.Errorf("must use tcp or ssl protocol")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return Endpoint{}, fmt.Errorf("must use %s:<host>:<port>: %w", protocol, err)
	}
	if host == "" {
		return Endpoint{}, fmt.Errorf("host is required")
	}
	parsedPort, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsedPort == 0 {
		return Endpoint{}, fmt.Errorf("port must be between 1 and 65535")
	}
	if _, err := netip.ParseAddr(host); err != nil {
		if messages := kvalidation.IsDNS1123Subdomain(host); len(messages) != 0 {
			return Endpoint{}, fmt.Errorf("host %q is invalid: %s", host, messages[0])
		}
	}
	return Endpoint{Protocol: protocol, Host: host, Port: int32(parsedPort)}, nil
}
