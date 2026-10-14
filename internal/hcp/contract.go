// Package hcp validates the two-sided contract for hosted control-plane
// installations. It never transports or copies Secret data.
package hcp

import (
	"fmt"
	"net"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

type Peer struct {
	Name       string
	Version    string
	Role       operatorv1alpha1.HCPRole
	NBEndpoint string
	SBEndpoint string
}

func ValidatePair(local operatorv1alpha1.HCPConfig, localVersion string, peer Peer) error {
	if !local.Enabled {
		return nil
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
	for name, endpoint := range map[string]string{"local NB": local.NBEndpoint, "local SB": local.SBEndpoint, "peer NB": peer.NBEndpoint, "peer SB": peer.SBEndpoint} {
		if strings.TrimSpace(endpoint) == "" {
			return fmt.Errorf("%s endpoint is required", name)
		}
		if host, _, err := net.SplitHostPort(endpoint); err == nil && strings.TrimSpace(host) == "" {
			return fmt.Errorf("%s endpoint has an empty host", name)
		}
	}
	return nil
}
