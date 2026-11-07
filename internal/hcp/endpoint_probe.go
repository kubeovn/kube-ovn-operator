package hcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

const endpointProbeTimeout = 5 * time.Second

// ProbeEndpoints verifies that both configured HCP OVN transports are
// reachable. For tcp endpoints this proves only that a connection can be
// established. For ssl endpoints it also proves a TLS handshake using the
// provider's in-memory Secret-backed configuration. It does not claim peer
// role, version, schema or OVN database readiness; those require protocol
// specific evidence from the actual peer provider.
func ProbeEndpoints(ctx context.Context, config operatorv1alpha1.HCPConfig, tlsConfig *tls.Config) error {
	if !config.Enabled {
		return nil
	}
	var errs []error
	for _, item := range []struct {
		name     string
		endpoint string
	}{
		{name: "NB", endpoint: config.NBEndpoint},
		{name: "SB", endpoint: config.SBEndpoint},
	} {
		if err := ProbeEndpoint(ctx, item.endpoint, tlsConfig); err != nil {
			errs = append(errs, fmt.Errorf("probe HCP %s endpoint: %w", item.name, err))
		}
	}
	return errors.Join(errs...)
}

// ProbeEndpoint establishes one bounded transport connection. A TLS config
// is cloned before setting the endpoint-derived ServerName so callers can
// safely reuse a Secret-backed configuration for NB and SB.
func ProbeEndpoint(ctx context.Context, endpoint string, tlsConfig *tls.Config) error {
	parsed, err := ParseEndpoint(endpoint)
	if err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, endpointProbeTimeout)
	defer cancel()
	dialer := new(net.Dialer)
	connection, err := dialer.DialContext(probeCtx, "tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(int(parsed.Port))))
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	if parsed.Protocol != "ssl" {
		return nil
	}
	if tlsConfig == nil {
		return errors.New("ssl endpoint requires a TLS configuration")
	}
	clientConfig := tlsConfig.Clone()
	if clientConfig.ServerName == "" {
		clientConfig.ServerName = parsed.Host
	}
	secure := tls.Client(connection, clientConfig)
	if err := secure.HandshakeContext(probeCtx); err != nil {
		return fmt.Errorf("TLS handshake: %w", err)
	}
	return nil
}
