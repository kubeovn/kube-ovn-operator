package hcp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	secretCAKey       = "ca.crt"
	secretCertKey     = "tls.crt"
	secretKeyKey      = "tls.key"
	nativeSecretCAKey = "cacert"
	nativeSecretCert  = "cert"
	nativeSecretKey   = "key"
)

var (
	ErrSecretIdentityChanged = errors.New("HCP TLS Secret identity changed")
	ErrTLSSecretInvalid      = errors.New("HCP TLS Secret is invalid")
)

// LoadTLSConfig reads standard Kubernetes TLS Secret keys into a client-side
// TLS configuration. It verifies the Secret identity before reading any
// credential bytes and returns only an in-memory tls.Config to the caller.
// Secret contents are never included in a PeerRequest, PeerReport, status or
// rendered object.
func LoadTLSConfig(ctx context.Context, reader client.Reader, request PeerRequest) (*tls.Config, error) {
	if reader == nil || request.TLSSecret.Namespace == "" || request.TLSSecret.Name == "" || request.TLSSecret.UID == "" {
		return nil, fmt.Errorf("load HCP TLS config: Secret reader and complete Secret identity are required")
	}
	secret := new(corev1.Secret)
	key := client.ObjectKey{Namespace: request.TLSSecret.Namespace, Name: request.TLSSecret.Name}
	if err := reader.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("load HCP TLS config from %s/%s: %w", key.Namespace, key.Name, err)
	}
	if secret.Namespace != request.TLSSecret.Namespace || secret.Name != request.TLSSecret.Name || secret.UID != request.TLSSecret.UID || request.TLSSecret.ResourceVersion != "" && secret.ResourceVersion != request.TLSSecret.ResourceVersion {
		return nil, fmt.Errorf("%w for %s/%s", ErrSecretIdentityChanged, key.Namespace, key.Name)
	}
	caData := firstSecretData(secret.Data, nativeSecretCAKey, secretCAKey)
	certData := firstSecretData(secret.Data, nativeSecretCert, secretCertKey)
	keyData := firstSecretData(secret.Data, nativeSecretKey, secretKeyKey)
	if len(caData) == 0 || len(certData) == 0 || len(keyData) == 0 {
		return nil, fmt.Errorf("%w: %s/%s must contain native Kube-OVN keys %q, %q and %q or standard TLS keys %q, %q and %q", ErrTLSSecretInvalid, key.Namespace, key.Name, nativeSecretCAKey, nativeSecretCert, nativeSecretKey, secretCAKey, secretCertKey, secretKeyKey)
	}
	roots := x509.NewCertPool()
	if !appendPEMCertificates(roots, caData) {
		return nil, fmt.Errorf("%w: %s/%s contains no CA certificate in %q", ErrTLSSecretInvalid, key.Namespace, key.Name, secretCAKey)
	}
	certificate, err := tls.X509KeyPair(certData, keyData)
	if err != nil {
		return nil, fmt.Errorf("%w: parse client certificate in %s/%s: %v", ErrTLSSecretInvalid, key.Namespace, key.Name, err)
	}
	if len(certificate.Certificate) == 0 {
		return nil, fmt.Errorf("%w: %s/%s client certificate chain is empty", ErrTLSSecretInvalid, key.Namespace, key.Name)
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("%w: parse client certificate in %s/%s: %v", ErrTLSSecretInvalid, key.Namespace, key.Name, err)
	}
	now := time.Now()
	if now.Before(leaf.NotBefore) || !now.Before(leaf.NotAfter) {
		return nil, fmt.Errorf("%w: client certificate in %s/%s is outside its validity period", ErrTLSSecretInvalid, key.Namespace, key.Name)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: []tls.Certificate{certificate},
	}, nil
}

func firstSecretData(data map[string][]byte, keys ...string) []byte {
	for _, key := range keys {
		if value := data[key]; len(value) != 0 {
			return value
		}
	}
	return nil
}

func appendPEMCertificates(pool *x509.CertPool, data []byte) bool {
	added := false
	for rest := data; len(rest) > 0; {
		block, next := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = next
		if block.Type != "CERTIFICATE" {
			continue
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !certificate.IsCA {
			continue
		}
		if pool.AppendCertsFromPEM(pem.EncodeToMemory(block)) {
			added = true
		}
	}
	return added
}
