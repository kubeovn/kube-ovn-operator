package hcp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLoadTLSConfigBindsSecretGeneration(t *testing.T) {
	secretData := tlsSecretData(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "network", UID: types.UID("secret-uid"), ResourceVersion: "7"}, Data: secretData}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	request := PeerRequest{Config: operatorv1alpha1.HCPConfig{Enabled: true}, TLSSecret: SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: secret.UID, ResourceVersion: secret.ResourceVersion}}
	config, err := LoadTLSConfig(t.Context(), reader, request)
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS12 || len(config.Certificates) != 1 || config.RootCAs == nil {
		t.Fatalf("unexpected TLS config: min=%d certificates=%d roots=%v", config.MinVersion, len(config.Certificates), config.RootCAs != nil)
	}
}

func TestLoadTLSConfigAcceptsNativeKubeOVNSecretKeys(t *testing.T) {
	data := tlsSecretData(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour))
	native := map[string][]byte{nativeSecretCAKey: data[secretCAKey], nativeSecretCert: data[secretCertKey], nativeSecretKey: data[secretKeyKey]}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "network", UID: types.UID("secret-uid"), ResourceVersion: "7"}, Data: native}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	request := PeerRequest{TLSSecret: SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: secret.UID, ResourceVersion: secret.ResourceVersion}}
	if _, err := LoadTLSConfig(t.Context(), reader, request); err != nil {
		t.Fatalf("native Kube-OVN Secret keys were rejected: %v", err)
	}
}

func TestLoadTLSConfigRejectsSecretReplacementAndRotation(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "network", UID: types.UID("new-secret"), ResourceVersion: "8"}, Data: tlsSecretData(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour))}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	request := PeerRequest{TLSSecret: SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: types.UID("old-secret"), ResourceVersion: "7"}}
	if _, err := LoadTLSConfig(t.Context(), reader, request); !errors.Is(err, ErrSecretIdentityChanged) {
		t.Fatalf("replaced Secret was accepted: %v", err)
	}
	request.TLSSecret.UID = secret.UID
	if _, err := LoadTLSConfig(t.Context(), reader, request); !errors.Is(err, ErrSecretIdentityChanged) {
		t.Fatalf("rotated Secret was accepted: %v", err)
	}
}

func TestLoadTLSConfigRejectsInvalidCertificateLifetime(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "network", UID: types.UID("secret-uid"), ResourceVersion: "7"}, Data: tlsSecretData(t, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Minute))}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	request := PeerRequest{TLSSecret: SecretIdentity{Namespace: "network", Name: "ovn-tls", UID: secret.UID, ResourceVersion: secret.ResourceVersion}}
	if _, err := LoadTLSConfig(t.Context(), reader, request); !errors.Is(err, ErrTLSSecretInvalid) {
		t.Fatalf("expired client certificate was accepted: %v", err)
	}
}

func tlsSecretData(t *testing.T, notBefore, notAfter time.Time) map[string][]byte {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: notBefore, NotAfter: notAfter, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "test-client"}, NotBefore: notBefore, NotAfter: notAfter, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{
		secretCAKey:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		secretCertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		secretKeyKey:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
}
