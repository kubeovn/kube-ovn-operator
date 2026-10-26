package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json/v2"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestLeaseHeldRequiresCurrentRenewalAndDuration(t *testing.T) {
	now := time.Unix(100, 0)
	holder := "operator"
	duration := int32(30)
	renewed := metav1.NewMicroTime(time.Unix(90, 0))
	record := &coordinationv1.Lease{Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renewed}}
	if !leaseHeld(record, now) {
		t.Fatal("renewed Lease was considered inactive")
	}
	if leaseHeld(record, now.Add(21*time.Second)) {
		t.Fatal("expired Lease remained active")
	}
	record.Spec.RenewTime = nil
	if leaseHeld(record, now) {
		t.Fatal("Lease without a renewal time was considered active")
	}
}

func TestValidateFlagsRequiresMutualTLS(t *testing.T) {
	base := []string{"namespace", "lease", "installation", "server.crt", "server.key"}
	if err := validateFlags(base[0], base[1], base[2], base[3], base[4], "", time.Second, time.Second); err == nil {
		t.Fatal("missing client CA was accepted")
	}
	if err := validateFlags(base[0], base[1], base[2], base[3], base[4], "client-ca.pem", time.Second, time.Second); err != nil {
		t.Fatalf("valid mTLS flags rejected: %v", err)
	}
}

func TestInstallationUIDCanBeDerivedFromLeaseOwner(t *testing.T) {
	record := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{
		{APIVersion: "operator.kubeovn.io/v1alpha1", Kind: "KubeOVNInstallation", UID: types.UID("installation-uid")},
	}}}
	if got := installationUIDFromOwner(record); got != "installation-uid" {
		t.Fatalf("derived installation UID = %q", got)
	}
	if got := installationUIDFromOwner(&coordinationv1.Lease{}); got != "" {
		t.Fatalf("unrelated Lease returned installation UID %q", got)
	}
}

func TestServerTLSConfigRequiresAndVerifiesClientCertificate(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	path := t.TempDir() + "/client-ca.pem"
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := serverTLSConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS13 || config.ClientAuth != tls.RequireAndVerifyClientCert || config.ClientCAs == nil {
		t.Fatalf("unexpected mTLS config: %#v", config)
	}
}

func TestFenceAgentMTLSHandshakeRequiresClientCertificate(t *testing.T) {
	caCert, caKey := testCertificateAuthority(t)
	serverCertificate := testSignedCertificate(t, caCert, caKey, "fence.test", x509.ExtKeyUsageServerAuth)
	clientCertificate := testSignedCertificate(t, caCert, caKey, "operator", x509.ExtKeyUsageClientAuth)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw})
	caPath := t.TempDir() + "/client-ca.pem"
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	serverConfig, err := serverTLSConfig(caPath)
	if err != nil {
		t.Fatal(err)
	}
	serverConfig.Certificates = []tls.Certificate{serverCertificate}
	gate := new(fencing.Gate)
	server := httptest.NewUnstartedServer(&fencing.Handler{
		Gate: gate,
		Execute: func(context.Context, []string) ([]byte, error) {
			return []byte("accepted"), nil
		},
	})
	server.TLS = serverConfig
	server.StartTLS()
	defer server.Close()

	roots := x509.NewCertPool()
	roots.AddCert(caCert)
	clientTransport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{clientCertificate}, ServerName: "fence.test"}}
	client := &http.Client{Transport: clientTransport}
	defer clientTransport.CloseIdleConnections()
	response, err := client.Get(server.URL + fencing.HealthPath)
	if err != nil {
		t.Fatalf("mTLS client could not reach health endpoint: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mTLS health status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	fenceToken, err := fencing.TokenFromLease(types.UID("installation"), types.UID("lease"), "operator", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.Arm(fenceToken); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(fencing.Request{Command: []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", "cluster/leave", "OVN_Northbound"}, Token: &fenceToken})
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Post(server.URL+fencing.ExecutePath, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("mTLS client could not execute a fenced command: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("mTLS fenced command status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	noCertificateTransport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "fence.test"}}
	noCertificateClient := &http.Client{Transport: noCertificateTransport}
	defer noCertificateTransport.CloseIdleConnections()
	if _, err := noCertificateClient.Get(server.URL + fencing.HealthPath); err == nil {
		t.Fatal("fence-agent accepted a TLS client without a certificate")
	}
}

func testCertificateAuthority(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: testSerialNumber(t), Subject: pkix.Name{CommonName: "fence-test-ca"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key
}

func testSignedCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, commonName string, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: testSerialNumber(t), Subject: pkix.Name{CommonName: commonName}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, DNSNames: []string{"fence.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func testSerialNumber(t *testing.T) *big.Int {
	t.Helper()
	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

func TestObserveLeaseStreamRearmsAndClearsOnDelete(t *testing.T) {
	holder := "operator"
	duration := int32(30)
	transitions := int32(1)
	now := metav1.NewMicroTime(time.Now().Add(-time.Second))
	record := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("lease-1"), ResourceVersion: "1"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &duration,
			RenewTime:            &now,
			LeaseTransitions:     &transitions,
		},
	}
	clientset := fake.NewSimpleClientset(record)
	stream := watch.NewRaceFreeFake()
	clientset.PrependWatchReactor("leases", func(_ clienttesting.Action) (bool, watch.Interface, error) {
		return true, stream, nil
	})
	gate := new(fencing.Gate)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- observeLeaseStream(ctx, clientset.CoordinationV1().Leases("default"), "central", types.UID("installation"), gate)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if token, armed := gate.Current(); armed {
			if token.LeaseUID != "lease-1" || token.Epoch != 1 {
				t.Fatalf("unexpected initial fence token: %+v", token)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Lease observer did not arm the initial token")
		}
		time.Sleep(time.Millisecond)
	}
	updated := record.DeepCopy()
	updated.ResourceVersion = "2"
	updated.Spec.LeaseTransitions = new(int32)
	*updated.Spec.LeaseTransitions = 2
	stream.Modify(updated)
	deadline = time.Now().Add(time.Second)
	for {
		if token, armed := gate.Current(); armed && token.Epoch == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Lease observer did not rearm the updated token")
		}
		time.Sleep(time.Millisecond)
	}
	stream.Delete(updated)
	deadline = time.Now().Add(time.Second)
	for {
		if _, armed := gate.Current(); !armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Lease deletion did not clear the fence")
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-done; err == nil {
		t.Fatal("Lease deletion did not stop the watch stream")
	}
}

func TestObserveLeaseStreamClearsWhenRenewalExpiresWithoutWatchEvent(t *testing.T) {
	holder := "operator"
	duration := int32(1)
	transitions := int32(1)
	renewed := metav1.NewMicroTime(time.Now())
	record := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("lease-expiry"), ResourceVersion: "1"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renewed, LeaseTransitions: &transitions},
	}
	clientset := fake.NewSimpleClientset(record)
	stream := watch.NewRaceFreeFake()
	clientset.PrependWatchReactor("leases", func(_ clienttesting.Action) (bool, watch.Interface, error) {
		return true, stream, nil
	})
	gate := new(fencing.Gate)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- observeLeaseStream(ctx, clientset.CoordinationV1().Leases("default"), "central", types.UID("installation"), gate)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, armed := gate.Current(); !armed {
			if time.Now().After(deadline) {
				t.Fatal("Lease observer never armed the initial token")
			}
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	for {
		if _, armed := gate.Current(); !armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expired Lease remained armed without a watch event")
		}
		time.Sleep(time.Millisecond)
	}
	if err := <-done; err == nil {
		t.Fatal("expired Lease did not terminate the watch cycle")
	}
}

func TestLeaseTakeoverRejectsOldHTTPMutationAndRestoreTokens(t *testing.T) {
	holder := "operator"
	duration := int32(30)
	transitions := int32(1)
	renewed := metav1.NewMicroTime(time.Now().Add(-time.Second))
	record := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "central", Namespace: "default", UID: types.UID("lease-1"), ResourceVersion: "1"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &duration,
			RenewTime:            &renewed,
			LeaseTransitions:     &transitions,
		},
	}
	clientset := fake.NewSimpleClientset(record)
	stream := watch.NewRaceFreeFake()
	clientset.PrependWatchReactor("leases", func(_ clienttesting.Action) (bool, watch.Interface, error) {
		return true, stream, nil
	})
	gate := new(fencing.Gate)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- observeLeaseStream(ctx, clientset.CoordinationV1().Leases("default"), "central", types.UID("installation"), gate)
	}()

	waitForFenceEpoch(t, gate, 1)
	server := httptest.NewServer(&fencing.Handler{
		Gate: gate,
		Execute: func(context.Context, []string) ([]byte, error) {
			return []byte("accepted"), nil
		},
		Restore: func(context.Context, string, io.Reader) error { return nil },
	})
	defer server.Close()

	oldToken, err := fencing.TokenFromLease(types.UID("installation"), record.UID, holder, 1)
	if err != nil {
		t.Fatal(err)
	}
	if status := sendFenceMutation(t, server.URL, oldToken); status != http.StatusOK {
		t.Fatalf("initial token status = %d, want 200", status)
	}

	updated := record.DeepCopy()
	updated.ResourceVersion = "2"
	updated.Spec.LeaseTransitions = new(int32)
	*updated.Spec.LeaseTransitions = 2
	stream.Modify(updated)
	waitForFenceEpoch(t, gate, 2)

	if status := sendFenceMutation(t, server.URL, oldToken); status != http.StatusConflict {
		t.Fatalf("stale token status = %d, want 409", status)
	}
	newToken, err := fencing.TokenFromLease(types.UID("installation"), record.UID, holder, 2)
	if err != nil {
		t.Fatal(err)
	}
	if status := sendFenceMutation(t, server.URL, newToken); status != http.StatusOK {
		t.Fatalf("current token status = %d, want 200", status)
	}
	if status := sendFenceRestore(t, server.URL, oldToken); status != http.StatusConflict {
		t.Fatalf("stale restore token status = %d, want 409", status)
	}
	if status := sendFenceRestore(t, server.URL, newToken); status != http.StatusOK {
		t.Fatalf("current restore token status = %d, want 200", status)
	}

	cancel()
	if err := <-watchDone; err == nil {
		t.Fatal("Lease watcher stopped without returning its cancellation error")
	}
}

func waitForFenceEpoch(t *testing.T, gate *fencing.Gate, epoch int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		token, armed := gate.Current()
		if armed && token.Epoch == epoch {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("fence epoch did not become %d: token=%+v armed=%t", epoch, token, armed)
		}
		time.Sleep(time.Millisecond)
	}
}

func sendFenceMutation(t *testing.T, endpoint string, token lease.FenceToken) int {
	t.Helper()
	command := []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", "cluster/leave", "OVN_Northbound"}
	body, err := json.Marshal(fencing.Request{Command: command, Token: &token})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+fencing.ExecutePath, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func sendFenceRestore(t *testing.T, endpoint string, token lease.FenceToken) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+fencing.RestorePath, strings.NewReader("verified-backup"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	request.Header.Set("X-Kube-OVN-Installation-UID", string(token.InstallationUID))
	request.Header.Set("X-Kube-OVN-Lease-UID", string(token.LeaseUID))
	request.Header.Set("X-Kube-OVN-Fence-Scope", string(token.Scope))
	request.Header.Set("X-Kube-OVN-Fence-Holder", token.Holder)
	request.Header.Set("X-Kube-OVN-Fence-Epoch", strconv.FormatInt(int64(token.Epoch), 10))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}
