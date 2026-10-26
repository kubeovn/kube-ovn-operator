package hcp

import (
	"context"
	"crypto/tls"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestKubernetesPeerProviderObservesReadyReciprocalPeer(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "local-tls", Namespace: "default", UID: types.UID("secret-uid"), ResourceVersion: "7"}}
	peer := readyPeerInstallation()
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, peer).Build()
	loadCalls, probeCalls := 0, 0
	provider := &KubernetesPeerProvider{
		Reader: reader,
		LoadTLS: func(_ context.Context, _ client.Reader, request PeerRequest) (*tls.Config, error) {
			loadCalls++
			if request.TLSSecret.UID != secret.UID {
				t.Fatalf("provider passed the wrong Secret identity: %+v", request.TLSSecret)
			}
			return &tls.Config{}, nil
		},
		Probe: func(_ context.Context, config operatorv1alpha1.HCPConfig, tlsConfig *tls.Config) error {
			probeCalls++
			if config.NBEndpoint != "tcp:nb.remote:6641" || tlsConfig == nil {
				t.Fatalf("provider passed unexpected probe input: config=%+v tls=%v", config, tlsConfig)
			}
			return nil
		},
	}
	report, err := provider.Observe(t.Context(), PeerRequest{
		Installation: types.NamespacedName{Namespace: "default", Name: "data"},
		Version:      "v1.17.0",
		Config: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "tcp:sb.remote:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "local-tls"},
		},
		TLSSecret: SecretIdentity{Namespace: secret.Namespace, Name: secret.Name, UID: secret.UID, ResourceVersion: secret.ResourceVersion},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Paired || !report.EndpointReady || !report.Ready || report.Reason != "PeerReady" {
		t.Fatalf("ready reciprocal peer was not accepted: %+v", report)
	}
	if report.Peer.Role != operatorv1alpha1.HCPRoleManagement || report.Peer.Version != "v1.17.0" || loadCalls != 1 || probeCalls != 1 {
		t.Fatalf("unexpected provider evidence: report=%+v load=%d probe=%d", report, loadCalls, probeCalls)
	}
}

func TestKubernetesPeerProviderRequiresReciprocalPair(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	peer := readyPeerInstallation()
	peer.Spec.HCP.PairRef = "other"
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(peer).Build()
	provider := &KubernetesPeerProvider{Reader: reader}
	report, err := provider.Observe(t.Context(), validPeerRequest())
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.Paired || report.Reason != "PeerPairRefMismatch" {
		t.Fatalf("non-reciprocal peer was accepted: %+v", report)
	}
}

func TestKubernetesPeerProviderWaitsForPeerWorkloadObservation(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	peer := readyPeerInstallation()
	peer.Status.ObservedVersion = ""
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(peer).Build()
	provider := &KubernetesPeerProvider{Reader: reader}
	report, err := provider.Observe(t.Context(), validPeerRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Paired || report.Ready || report.Reason != "PeerNotReady" {
		t.Fatalf("unobserved peer was accepted: %+v", report)
	}
}

func validPeerRequest() PeerRequest {
	return PeerRequest{
		Installation: types.NamespacedName{Namespace: "default", Name: "data"},
		Version:      "v1.17.0",
		Config: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "tcp:sb.remote:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "local-tls"},
		},
		TLSSecret: SecretIdentity{Namespace: "default", Name: "local-tls", UID: types.UID("secret-uid"), ResourceVersion: "7"},
	}
}

func readyPeerInstallation() *operatorv1alpha1.KubeOVNInstallation {
	return &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "management", Namespace: "hcp", UID: types.UID("peer-uid"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0",
			Mode:    operatorv1alpha1.InstallationModeControlPlaneOnly,
			HCP: operatorv1alpha1.HCPConfig{
				Enabled: true, Role: operatorv1alpha1.HCPRoleManagement, PairRef: "data", CentralNamespace: "default",
				NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "tcp:sb.remote:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "peer-tls"},
			},
		},
		Status: operatorv1alpha1.KubeOVNInstallationStatus{
			ObservedVersion: "v1.17.0",
			Conditions:      []metav1.Condition{{Type: "WorkloadsReady", Status: metav1.ConditionTrue}},
		},
	}
}
