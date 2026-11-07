package controller

import (
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/planner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestHCPProviderIsRequiredBeforeWorkloadApply(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data-plane", Namespace: "default", UID: types.UID("hcp-installation"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
			HCP: operatorv1alpha1.HCPConfig{
				Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
				NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "tcp:sb.example:6642",
				TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme, Renderer: objectRenderer{}, Workload: new(recordingWorkload)}
	if _, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name}}); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Upgrade.Step != "HCPPeer" || !hasCondition(got.Status.Conditions, "HCPReady", metav1.ConditionFalse, "PeerProviderUnavailable") {
		t.Fatalf("missing HCP provider was not a safety gate: %+v", got.Status)
	}
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: installation.Namespace, Name: planConfigMapPrefix + installation.Name}, new(corev1.ConfigMap)); err == nil {
		t.Fatal("HCP safety gate unexpectedly wrote a plan")
	}
}

func TestHCPPeerRequestPinsTLSSecretGeneration(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data-plane", Namespace: "default", UID: types.UID("hcp-installation"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
			HCP: operatorv1alpha1.HCPConfig{
				Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
				NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "tcp:sb.example:6642",
				TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
			},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "default", UID: types.UID("secret-v1"), ResourceVersion: "1"}, Data: map[string][]byte{"tls.crt": []byte("redacted")}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation, secret).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	first, err := reconciler.hcpPeerRequest(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if first.TLSSecret.UID != secret.UID || first.TLSSecret.ResourceVersion != secret.ResourceVersion {
		t.Fatalf("request did not pin Secret identity: %+v", first.TLSSecret)
	}
	secret = secret.DeepCopy()
	if err := kube.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	secret.UID, secret.ResourceVersion = types.UID("secret-v2"), ""
	if err := kube.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	second, err := reconciler.hcpPeerRequest(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if second.TLSSecret.UID == first.TLSSecret.UID {
		t.Fatalf("Secret replacement was not visible in the next request: first=%+v second=%+v", first.TLSSecret, second.TLSSecret)
	}
}

func TestHCPPeerRequestUsesUncachedIdentityReader(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data-plane", Namespace: "default", UID: types.UID("hcp-installation"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "tcp:sb.example:6642", TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
		}},
	}
	stale := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "default", UID: types.UID("secret-stale"), ResourceVersion: "3"}}
	fresh := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "ovn-tls", Namespace: "default", UID: types.UID("secret-fresh"), ResourceVersion: "4"}}
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stale).Build()
	uncached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fresh).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: cached, APIReader: uncached, Scheme: scheme}
	request, err := reconciler.hcpPeerRequest(t.Context(), installation)
	if err != nil {
		t.Fatal(err)
	}
	if request.TLSSecret.UID != fresh.UID || request.TLSSecret.ResourceVersion != fresh.ResourceVersion {
		t.Fatalf("HCP request used a cached Secret generation: %+v", request.TLSSecret)
	}
}

func TestHCPReportParticipatesInReadyGateAndStatus(t *testing.T) {
	scheme := controllerScheme(t)
	installation := &operatorv1alpha1.KubeOVNInstallation{
		ObjectMeta: metav1.ObjectMeta{Name: "data-plane", Namespace: "default", UID: types.UID("hcp-installation"), Generation: 1},
		Spec: operatorv1alpha1.KubeOVNInstallationSpec{
			Version: "v1.17.0", Mode: operatorv1alpha1.InstallationModeDataPlaneOnly,
			Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
			HCP: operatorv1alpha1.HCPConfig{
				Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
				NBEndpoint: "tcp:nb.example:6641", SBEndpoint: "tcp:sb.example:6642",
				TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "ovn-tls"},
			},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(installation).WithObjects(installation).Build()
	reconciler := &KubeOVNInstallationReconciler{Client: kube, Scheme: scheme}
	peerReport := &hcp.PeerReport{Peer: hcp.Peer{Name: "management", Version: "v1.17.0", Role: operatorv1alpha1.HCPRoleManagement, NBEndpoint: "tcp:nb.remote:6641", SBEndpoint: "tcp:sb.remote:6642"}, Paired: true, EndpointReady: true, Ready: true}
	workload := &apply.Observation{Ready: true, ReadyCentralReplicas: 3}
	ovn := &health.OVNReport{Ready: true, Reason: "NotRequired", Message: "external OVN central"}
	runtime := &health.RuntimeReport{Northd: metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue, Reason: "NotRequired"}, DataPlane: metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionTrue, Reason: "DataPlaneConnected"}, Nodes: []operatorv1alpha1.DataPlaneNodeStatus{{Node: "node", NodeUID: "node-uid"}}}
	network := &health.NetworkReport{Condition: metav1.Condition{Type: "NetworkReady", Status: metav1.ConditionTrue, Reason: "NetworkProbesPassed"}, Evidence: &operatorv1alpha1.NetworkStatus{ObservedNodes: 1, AddressFamilies: []string{"IPv4"}, SuccessfulProbes: 3}}
	if err := reconciler.updateStatus(t.Context(), installation, planner.Plan{Phase: operatorv1alpha1.PhaseInstalling, Step: "Verify"}, time.Unix(10, 0), workload, ovn, runtime, network, nil, peerReport); err != nil {
		t.Fatal(err)
	}
	got := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(t.Context(), types.NamespacedName{Namespace: installation.Namespace, Name: installation.Name}, got); err != nil {
		t.Fatal(err)
	}
	if !hasCondition(got.Status.Conditions, "HCPReady", metav1.ConditionTrue, "PeerReady") || !hasCondition(got.Status.Conditions, "Ready", metav1.ConditionTrue, "VerificationComplete") || got.Status.HCP.PeerObservedVersion != "v1.17.0" || !got.Status.HCP.Paired || !got.Status.HCP.EndpointReady {
		t.Fatalf("HCP report did not gate or populate ready status: %+v", got.Status)
	}
}

func hasCondition(conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus, reason string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == status && condition.Reason == reason {
			return true
		}
	}
	return false
}
