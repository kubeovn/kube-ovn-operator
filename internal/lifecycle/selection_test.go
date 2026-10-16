package lifecycle

import (
	"strings"
	"testing"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSelectRemovalRequiresOnePinnedIndependentMember(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	request, err := SelectRemoval(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.RemoveEndpoint != "member-0-0" || request.Retirement == nil {
		t.Fatalf("unexpected removal request: %+v", request)
	}
	if len(request.Databases) != 2 || len(request.Databases[0].Peers) != 4 || request.Databases[0].Peers[0].Name != "chart-1-0" {
		t.Fatalf("request did not retain the complete inventory: %+v", request.Databases)
	}
	if request.Retirement.SpecDigest == "" || request.OperationID == "" {
		t.Fatalf("request is missing immutable identity: %+v", request)
	}
}

func TestSelectRemovalRejectsUnsafeOrAmbiguousChanges(t *testing.T) {
	tests := []struct {
		name       string
		stateful   int
		desired    int32
		wantPhrase string
	}{
		{name: "ordinary chart deployment", stateful: 0, desired: 3, wantPhrase: "no independently managed"},
		{name: "ambiguous independent members", stateful: 2, desired: 3, wantPhrase: "ambiguous"},
		{name: "multiple member decrease", stateful: 1, desired: 2, wantPhrase: "one member per transition"},
		{name: "addition without prepared input", stateful: 1, desired: 5, wantPhrase: "explicit prepared member"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			installation, topology, objects := selectionFixture(t, test.stateful)
			installation.Spec.Central.Replicas = test.desired
			_, err := SelectRemoval(t.Context(), selectionClient(t, objects), installation, topology)
			if err == nil || !strings.Contains(err.Error(), test.wantPhrase) {
				t.Fatalf("expected %q in error, got %v", test.wantPhrase, err)
			}
		})
	}
}

func TestSelectRemovalReturnsNoRequestWhenTopologyAlreadyMatches(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = int32(len(topology.Members))
	request, err := SelectRemoval(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil || request != nil {
		t.Fatalf("matching topology selected an operation: request=%+v err=%v", request, err)
	}
}

func selectionClient(t *testing.T, objects []client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func selectionFixture(t *testing.T, stateful int) (*api.KubeOVNInstallation, health.CentralTopology, []client.Object) {
	t.Helper()
	installation := &api.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "installation", Namespace: "default", UID: types.UID("installation-uid"), Generation: 4}, Spec: api.KubeOVNInstallationSpec{Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3}}}
	objects := make([]client.Object, 0, stateful)
	topology := health.CentralTopology{Members: make([]health.CentralIdentity, 0, 4)}
	for index := range 4 {
		serverID := "0000000" + string(rune('1'+index)) + "-1111-2222-3333-444444444444"
		workloadKind, workloadName := "Deployment", "chart-"+string(rune('0'+index))
		if index < stateful {
			workloadKind, workloadName = "StatefulSet", "member-"+string(rune('0'+index))
			set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: workloadName, Namespace: installation.Namespace, UID: types.UID("workload-" + string(rune('0'+index))), Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: new(true)}}}, Spec: appsv1.StatefulSetSpec{Replicas: new(int32(1)), UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ovn-central", Image: "example/central@sha256:" + strings.Repeat("a", 64)}}}}}}
			objects = append(objects, set)
		}
		member := health.CentralIdentity{Node: health.ObjectIdentity{Name: "node-" + string(rune('0'+index)), UID: types.UID("node-uid-" + string(rune('0'+index)))}, Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: workloadName, UID: types.UID("workload-" + string(rune('0'+index)))}, Kind: workloadKind}, Storage: []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: "/var/lib/ovn/" + string(rune('0'+index))}}, Pod: health.ObjectIdentity{Name: workloadName + "-0", UID: types.UID("pod-" + string(rune('0'+index)))}, ContainerID: "containerd://container-" + string(rune('0'+index))}
		for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
			member.Databases[database] = health.DatabaseIdentity{Database: name, ClusterID: "1111111" + string(rune('0'+database)) + "-2222-3333-4444-555555555555", ServerID: serverID, Address: "tcp:192.0.2." + string(rune('1'+index)) + ":" + string(rune('6'+database)) + "643", ClientAddress: "tcp:192.0.2." + string(rune('1'+index)) + ":" + string(rune('6'+database)) + "641"}
		}
		topology.Members = append(topology.Members, member)
	}
	return installation, topology, objects
}
