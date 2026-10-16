package lifecycle

import (
	"strings"
	"testing"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
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

func TestSelectRemovalBuildsExplicitChartDeploymentScaleIn(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 3)
	installation.Spec.Central.Replicas = int32(len(topology.Members) - 1)
	installation.Spec.Central.DynamicMembership = true
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{{Name: "retire-chart", RetireEndpoint: "chart-3-0"}}
	objects = append(objects, chartDeploymentObjects(installation, "chart-3", "chart-3-0", "pod-3", "chart-rs", "chart-rs-uid")...)

	request, err := SelectRemoval(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Add != nil || request.RemoveEndpoint != "chart-3-0" || request.Retirement == nil || request.Retirement.Kind != deploymentRetirementKind {
		t.Fatalf("unexpected Chart Deployment scale-in request: %+v", request)
	}
	if _, _, err := canonical(*request); err != nil {
		t.Fatalf("Chart Deployment scale-in request is not canonical: %v", err)
	}
}

func TestSelectAdditionRequiresPinnedMemberObjectsAndBuildsBothDatabasePlan(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = 5
	installation.Spec.Central.DynamicMembership = true
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{{
		Name:                    "member-new",
		NodeName:                "node-new",
		ClaimName:               "claim-new",
		VolumeName:              "volume-new",
		Image:                   "example/central@sha256:" + strings.Repeat("a", 64),
		NorthboundAddress:       "tcp:192.0.2.99:6643",
		SouthboundAddress:       "tcp:192.0.2.99:6644",
		NorthboundClientAddress: "tcp:192.0.2.99:6641",
		SouthboundClientAddress: "tcp:192.0.2.99:6642",
	}}
	objects = append(objects,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-new", UID: types.UID("node-new-uid")}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.99"}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "volume-new"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "volume-new", UID: types.UID("volume-new-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "claim-new", Namespace: installation.Namespace, UID: types.UID("claim-new-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	)
	request, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Add == nil || request.Add.Workload.Preparation.MemberName != "member-new" || len(request.Databases) != 2 || len(request.Databases[0].Peers) != 4 || len(request.Databases[1].Peers) != 4 {
		t.Fatalf("unexpected addition request: %+v", request)
	}
	if request.Add.Workload.Node.UID != types.UID("node-new-uid") || request.Add.Workload.Claim.UID != types.UID("claim-new-uid") || request.Add.Workload.Volume.UID != types.UID("volume-new-uid") {
		t.Fatalf("request did not pin live object identities: %+v", request.Add.Workload)
	}
}

func TestSelectAdditionBuildsSameSizeReplacementForPinnedStatefulSet(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = int32(len(topology.Members))
	installation.Spec.Central.DynamicMembership = true
	member := memberRequest("member-replacement", "node-new", "claim-new", "volume-new", "192.0.2.99")
	member.ReplaceEndpoint = "member-0-0"
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{member}
	objects = append(objects, memberObjects(installation, "node-new", "claim-new", "volume-new", "192.0.2.99")...)

	request, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Add == nil || request.RemoveEndpoint != member.ReplaceEndpoint || request.Retirement == nil {
		t.Fatalf("unexpected same-size replacement request: %+v", request)
	}
	if request.Retirement.Workload.Name != "member-0" || request.Add.Workload.Preparation.MemberName != member.Name {
		t.Fatalf("replacement did not bind old and new workload identities: %+v", request)
	}
	if _, _, err := canonical(*request); err != nil {
		t.Fatalf("same-size replacement request is not canonical: %v", err)
	}
}

func TestSelectAdditionBuildsSameSizeReplacementForChartDeployment(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 3)
	installation.Spec.Central.Replicas = int32(len(topology.Members))
	installation.Spec.Central.DynamicMembership = true
	member := memberRequest("member-replacement", "node-new", "claim-new", "volume-new", "192.0.2.99")
	member.ReplaceEndpoint = "chart-3-0"
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{member}
	objects = append(objects, memberObjects(installation, "node-new", "claim-new", "volume-new", "192.0.2.99")...)
	objects = append(objects, chartDeploymentObjects(installation, "chart-3", "chart-3-0", "pod-3", "chart-rs", "chart-rs-uid")...)

	topology.Members[3].Pod = health.ObjectIdentity{Name: "chart-3-0", UID: types.UID("pod-3")}
	request, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Retirement == nil || request.Retirement.Kind != deploymentRetirementKind || request.Retirement.Endpoint.Name != "chart-3-0" || request.Retirement.ReplicaSet.Name != "chart-rs" {
		t.Fatalf("unexpected Chart Deployment replacement request: %+v", request)
	}
}

func TestSelectAdditionRejectsUnpinnedSlots(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = 5
	installation.Spec.Central.DynamicMembership = true
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{{Name: "first"}}
	if _, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology); err == nil || !strings.Contains(err.Error(), "requires node") {
		t.Fatalf("expected incomplete member request to be rejected, got %v", err)
	}
}

func TestSelectAdditionSkipsRetirementOnlySlots(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = 5
	installation.Spec.Central.DynamicMembership = true
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{
		{Name: "retire-chart", RetireEndpoint: "chart-0-0"},
		memberRequest("member-new", "node-new", "claim-new", "volume-new", "192.0.2.99"),
	}
	objects = append(objects, memberObjects(installation, "node-new", "claim-new", "volume-new", "192.0.2.99")...)

	request, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Add == nil || request.Add.Workload.Preparation.MemberName != "member-new" {
		t.Fatalf("retirement-only slot was selected as an addition: %+v", request)
	}
}

func TestSelectAdditionSkipsACompletedMemberRecord(t *testing.T) {
	installation, topology, objects := selectionFixture(t, 1)
	installation.Spec.Central.Replicas = 5
	installation.Spec.Central.DynamicMembership = true
	installation.Spec.Central.MemberRequests = []api.CentralMemberSpec{
		memberRequest("first", "node-first", "claim-first", "volume-first", "192.0.2.98"),
		memberRequest("second", "node-second", "claim-second", "volume-second", "192.0.2.99"),
	}
	// The first request already has the stable workload slot that the
	// provisioner would create. It must not be selected again after a restart.
	topology.Members[0].Workload.Name = workload.RecordName(string(installation.UID), "first")
	objects = append(objects, memberObjects(installation, "node-second", "claim-second", "volume-second", "192.0.2.99")...)
	request, err := SelectAddition(t.Context(), selectionClient(t, objects), installation, topology)
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Add == nil || request.Add.Workload.Preparation.MemberName != "second" {
		t.Fatalf("completed member record was selected again: %+v", request)
	}
}

func memberRequest(name, node, claim, volume, address string) api.CentralMemberSpec {
	return api.CentralMemberSpec{
		Name: name, NodeName: node, ClaimName: claim, VolumeName: volume,
		Image:             "example/central@sha256:" + strings.Repeat("a", 64),
		NorthboundAddress: "tcp:" + address + ":6643", SouthboundAddress: "tcp:" + address + ":6644",
		NorthboundClientAddress: "tcp:" + address + ":6641", SouthboundClientAddress: "tcp:" + address + ":6642",
	}
}

func memberObjects(installation *api.KubeOVNInstallation, nodeName, claimName, volumeName, address string) []client.Object {
	return []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, UID: types.UID(nodeName + "-uid")}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: installation.Namespace, UID: types.UID(claimName + "-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: volumeName}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}},
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: volumeName, UID: types.UID(volumeName + "-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: claimName, Namespace: installation.Namespace, UID: types.UID(claimName + "-uid")}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}},
	}
}

func chartDeploymentObjects(installation *api.KubeOVNInstallation, deploymentName, targetName string, targetUID, replicaSetName, replicaSetUID string) []client.Object {
	controller := true
	labels := map[string]string{"app": "ovn-central"}
	owner := metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: &controller}
	deploymentOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: deploymentName, UID: types.UID("workload-3"), Controller: &controller}
	image := "example/central@sha256:" + strings.Repeat("a", 64)
	return []client.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: deploymentName, Namespace: installation.Namespace, UID: types.UID("workload-3"), Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)}, OwnerReferences: []metav1.OwnerReference{owner}},
			Spec:       appsv1.DeploymentSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: image}}}}},
		},
		&appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: replicaSetName, Namespace: installation.Namespace, UID: types.UID(replicaSetUID), OwnerReferences: []metav1.OwnerReference{deploymentOwner}},
			Spec:       appsv1.ReplicaSetSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: image}}}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: targetName, Namespace: installation.Namespace, UID: types.UID(targetUID), Labels: labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSetName, UID: types.UID(replicaSetUID), Controller: &controller}}},
			Spec:       corev1.PodSpec{NodeName: "node-3", HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: image}}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: deploymentName + "-1", Namespace: installation.Namespace, UID: types.UID("pod-3-survivor"), Labels: labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSetName, UID: types.UID(replicaSetUID), Controller: &controller}}},
			Spec:       corev1.PodSpec{NodeName: "node-4", HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: image}}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		},
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
