package health

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestRemoteRestoreFencePreflightsEveryRetainedCentralPod(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen = append(seen, request.URL.Path)
		if request.URL.Path == "/fence/default/central-1/ovn-central"+fencing.ReadyPath {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"code":"fence_not_armed","message":"stale Lease"}`))
			return
		}
		_, _ = writer.Write([]byte("ok\n"))
	}))
	defer server.Close()
	remote, err := NewFenceHTTPClient(server.URL+"/fence/{namespace}/{pod}/{container}", nil)
	if err != nil {
		t.Fatal(err)
	}
	fence := &RemoteRestoreFence{Remote: remote}
	installation := &operatorv1alpha1.KubeOVNInstallation{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"}}
	topology := CentralTopology{Members: []CentralIdentity{
		restoreFenceMember("central-0", "00000000-0000-0000-0000-000000000001"),
		restoreFenceMember("central-1", "00000000-0000-0000-0000-000000000002"),
	}}
	if err := fence.ReadyFor(t.Context(), installation, topology); err == nil || !strings.Contains(err.Error(), "central-1") {
		t.Fatalf("unarmed retained Pod was accepted: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("readiness probes = %v, want both retained Pods", seen)
	}
}

func TestRetainedFenceHostAcceptsTLSAndIPv6DatabaseAddresses(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    string
	}{
		{name: "ssl IPv4", address: "ssl:192.0.2.10:6643", want: "192.0.2.10"},
		{name: "ssl IPv6", address: "ssl:[2001:db8::10]:6643", want: "2001:db8::10"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			member := restoreFenceMember("central-0", "00000000-0000-0000-0000-000000000001")
			member.Databases[0].Address = testCase.address
			if got := retainedFenceHost(member); got != testCase.want {
				t.Fatalf("retainedFenceHost() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func restoreFenceMember(podName, serverID string) CentralIdentity {
	octet := int(podName[len(podName)-1]-'0') + 1
	host := "192.0.2." + strconv.Itoa(octet)
	return CentralIdentity{
		Node:        ObjectIdentity{Name: "node-" + podName, UID: types.UID("uid-node-" + podName)},
		Workload:    WorkloadIdentity{ObjectIdentity: ObjectIdentity{Name: "central", UID: types.UID("uid-workload")}, Kind: "Deployment"},
		Storage:     []StorageIdentity{{MountPath: "/etc/ovn", HostPath: "/var/lib/ovn/" + podName}},
		Pod:         ObjectIdentity{Name: podName, UID: types.UID("uid-" + podName)},
		ContainerID: "containerd://" + podName,
		Databases: [2]DatabaseIdentity{
			{Database: "OVN_Northbound", ClusterID: "11111111-2222-3333-4444-555555555555", ServerID: serverID, Address: "tcp:" + host + ":6643", ClientAddress: "tcp:" + host + ":6641"},
			{Database: "OVN_Southbound", ClusterID: "66666666-7777-8888-9999-aaaaaaaaaaaa", ServerID: serverID, Address: "tcp:" + host + ":6644", ClientAddress: "tcp:" + host + ":6642"},
		},
	}
}
