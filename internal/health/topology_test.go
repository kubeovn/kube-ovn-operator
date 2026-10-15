package health

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func topologyStatus(pod, database string) string {
	index := int(pod[len(pod)-1] - '0')
	ids := []string{"aaaa0000-1111-2222-3333-444444444444", "bbbb0000-1111-2222-3333-444444444444", "cccc0000-1111-2222-3333-444444444444"}
	cid, port := "dddd0000-1111-2222-3333-444444444444", 6643
	if database == "OVN_Southbound" {
		cid, port = "eeee0000-1111-2222-3333-444444444444", 6644
	}
	role, leader := "follower", "member-0"
	if index == 0 {
		role, leader = "leader", "self"
	}
	result := fmt.Sprintf("Name: %s\nCluster ID: %s (%s)\nServer ID: %s (%s)\nAddress: tcp:192.0.2.%d:%d\nStatus: cluster member\nRole: %s\nTerm: 7\nLeader: %s\nLog: [1, 21]\nEntries not yet committed: 0\nEntries not yet applied: 0\nConnections:", database, cid[:4], cid, ids[index][:4], ids[index], index+1, port, role, leader)
	for i := range 3 {
		if i != index {
			result += fmt.Sprintf(" ->member-%d", i)
		}
	}
	result += "\nServers:\n"
	for i, id := range ids {
		result += fmt.Sprintf("member-%d (%s at tcp:192.0.2.%d:%d)", i, id[:4], i+1, port)
		if i == index {
			result += " (self)"
		}
		result += "\n"
	}
	return result
}

func TestCentralTopologyRequiresCompleteJointAndKubernetesIdentity(t *testing.T) {
	for _, scenario := range []string{"healthy", "dynamic listener", "missing pod", "unhealthy peer", "extra raft member", "cluster drift", "node replaced", "pod replaced", "listener changed", "ephemeral storage", "wrong node address", "no listener"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			for i := range 3 {
				pod := objects[i+2].(*corev1.Pod)
				pod.Spec.NodeName, pod.Spec.HostNetwork = fmt.Sprintf("node-%d", i), true
				pod.Spec.Containers = []corev1.Container{{Name: "ovn-central", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/etc/ovn"}}}}
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/etc/origin/ovn"}}}}
				if scenario == "ephemeral storage" {
					pod.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{EmptyDir: new(corev1.EmptyDirVolumeSource)}
				}
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://" + pod.Name, Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				node := &corev1.Node{Name: pod.Spec.NodeName, UID: types.UID(pod.Spec.NodeName), Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: fmt.Sprintf("192.0.2.%d", i+1)}}}}
				if scenario == "wrong node address" {
					node.Status.Addresses[0].Address = "192.0.2.99"
				}
				if err := kube.Create(t.Context(), node); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "missing pod" {
				if err := kube.Delete(t.Context(), objects[2]); err != nil {
					t.Fatal(err)
				}
			}
			remoteReads := map[string]int{}
			mutated := false
			runner := runnerFunc(func(ctx context.Context, _, pod, _ string, command []string) ([]byte, error) {
				if !mutated && (scenario == "node replaced" || scenario == "pod replaced") {
					mutated = true
					var target client.Object
					if scenario == "node replaced" {
						target = &corev1.Node{Name: "node-0"}
					} else {
						target = &corev1.Pod{Name: pod, Namespace: installation.Namespace}
					}
					if err := kube.Get(ctx, client.ObjectKeyFromObject(target), target); err != nil {
						t.Fatal(err)
					}
					target.SetUID("replacement")
					if err := kube.Update(ctx, target); err != nil {
						t.Fatal(err)
					}
				}
				database, port := "OVN_Northbound", 6641
				if strings.Contains(command[3], "ovnsb") {
					database, port = "OVN_Southbound", 6642
				}
				if command[0] == "ovsdb-client" {
					return fmt.Appendf(nil, `[{"rows":[{"connections":["uuid","12345678-1111-2222-3333-444444444444"]}]},{"rows":[{"_uuid":["uuid","12345678-1111-2222-3333-444444444444"],"target":"ptcp:%d:0.0.0.0"}]}]`, port), nil
				}
				if command[4] == "ovsdb-server/list-remotes" {
					if scenario == "no listener" {
						return []byte("punix:/var/run/ovn/db.sock"), nil
					}
					if scenario == "dynamic listener" {
						global := "NB_Global"
						if database == "OVN_Southbound" {
							global = "SB_Global"
						}
						return []byte("db:" + database + "," + global + ",connections"), nil
					}
					key := pod + database
					remoteReads[key]++
					if scenario == "listener changed" && remoteReads[key] > 1 {
						port += 100
					}
					return fmt.Appendf(nil, "ptcp:%d:0.0.0.0\npunix:/var/run/ovn/db.sock", port), nil
				}
				if command[4] != "cluster/status" {
					return nil, errors.New("unexpected mutation")
				}
				data := topologyStatus(pod, database)
				if scenario == "unhealthy peer" && pod == "ovn-central-1" {
					data = strings.Replace(data, "Entries not yet applied: 0", "Entries not yet applied: 1", 1)
				}
				if scenario == "extra raft member" {
					data += "member-3 (ffff at tcp:192.0.2.4:6643)\n"
				}
				if scenario == "cluster drift" && pod == "ovn-central-1" {
					data = strings.ReplaceAll(data, "dddd0000", "dddd0001")
				}
				return []byte(data), nil
			})
			result, err := DiscoverCentralTopology(t.Context(), kube, runner, installation)
			if scenario != "healthy" && scenario != "dynamic listener" {
				if err == nil || len(result.Members) != 0 {
					t.Fatalf("unsafe topology accepted: %+v, %v", result, err)
				}
				return
			}
			if err != nil || len(result.Members) != 3 {
				t.Fatalf("discovery: %+v %v", result, err)
			}
			if result.Members[1].Databases[1].ClientAddress != "tcp:192.0.2.2:6642" || result.Members[1].Databases[1].Address != "tcp:192.0.2.2:6644" {
				t.Fatalf("client/raft endpoints confused: %+v", result)
			}
		})
	}
}

func TestPassiveClientAddressFamiliesAndAmbiguity(t *testing.T) {
	for _, tc := range []struct{ remote, host, want string }{
		{"pssl:6641:[2001:db8::1]", "2001:db8::1", "ssl:[2001:db8::1]:6641"},
		{"ptcp:6641:[::]", "2001:db8::1", "tcp:[2001:db8::1]:6641"},
		{"ptcp:6641:[::]", "192.0.2.1", ""},
		{"ptcp:6641:127.0.0.1", "192.0.2.1", ""},
		{"ptcp:6641", "192.0.2.1", "tcp:192.0.2.1:6641"},
	} {
		got, err := passiveClient(tc.remote, tc.host)
		if err != nil || got != tc.want {
			t.Fatalf("%s: %s %v", tc.remote, got, err)
		}
	}
	for _, remote := range []string{"ptcp:0", "ptcp:06641", "pssl:6641:hostname"} {
		if _, err := passiveClient(remote, "192.0.2.1"); err == nil {
			t.Fatalf("accepted %s", remote)
		}
	}
}

func TestClientConnectionsIgnoreUnreferencedRowsAndRejectMissingReferences(t *testing.T) {
	data := []byte(`[{"rows":[{"connections":["set",[["uuid","12345678-1111-2222-3333-444444444444"]]]}]},{"rows":[{"_uuid":["uuid","12345678-1111-2222-3333-444444444444"],"target":"ptcp:6641"},{"_uuid":["uuid","87654321-1111-2222-3333-444444444444"],"target":"ptcp:9999"}]}]`)
	remotes, err := connectionTargets(data)
	if err != nil || len(remotes) != 1 || remotes[0] != "ptcp:6641" {
		t.Fatalf("unreferenced connection accepted: %v %v", remotes, err)
	}
	data = []byte(strings.Replace(string(data), "12345678", "00000001", 1))
	if _, err := connectionTargets(data); err == nil {
		t.Fatal("missing connection target accepted")
	}
}
