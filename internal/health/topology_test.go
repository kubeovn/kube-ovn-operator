package health

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
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
	for _, scenario := range []string{"healthy", "unready pod", "readiness changed during discovery", "dynamic listener", "local config listener", "unsupported listener database", "missing pod", "unhealthy peer", "extra raft member", "cluster drift", "node replaced", "pod replaced", "listener changed", "ephemeral storage", "wrong node address", "wrong node address with explicit replacement", "wrong node address with replacement for another endpoint", "cordoned node", "cordoned node with explicit replacement", "no listener"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			if strings.Contains(scenario, "with explicit replacement") || strings.Contains(scenario, "replacement for another endpoint") {
				installation.Spec.Central.DynamicMembership = true
				installation.Status.Topology.Inventory = &operatorv1alpha1.CentralTopologyReference{Name: "retained", UID: "retained-uid", Digest: "sha256:retained"}
				target := "ovn-central-0"
				if strings.Contains(scenario, "another endpoint") {
					target = "ovn-central-1"
				}
				installation.Spec.Central.MemberRequests = []operatorv1alpha1.CentralMemberSpec{{Name: "replacement", ReplaceEndpoint: target}}
			}
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
				if scenario == "unready pod" && i == 0 {
					pod.Status.Conditions[0].Status = corev1.ConditionFalse
					pod.Status.ContainerStatuses[0].Ready = false
				}
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				node := &corev1.Node{Name: pod.Spec.NodeName, UID: types.UID(pod.Spec.NodeName), Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: fmt.Sprintf("192.0.2.%d", i+1)}}}}
				if strings.Contains(scenario, "wrong node address") && i == 0 {
					node.Status.Addresses[0].Address = "192.0.2.99"
				}
				if strings.Contains(scenario, "cordoned node") && i == 0 {
					node.Spec.Unschedulable = true
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
				if !mutated && scenario == "readiness changed during discovery" {
					mutated = true
					current := new(corev1.Pod)
					if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: pod}, current); err != nil {
						t.Fatal(err)
					}
					current.Status.Conditions[0].Status = corev1.ConditionFalse
					if err := kube.Status().Update(ctx, current); err != nil {
						t.Fatal(err)
					}
				}
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
					if scenario == "local config listener" && !strings.Contains(command[4], `["Local_Config",{"op":"select","table":"Config"`) {
						t.Fatalf("local listener query used the wrong database or table: %v", command)
					}
					return fmt.Appendf(nil, `[{"rows":[{"connections":["uuid","12345678-1111-2222-3333-444444444444"]}]},{"rows":[{"_uuid":["uuid","12345678-1111-2222-3333-444444444444"],"target":"ptcp:%d:0.0.0.0"}]}]`, port), nil
				}
				if command[4] == "ovsdb-server/list-remotes" {
					if scenario == "local config listener" {
						return []byte("db:Local_Config,Config,connections\npunix:/var/run/ovn/db.sock"), nil
					}
					if scenario == "unsupported listener database" {
						return []byte("db:Unexpected,Config,connections"), nil
					}
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
			accepted := scenario == "healthy" || scenario == "dynamic listener" || scenario == "local config listener" || scenario == "wrong node address with explicit replacement" || scenario == "cordoned node with explicit replacement"
			if !accepted {
				if scenario == "wrong node address" {
					if _, ok := errors.AsType[*EndpointDriftError](err); !ok {
						t.Fatalf("expected a typed endpoint drift error, got %v", err)
					}
				}
				if scenario == "cordoned node" {
					if _, ok := errors.AsType[*NodeLifecycleError](err); !ok {
						t.Fatalf("expected a typed node lifecycle error, got %v", err)
					}
				}
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

func TestSameTopologyIdentitySurvivesPodRenameOrdering(t *testing.T) {
	first := CentralIdentity{Node: ObjectIdentity{Name: "node-a", UID: "node-a"}, Workload: WorkloadIdentity{ObjectIdentity: ObjectIdentity{Name: "central", UID: "deployment"}, Kind: "Deployment"}, Pod: ObjectIdentity{Name: "central-old-a", UID: "old-a"}, ContainerID: "old-a"}
	second := first
	second.Node = ObjectIdentity{Name: "node-b", UID: "node-b"}
	second.Pod = ObjectIdentity{Name: "central-old-b", UID: "old-b"}
	second.ContainerID = "old-b"
	before := CentralTopology{Members: []CentralIdentity{first, second}}
	first.Pod = ObjectIdentity{Name: "central-new-z", UID: "new-a"}
	first.ContainerID = "new-a"
	after := CentralTopology{Members: []CentralIdentity{second, first}}
	if !before.SameIdentity(after) {
		t.Fatal("Pod rename/order changed retained native identities")
	}
	after.Members[1].Node.UID = "replacement-node"
	if before.SameIdentity(after) {
		t.Fatal("node replacement was accepted as a process restart")
	}
}

func TestRebindRetainedProcessesKeepsUniqueLossAndRejectsIdentityDrift(t *testing.T) {
	for _, scenario := range []string{"restart and loss", "server changed", "cluster changed", "listener changed", "node replaced", "storage changed", "node changed during sample"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			for i := range 3 {
				pod := objects[i+2].(*corev1.Pod)
				pod.Spec.NodeName, pod.Spec.HostNetwork = fmt.Sprintf("node-%d", i), true
				pod.Spec.Containers = []corev1.Container{{Name: "ovn-central", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/etc/ovn"}}}}
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/etc/origin/ovn"}}}}
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://" + pod.Name, Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				node := &corev1.Node{Name: pod.Spec.NodeName, UID: types.UID(pod.Spec.NodeName), Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: fmt.Sprintf("192.0.2.%d", i+1)}}}}
				if err := kube.Create(t.Context(), node); err != nil {
					t.Fatal(err)
				}
			}
			rebinding := false
			reads := 0
			runner := runnerFunc(func(ctx context.Context, _, pod, _ string, command []string) ([]byte, error) {
				database, port := "OVN_Northbound", 6641
				if strings.Contains(command[3], "ovnsb") {
					database, port = "OVN_Southbound", 6642
				}
				if rebinding {
					reads++
					if scenario == "node changed during sample" {
						node := new(corev1.Node)
						if err := kube.Get(ctx, client.ObjectKey{Name: "node-0"}, node); err != nil {
							t.Fatal(err)
						}
						node.UID = "replaced-during-sample"
						if err := kube.Update(ctx, node); err != nil {
							t.Fatal(err)
						}
					}
				}
				if command[4] == "ovsdb-server/list-remotes" {
					if rebinding && scenario == "listener changed" {
						port += 100
					}
					return fmt.Appendf(nil, "ptcp:%d:0.0.0.0", port), nil
				}
				if command[4] != "cluster/status" {
					return nil, errors.New("unexpected mutation")
				}
				data := topologyStatus(pod, database)
				if rebinding && scenario == "server changed" {
					data = strings.ReplaceAll(data, "aaaa0000", "aaaa0001")
				}
				if rebinding && scenario == "cluster changed" {
					data = strings.ReplaceAll(data, "dddd0000", "dddd0001")
				}
				return []byte(data), nil
			})
			retained, err := DiscoverCentralTopology(t.Context(), kube, runner, installation)
			if err != nil {
				t.Fatal(err)
			}
			before := CentralTopology{Members: append([]CentralIdentity(nil), retained.Members...)}
			// Two Deployment processes roll over; a third member is truly lost.
			// Full healthy discovery cannot hide that loss or approve a new baseline.
			for i := range 3 {
				pod := new(corev1.Pod)
				if err := kube.Get(t.Context(), client.ObjectKeyFromObject(objects[i+2]), pod); err != nil {
					t.Fatal(err)
				}
				if err := kube.Delete(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
				if i == 2 {
					continue
				}
				pod.Name, pod.UID, pod.ResourceVersion = fmt.Sprintf("restarted-central-%d", i), types.UID(fmt.Sprintf("restarted-%d", i)), ""
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://" + pod.Name
				if scenario == "storage changed" && i == 0 {
					pod.Spec.Volumes[0].HostPath.Path = "/different-data"
				}
				if err := kube.Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "node replaced" {
				node := new(corev1.Node)
				if err := kube.Get(t.Context(), client.ObjectKey{Name: "node-0"}, node); err != nil {
					t.Fatal(err)
				}
				node.UID = "replaced-node"
				if err := kube.Update(t.Context(), node); err != nil {
					t.Fatal(err)
				}
			}
			rebinding = true
			got, err := RebindRetainedProcesses(t.Context(), kube, runner, installation, retained)
			if scenario != "restart and loss" {
				if err == nil {
					t.Fatalf("identity drift accepted: %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if reads != 8 || !retained.SameIdentity(got) || !reflect.DeepEqual(retained, before) {
				t.Fatalf("unverified process or mutated baseline: reads=%d, topology=%+v", reads, got)
			}
			for i, member := range got.Members {
				if member.Node.Name == "node-2" {
					if !reflect.DeepEqual(member, retained.Members[i]) {
						t.Fatal("missing member identity changed")
					}
				} else if !strings.HasPrefix(member.Pod.Name, "restarted-central-") || member.Pod.UID == retained.Members[i].Pod.UID || member.ContainerID == retained.Members[i].ContainerID {
					t.Fatalf("surviving process was not rebound: %+v", member)
				}
			}
		})
	}
}
