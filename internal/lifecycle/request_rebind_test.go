package lifecycle

import (
	"slices"
	"strings"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	"k8s.io/apimachinery/pkg/types"
)

func TestRebindAutomaticReplacementUpdatesOnlyTargetAddresses(t *testing.T) {
	retained := rebindTopology()
	observed := rebindTopology()
	for database := range observed.Members[0].Databases {
		observed.Members[0].Databases[database].Address = "tcp:192.0.2.11:" + string(rune('6'+database)) + "643"
		observed.Members[0].Databases[database].ClientAddress = "tcp:192.0.2.11:" + string(rune('6'+database)) + "641"
	}
	request := rebindRequest(retained)
	updated, err := RebindAutomaticReplacement(request, retained, observed)
	if err != nil {
		t.Fatal(err)
	}
	for database := range updated.Databases {
		if updated.Databases[database].Peers[0].Address != observed.Members[0].Databases[database].Address || updated.Databases[database].Peers[0].ClientAddress != observed.Members[0].Databases[database].ClientAddress {
			t.Fatalf("target database %d was not rebound: %+v", database, updated.Databases[database].Peers[0])
		}
		if updated.Databases[database].Peers[1] != request.Databases[database].Peers[1] {
			t.Fatalf("non-target database %d changed: before=%+v after=%+v", database, request.Databases[database].Peers[1], updated.Databases[database].Peers[1])
		}
		if !slices.Contains(updated.Add.Workload.Preparation.Databases[database].RemoteAddresses, observed.Members[0].Databases[database].Address) {
			t.Fatalf("preparation database %d did not receive the fresh target remote: %+v", database, updated.Add.Workload.Preparation.Databases[database].RemoteAddresses)
		}
	}
	if _, _, err := canonical(updated); err != nil {
		t.Fatalf("rebound request is not canonical: %v", err)
	}
}

func TestRebindAutomaticReplacementRejectsNonTargetIdentityChange(t *testing.T) {
	retained := rebindTopology()
	observed := rebindTopology()
	observed.Members[1].Databases[0].Address = "tcp:192.0.2.22:6643"
	request := rebindRequest(retained)
	if _, err := RebindAutomaticReplacement(request, retained, observed); err == nil {
		t.Fatal("non-target endpoint change was accepted")
	}
}

func rebindRequest(topology health.CentralTopology) Request {
	operationID := "eeee0000-1111-2222-3333-444444444444"
	plans := make([]prepare.Database, 0, 2)
	inventories := make([]Inventory, 0, 2)
	for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		peers := make([]Peer, 0, len(topology.Members))
		remotes := make([]string, 0, len(topology.Members))
		for _, member := range topology.Members {
			db := member.Databases[database]
			peers = append(peers, Peer{Name: member.Pod.Name, ServerID: db.ServerID, Address: db.Address, ClientAddress: db.ClientAddress})
			remotes = append(remotes, db.Address)
		}
		plans = append(plans, prepare.Database{Name: name, ClusterID: topology.Members[0].Databases[database].ClusterID, LocalAddress: "tcp:192.0.2.30:" + string(rune('6'+database)) + "643", RemoteAddresses: remotes})
		inventories = append(inventories, Inventory{Database: name, ClusterID: topology.Members[0].Databases[database].ClusterID, Peers: peers})
	}
	return Request{
		FormatVersion: 1, InstallationUID: "installation-uid", InstallationGeneration: 4, OperationID: operationID,
		Databases: inventories, AutomaticReplacement: true, RemoveEndpoint: "member-0-0",
		Retirement: &Retirement{Kind: statefulSetRetirementKind, Workload: workload.Reference{Name: "member-0", UID: "workload-0"}, Endpoint: workload.Reference{Name: "member-0-0", UID: "pod-0"}, SpecDigest: "sha256:" + strings.Repeat("a", 64)},
		Add:        &Addition{Workload: workload.Request{InstallationGeneration: 4, Preparation: prepare.Plan{FormatVersion: 1, InstallationUID: "installation-uid", OperationID: operationID, MemberName: "member-new", Databases: plans}, Node: workload.Reference{Name: "node-new", UID: "node-new-uid"}, Claim: workload.Reference{Name: "claim-new", UID: "claim-new-uid"}, Volume: workload.Reference{Name: "volume-new", UID: "volume-new-uid"}, Image: "example/central@sha256:" + strings.Repeat("b", 64)}, NorthboundAddress: "tcp:192.0.2.30:6641", SouthboundAddress: "tcp:192.0.2.30:6642"},
	}
}

func rebindTopology() health.CentralTopology {
	result := health.CentralTopology{Members: make([]health.CentralIdentity, 0, 2)}
	for index := range 2 {
		member := health.CentralIdentity{Node: health.ObjectIdentity{Name: "node-" + string(rune('0'+index)), UID: types.UID("node-uid-" + string(rune('0'+index)))}, Workload: health.WorkloadIdentity{ObjectIdentity: health.ObjectIdentity{Name: "member-" + string(rune('0'+index)), UID: types.UID("workload-" + string(rune('0'+index)))}, Kind: "StatefulSet"}, Storage: []health.StorageIdentity{{MountPath: "/var/lib/ovn-member", HostPath: "/var/lib/ovn/" + string(rune('0'+index))}}, Pod: health.ObjectIdentity{Name: "member-" + string(rune('0'+index)) + "-0", UID: types.UID("pod-" + string(rune('0'+index)))}, ContainerID: "containerd://container-" + string(rune('0'+index))}
		for database, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
			member.Databases[database] = health.DatabaseIdentity{Database: name, ClusterID: "1111111" + string(rune('0'+database)) + "-2222-3333-4444-555555555555", ServerID: "0000000" + string(rune('1'+index)) + "-1111-2222-3333-444444444444", Address: "tcp:192.0.2." + string(rune('1'+index)) + ":" + string(rune('6'+database)) + "643", ClientAddress: "tcp:192.0.2." + string(rune('1'+index)) + ":" + string(rune('6'+database)) + "641"}
		}
		result.Members = append(result.Members, member)
	}
	return result
}
